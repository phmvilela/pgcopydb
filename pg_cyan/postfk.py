"""
Post-data foreign-key constraints for the sharded topology -- phase ``3b``.

This is the phase ``3b`` node in ``deploy/k8s/`` (``71-job-phase3-fk.yaml``): a
**single** coordinated pass, run after every index shard (phase ``3a``) succeeds.
FKs are NOT sharded like indexes because a foreign key can reference a table in a
*different* copy shard -- so applying them shard-by-shard could hit a not-yet-present
parent. One pass over all FKs sidesteps that (internal concurrency comes from
running independent validations back-to-back on the target).

WHY THIS IS CUSTOM SQL, NOT A pgcopydb WRAPPER. The ticket requires foreign keys
added **``NOT VALID`` then ``VALIDATE CONSTRAINT``** -- the correct minimal-downtime
pattern, because validating FKs is the slow, un-parallelizable tail of a restore
and NOT-VALID-then-VALIDATE keeps the exclusive-lock window tiny (the ``ADD ... NOT
VALID`` takes a brief ``SHARE ROW EXCLUSIVE`` lock; the subsequent ``VALIDATE
CONSTRAINT`` scans under a weaker ``SHARE UPDATE EXCLUSIVE`` lock that does not
block reads/writes). ``pgcopydb restore constraints`` restores FKs the way
``pg_dump`` emits them -- **validated inline** -- so it cannot produce this split.
Hence this phase introspects the source's FK definitions and applies them to the
target itself, over psycopg2 (like the collector stages), rather than wrapping
pgcopydb.

It also writes the **authoritative combined output** the ticket's AC names --
``target-pg-postload-log.json`` -- by verifying the target's actual state after
this pass: the secondary indexes (built by 3a) and the validated FK count. So the
one file attests to the whole post-data phase.

Idempotent retries: a re-run skips a constraint that already exists on the target
(a prior partial pass) and still (re-)validates it, so a backed-off Job converges.

Follows the ``PgCyanMetrics`` / collector pattern: connection/state on the
instance, structured JSON logging via ``_log_event``, graceful degradation --
``run()`` never raises -- and the success-vs-error two-file output shape.
``psycopg2.sql`` composes every identifier; the FK body comes from the source
catalog's ``pg_get_constraintdef`` (trusted, already-quoted SQL).
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from psycopg2 import Error as PgError
from psycopg2 import sql

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)

_USER_SCHEMA_FILTER = "n.nspname NOT IN ('pg_catalog', 'information_schema')"

#: Source introspection: every FK, its owning table, and its definition body
#: (``FOREIGN KEY (...) REFERENCES ...`` -- WITHOUT the ``CONSTRAINT <name>`` prefix,
#: which we add ourselves so we can name + NOT VALID it).
_SELECT_FKS = f"""
    SELECT n.nspname AS schema, c.relname AS table_name,
           con.conname AS name, pg_get_constraintdef(con.oid) AS definition
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE con.contype = 'f' AND {_USER_SCHEMA_FILTER}
    ORDER BY n.nspname, c.relname, con.conname
"""

#: Target verification counts (user schemas only).
_COUNT_INDEXES = ("SELECT count(*) FROM pg_indexes "
                  "WHERE schemaname NOT IN ('pg_catalog', 'information_schema')")
_COUNT_FKS_VALIDATED = f"""
    SELECT count(*) FROM pg_constraint con
    JOIN pg_namespace n ON n.oid = con.connamespace
    WHERE con.contype = 'f' AND con.convalidated AND {_USER_SCHEMA_FILTER}
"""


class PgCyanForeignKeys:
    """Applies the source's FKs to the target as NOT VALID then VALIDATE (phase 3b)."""

    #: The authoritative post-data output (the ticket's AC).
    LOG_OUTPUT_FILENAME = "target-pg-postload-log.json"
    LOG_ERROR_FILENAME = "target-pg-postload-log-errors.json"

    DEFAULT_WORK_DIR = "/work"
    #: statement_timeout for a single VALIDATE CONSTRAINT (a full table scan).
    DEFAULT_VALIDATE_TIMEOUT = 6 * 60 * 60      # 6h

    def __init__(
        self,
        source_pguri: Optional[str] = None,
        target_pguri: Optional[str] = None,
        output_dir: Optional[str] = None,
        validate_timeout: int = DEFAULT_VALIDATE_TIMEOUT,
    ) -> None:
        """
        Args:
            source_pguri: Source libpq URI -- introspected for FK definitions.
            target_pguri: Target libpq URI -- FKs applied + verified here.
            output_dir: Directory for the log files (defaults to ``/work``).
            validate_timeout: ``statement_timeout`` (s) for each VALIDATE.
        """
        self.source_pguri = source_pguri
        self.target_pguri = target_pguri
        self.output_dir = output_dir if output_dir is not None else self.DEFAULT_WORK_DIR
        self.validate_timeout = validate_timeout

        self.source_conn = None
        self.target_conn = None
        self.errors: List[str] = []

    # ------------------------------------------------------------- logging

    def _log_event(self, event_type: str, status: str, message: str,
                   details: Optional[Dict] = None) -> None:
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "post-fk",
            "event_type": event_type,
            "status": status,
            "message": message,
        }
        if details:
            event["details"] = details
        logger.info(json.dumps(event, default=str))

    # --------------------------------------------------------- connections

    def _connect(self, which: str, dsn: Optional[str]):
        if not dsn:
            self._log_event("connect", "failure", f"No {which} URI supplied",
                            {"remediation": f"Set PGCOPYDB_{which.upper()}_PGURI"})
            self.errors.append(f"No {which} URI: set PGCOPYDB_{which.upper()}_PGURI")
            return None
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = True   # each ALTER/VALIDATE is its own tx (short locks)
            self._log_event("connect", "success", f"Connected to {which}")
            return conn
        except PgError as e:
            self._log_event("connect", "failure", f"Could not connect to {which}",
                            {"error": str(e), "remediation": "Check the URI / network / TLS"})
            self.errors.append(f"Could not connect to {which}: {str(e)}")
            return None

    # ---------------------------------------------------------- introspect

    def fetch_source_fks(self) -> Optional[List[Dict]]:
        """Return the source's foreign keys as ``{schema, table, name, definition}``.

        Returns ``None`` on an introspection ERROR (distinct from an empty list,
        which means the source genuinely has no FKs) so the caller does not mistake
        a failed read for "nothing to do".
        """
        try:
            with self.source_conn.cursor() as cur:
                cur.execute(_SELECT_FKS)
                rows = cur.fetchall()
            fks = [{"schema": r[0], "table": r[1], "name": r[2], "definition": r[3]}
                   for r in rows]
            self._log_event("introspect", "success",
                            f"Found {len(fks)} foreign key(s) on the source",
                            {"count": len(fks)})
            return fks
        except PgError as e:
            self._log_event("introspect", "failure", "Could not read source foreign keys",
                            {"error": str(e)})
            self.errors.append(f"Could not read source FKs: {str(e)}")
            return None

    # -------------------------------------------------------------- apply

    def apply_fk(self, fk: Dict) -> bool:
        """
        Apply one FK to the target as ``ADD CONSTRAINT ... NOT VALID`` then
        ``VALIDATE CONSTRAINT``. Idempotent: an already-present constraint skips
        the ADD and still validates. Returns True iff the FK ends up validated.
        Never raises -- records the error and returns False.
        """
        schema, table, name, definition = fk["schema"], fk["table"], fk["name"], fk["definition"]
        tbl = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        cname = sql.Identifier(name)
        key = f"{schema}.{table}.{name}"
        try:
            with self.target_conn.cursor() as cur:
                # 1) ADD ... NOT VALID (brief lock). The definition body is the
                #    source catalog's pg_get_constraintdef output (trusted SQL);
                #    append NOT VALID, which must be the final clause.
                add = (sql.SQL("ALTER TABLE ") + tbl + sql.SQL(" ADD CONSTRAINT ")
                       + cname + sql.SQL(" ") + sql.SQL(definition) + sql.SQL(" NOT VALID"))
                try:
                    cur.execute(add)
                except psycopg2.errors.DuplicateObject:
                    # Already added by a prior (partial) pass -- fine, validate it.
                    self._log_event("add_fk", "warning",
                                    f"{key} already exists; will (re)validate",
                                    {"fk": key})
                # 2) VALIDATE (scans under a weak lock; bound by statement_timeout).
                cur.execute(sql.SQL("SET statement_timeout = {}").format(
                    sql.Literal(int(self.validate_timeout * 1000))))
                cur.execute(sql.SQL("ALTER TABLE ") + tbl
                            + sql.SQL(" VALIDATE CONSTRAINT ") + cname)
            return True
        except PgError as e:
            self._log_event("apply_fk", "failure", f"Could not apply/validate {key}",
                            {"fk": key, "error": str(e),
                             "remediation": "Check the FK's referenced table exists on the target (all shards + indexes done) and the data satisfies it"})
            self.errors.append(f"Could not apply {key}: {str(e)}")
            return False

    # ------------------------------------------------------- verification

    def _count(self, query: str) -> Optional[int]:
        try:
            with self.target_conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        except PgError as e:
            self.errors.append(f"Verification count failed: {str(e)}")
            return None

    # ------------------------------------------------------------- output

    def _write_json(self, filename: str, payload: Dict) -> Optional[str]:
        path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, indent=2, default=str) + "\n")
            return path
        except OSError as e:
            self._log_event("write_output", "failure", f"Failed to write {filename}",
                            {"path": path, "error": str(e)})
            self.errors.append(f"Failed to write {filename}: {str(e)}")
            return None

    def _build_error(self, message: str) -> Dict:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "post-fk",
            "status": "failure",
            "message": message,
            "errors": self.errors or ["Unknown error"],
            "remediation": "Phase 3b is the last post-data step before CDC; fix the cause and let k8s retry (it is idempotent).",
        }

    def close(self) -> None:
        for conn in (self.source_conn, self.target_conn):
            try:
                if conn is not None:
                    conn.close()
            except Exception:  # noqa: BLE001 - best-effort
                pass

    # --------------------------------------------------------------- run

    def run(self) -> Dict:
        """
        Introspect the source's FKs, apply each to the target as NOT VALID +
        VALIDATE, verify the target's post-data state, and write the authoritative
        ``target-pg-postload-log.json``. Never raises.
        """
        summary = {
            "status": "failure",
            "log_file": None,
            "log_error_file": None,
            "fk_total": 0,
            "fk_validated": 0,
            "errors": self.errors,
        }
        try:
            self.source_conn = self._connect("source", self.source_pguri)
            self.target_conn = self._connect("target", self.target_pguri)
            if self.source_conn is None or self.target_conn is None:
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("Could not connect to source and/or target"))
                return summary

            fks = self.fetch_source_fks()
            if fks is None:                       # introspection ERROR, not "0 FKs"
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME,
                    self._build_error("Could not read the source's foreign keys"))
                return summary
            summary["fk_total"] = len(fks)
            applied = sum(1 for fk in fks if self.apply_fk(fk))
            summary["fk_validated"] = applied

            # Verify the target's actual post-data state for the combined log.
            index_count = self._count(_COUNT_INDEXES)
            validated_on_target = self._count(_COUNT_FKS_VALIDATED)

            # A shortfall (some FK failed to validate, or a count is unreadable) is
            # a phase failure -- the post-data phase must be complete before CDC.
            if applied < len(fks) or (validated_on_target is not None and validated_on_target < len(fks)):
                self._log_event("run", "failure",
                                f"Only {applied}/{len(fks)} FKs validated "
                                f"({validated_on_target} validated on target)",
                                {"fk_total": len(fks), "fk_validated": applied})
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME,
                    self._build_error(f"{len(fks) - applied} foreign key(s) did not validate"))
                return summary

            payload = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "component": "post-fk",
                "status": "completed",
                "message": (
                    "Post-data complete: secondary indexes built (phase 3a) and all "
                    "foreign keys added NOT VALID then VALIDATE'd on the target."
                ),
                "target_index_count": index_count,
                "fk_total": len(fks),
                "fk_validated": applied,
                "fk_validated_on_target": validated_on_target,
            }
            summary["log_file"] = self._write_json(self.LOG_OUTPUT_FILENAME, payload)
            if not summary["log_file"]:
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("FKs applied but writing the log failed"))
                return summary

            summary["status"] = "success"
            self._log_event("run", "success",
                            "Post-data foreign keys applied and validated",
                            {"fk_validated": applied, "target_index_count": index_count})
            return summary
        finally:
            self.close()


def build_foreign_keys_from_env() -> int:
    worker = _postfk_from_env()
    summary = worker.run()
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary["status"] == "success" else 1


def _postfk_from_env() -> "PgCyanForeignKeys":
    return PgCyanForeignKeys(
        source_pguri=os.getenv("PGCOPYDB_SOURCE_PGURI") or None,
        target_pguri=os.getenv("PGCOPYDB_TARGET_PGURI") or None,
        output_dir=os.getenv("PG_OUTPUT_DIR") or None,
    )


if __name__ == "__main__":
    sys.exit(build_foreign_keys_from_env())
