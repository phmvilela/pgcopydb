"""
Post-data index + PK/unique-constraint build for the sharded topology
-- step ``3a``.

This is the step ``3a`` node in ``deploy/k8s/`` (``70-job-indexes.template.yaml``):
one Job per index shard, all concurrent, each building the **secondary indexes AND
the primary-key / unique constraints** for its OWN disjoint set of tables (the
planner's ``shard-<n>.filters``). Index builds are independent, so they shard the
same way the copy does; foreign keys -- which can span shards -- are a separate
single pass (step ``3b``, ``pg_cyan/postfk.py``).

WHY PK/UNIQUE LIVE HERE. Pre-data creates tables with **no** indexes or
constraints of any kind -- a primary key is backed by an index, and building it
during pre-data would slow the COPY. So PK/unique constraints land here alongside
the secondary indexes, and this step builds both.

WHY CUSTOM SQL, NOT pgcopydb (like the foreign-keys step). pgcopydb's per-object ``copy indexes``
/ ``copy constraints`` are built for the shared ``--dir`` clone workflow and do
nothing in our standalone per-shard pods (they exit 0 having built no indexes).
Rather than fight that coupling, this step introspects the source's index and
PK/unique definitions directly and applies them to the target over psycopg2 --
the exact pattern the foreign-keys step uses for foreign keys. No pgcopydb, no ``--dir``
catalog, no snapshot import: the DDL definitions come straight from the source
catalog (``pg_get_indexdef`` / ``pg_get_constraintdef``), which is stable across
the migration window (the same assumption 3b already makes -- no snapshot needed).

WHERE THE WORK LIVES: an index build / constraint creation runs **on the target
server** -- the pod just issues DDL and waits. So these pods are deliberately
**thin**; you go faster with target-side levers (more index shards, capped by
target vCPU + ``max_connections``; ``maintenance_work_mem``;
``max_parallel_maintenance_workers``; temporarily scaling the target SKU up for the
window), not fatter pods. See ``notes/k8s-sharded-migration-topology.md``. Per-pod
builds are sequential; the concurrency comes from the shards (N pods at once). A
future optimisation could parallelise CREATE INDEX within a pod.

Idempotent: a re-run skips an index/constraint that already exists on the target
(a prior partial pass), so a backed-off Job converges.

Output (to ``--output-dir`` / ``$PG_OUTPUT_DIR``), the same success-vs-error
two-file shape as the other stages:
  * ``target-pg-index-log.json``        -- on success, per shard.
  * ``target-pg-index-log-errors.json`` -- on any failure.

The authoritative combined ``target-pg-postload-log.json`` (the ticket's AC) is
written by step ``3b`` after every index shard succeeds -- it verifies the
target's actual index + FK state directly.

Follows the ``PgCyanForeignKeys`` / collector pattern: structured JSON logging via
``_log_event`` and graceful degradation -- ``run()`` never raises. ``psycopg2.sql``
composes every identifier; the index/constraint bodies come from the source
catalog (trusted, already-quoted SQL).
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import Error as PgError
from psycopg2 import sql

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)

#: This table's secondary indexes -- i.e. indexes NOT backing a constraint (those
#: are created by the ADD CONSTRAINT below, which builds its own index).
_SELECT_SECONDARY_INDEXES = """
    SELECT ic.relname AS name, pg_get_indexdef(i.indexrelid) AS def
    FROM pg_index i
    JOIN pg_class ic ON ic.oid = i.indexrelid
    WHERE i.indrelid = %s::regclass
      AND NOT EXISTS (
          SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid)
    ORDER BY ic.relname
"""

#: This table's PRIMARY KEY + UNIQUE constraints (pg_get_constraintdef yields e.g.
#: 'PRIMARY KEY (id)' / 'UNIQUE (sku)'; ADD CONSTRAINT builds the backing index).
_SELECT_PK_UNIQUE = """
    SELECT con.conname AS name, pg_get_constraintdef(con.oid) AS def
    FROM pg_constraint con
    WHERE con.conrelid = %s::regclass AND con.contype IN ('p', 'u')
    ORDER BY con.contype DESC, con.conname
"""


class PgCyanPostIndex:
    """Builds one shard's secondary indexes + PK/unique constraints (the indexes step)."""

    #: Written on the happy path (per shard).
    LOG_OUTPUT_FILENAME = "target-pg-index-log.json"
    #: Written on any failure.
    LOG_ERROR_FILENAME = "target-pg-index-log-errors.json"

    DEFAULT_PLAN_DIR = "/plan"
    DEFAULT_OUTPUT_DIR = "/work"
    #: statement_timeout for a single CREATE INDEX / ADD CONSTRAINT (a table scan
    #: on the target); index builds on large tables can be long.
    DEFAULT_STATEMENT_TIMEOUT = 6 * 60 * 60      # 6h

    def __init__(
        self,
        source_pguri: Optional[str] = None,
        target_pguri: Optional[str] = None,
        filter_file: Optional[str] = None,
        plan_dir: str = DEFAULT_PLAN_DIR,
        shard_name: Optional[str] = None,
        output_dir: Optional[str] = None,
        statement_timeout: int = DEFAULT_STATEMENT_TIMEOUT,
    ) -> None:
        """
        Args:
            source_pguri: Source libpq URI -- introspected for index/constraint defs.
            target_pguri: Target libpq URI -- DDL applied here.
            filter_file: This shard's ``shard-<n>.filters`` (under ``plan_dir``);
                its ``[include-only-table]`` list scopes the work.
            plan_dir: Directory the shard filter files are mounted in.
            shard_name: This index shard's name (e.g. ``idx-0``), recorded in the log.
            output_dir: Directory for the log files.
            statement_timeout: ``statement_timeout`` (s) for each DDL statement.
        """
        self.source_pguri = source_pguri
        self.target_pguri = target_pguri
        self.filter_file = filter_file
        self.plan_dir = plan_dir
        self.shard_name = shard_name
        self.output_dir = output_dir if output_dir is not None else self.DEFAULT_OUTPUT_DIR
        self.statement_timeout = statement_timeout

        self.source_conn = None
        self.target_conn = None
        self.errors: List[str] = []

    # ------------------------------------------------------------- logging

    def _log_event(self, event_type: str, status: str, message: str,
                   details: Optional[Dict] = None) -> None:
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "post-index",
            "shard": self.shard_name,
            "event_type": event_type,
            "status": status,
            "message": message,
        }
        if details:
            event["details"] = details
        logger.info(json.dumps(event, default=str))

    # ------------------------------------------------------- shard tables

    def filter_path(self) -> Optional[str]:
        if not self.filter_file:
            return None
        if os.path.isabs(self.filter_file):
            return self.filter_file
        return os.path.join(self.plan_dir, self.filter_file)

    def parse_shard_tables(self) -> Optional[List[str]]:
        """Read this shard's ``[include-only-table]`` list from the filter file.

        Returns the schema-qualified table names, or ``None`` if the file is
        missing/unreadable (distinct from an empty shard).
        """
        path = self.filter_path()
        if not path or not os.path.isfile(path):
            self.errors.append(f"Shard filter file missing: {path or self.filter_file!r}")
            self._log_event("filter", "failure", "Shard filter file missing",
                            {"filter_file": self.filter_file, "resolved": path})
            return None
        try:
            with open(path, encoding="utf-8") as fh:
                lines = fh.read().splitlines()
        except OSError as e:
            self.errors.append(f"Could not read filter file {path}: {str(e)}")
            self._log_event("filter", "failure", "Could not read shard filter file",
                            {"path": path, "error": str(e)})
            return None
        tables, in_section = [], False
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("["):
                in_section = line.lower() == "[include-only-table]"
                continue
            if in_section:
                tables.append(line)
        return tables

    @staticmethod
    def _split_qualified(name: str) -> Tuple[str, str]:
        """'public.orders' -> ('public', 'orders'); bare name -> ('public', name)."""
        schema, _, table = name.partition(".")
        return (schema, table) if table else ("public", schema)

    # ---------------------------------------------------------- connections

    def _connect(self, which: str, dsn: Optional[str]):
        if not dsn:
            self._log_event("connect", "failure", f"No {which} URI supplied",
                            {"remediation": f"Set PGCOPYDB_{which.upper()}_PGURI"})
            self.errors.append(f"No {which} URI: set PGCOPYDB_{which.upper()}_PGURI")
            return None
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = True   # each DDL is its own tx
            self._log_event("connect", "success", f"Connected to {which}")
            return conn
        except PgError as e:
            self._log_event("connect", "failure", f"Could not connect to {which}",
                            {"error": str(e), "remediation": "Check the URI / network / TLS"})
            self.errors.append(f"Could not connect to {which}: {str(e)}")
            return None

    # ---------------------------------------------------------- introspect

    def _fetch(self, query: str, table: str, what: str) -> Optional[List[Dict]]:
        """Run one introspection query for ``table``; ``None`` on error."""
        try:
            with self.source_conn.cursor() as cur:
                cur.execute(query, (table,))
                rows = cur.fetchall()
            return [{"table": table, "name": r[0], "def": r[1]} for r in rows]
        except PgError as e:
            self._log_event("introspect", "failure", f"Could not read {what} for {table}",
                            {"table": table, "error": str(e)})
            self.errors.append(f"Could not read {what} for {table}: {str(e)}")
            return None

    # -------------------------------------------------------------- apply

    def _set_timeout(self, cur) -> None:
        cur.execute(sql.SQL("SET statement_timeout = {}").format(
            sql.Literal(int(self.statement_timeout * 1000))))

    def apply_index(self, idx: Dict) -> bool:
        """Run one CREATE INDEX on the target. Idempotent (skips if it exists)."""
        try:
            with self.target_conn.cursor() as cur:
                self._set_timeout(cur)
                # pg_get_indexdef is a complete, schema-qualified CREATE INDEX
                # statement from the source catalog (trusted SQL).
                try:
                    cur.execute(idx["def"])
                except psycopg2.errors.DuplicateTable:
                    self._log_event("apply_index", "warning",
                                    f"Index {idx['name']} already exists; skipping",
                                    {"index": idx["name"], "table": idx["table"]})
            return True
        except PgError as e:
            self._log_event("apply_index", "failure",
                            f"Could not create index {idx['name']}",
                            {"index": idx["name"], "table": idx["table"], "error": str(e)})
            self.errors.append(f"Could not create index {idx['name']}: {str(e)}")
            return False

    def apply_constraint(self, con: Dict) -> bool:
        """ADD one PK/UNIQUE constraint on the target. Idempotent."""
        schema, table = self._split_qualified(con["table"])
        tbl = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        try:
            with self.target_conn.cursor() as cur:
                self._set_timeout(cur)
                stmt = (sql.SQL("ALTER TABLE ") + tbl + sql.SQL(" ADD CONSTRAINT ")
                        + sql.Identifier(con["name"]) + sql.SQL(" ") + sql.SQL(con["def"]))
                try:
                    cur.execute(stmt)
                except (psycopg2.errors.DuplicateObject, psycopg2.errors.DuplicateTable):
                    self._log_event("apply_constraint", "warning",
                                    f"Constraint {con['name']} already exists; skipping",
                                    {"constraint": con["name"], "table": con["table"]})
            return True
        except PgError as e:
            self._log_event("apply_constraint", "failure",
                            f"Could not add constraint {con['name']}",
                            {"constraint": con["name"], "table": con["table"], "error": str(e)})
            self.errors.append(f"Could not add constraint {con['name']}: {str(e)}")
            return False

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
            "component": "post-index",
            "status": "failure",
            "shard": self.shard_name,
            "message": message,
            "errors": self.errors or ["Unknown error"],
            "remediation": "the indexes step is a barrier before the FK pass (3b); fix the cause and let k8s retry (it is idempotent).",
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
        Introspect this shard's tables' secondary indexes + PK/unique constraints
        from the source and apply them to the target, then write the log. Never
        raises. Returns a summary dict.
        """
        summary = {
            "status": "failure",
            "log_file": None,
            "log_error_file": None,
            "shard": self.shard_name,
            "indexes_built": 0,
            "constraints_built": 0,
            "errors": self.errors,
        }
        try:
            tables = self.parse_shard_tables()
            if tables is None:
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("Missing/unreadable shard filter file"))
                return summary

            self.source_conn = self._connect("source", self.source_pguri)
            self.target_conn = self._connect("target", self.target_pguri)
            if self.source_conn is None or self.target_conn is None:
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("Could not connect to source and/or target"))
                return summary

            # Introspect the shard's tables. A read error (None) is a failure, not
            # "nothing to do" -- distinct from a table legitimately having none.
            indexes: List[Dict] = []
            constraints: List[Dict] = []
            for table in tables:
                idx = self._fetch(_SELECT_SECONDARY_INDEXES, table, "secondary indexes")
                con = self._fetch(_SELECT_PK_UNIQUE, table, "PK/unique constraints")
                if idx is None or con is None:
                    summary["log_error_file"] = self._write_json(
                        self.LOG_ERROR_FILENAME, self._build_error(f"Could not introspect {table}"))
                    return summary
                indexes.extend(idx)
                constraints.extend(con)

            # Secondary indexes first, then PK/unique constraints (each builds its
            # own backing index). Every one must succeed -- 3a is a barrier before FKs.
            idx_ok = sum(1 for i in indexes if self.apply_index(i))
            con_ok = sum(1 for c in constraints if self.apply_constraint(c))
            summary["indexes_built"] = idx_ok
            summary["constraints_built"] = con_ok

            if idx_ok < len(indexes) or con_ok < len(constraints):
                self._log_event("run", "failure",
                                f"Built {idx_ok}/{len(indexes)} indexes, "
                                f"{con_ok}/{len(constraints)} constraints",
                                {"shard": self.shard_name})
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME,
                    self._build_error("Some indexes/constraints failed to build"))
                return summary

            payload = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "component": "post-index",
                "status": "completed",
                "shard": self.shard_name,
                "tables": tables,
                "indexes_built": idx_ok,
                "constraints_built": con_ok,
                "message": (
                    f"Built {idx_ok} secondary index(es) and {con_ok} PK/UNIQUE "
                    f"constraint(s) for shard {self.shard_name}. Foreign keys are "
                    "the separate the foreign-keys step."
                ),
            }
            summary["log_file"] = self._write_json(self.LOG_OUTPUT_FILENAME, payload)
            if not summary["log_file"]:
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("Built OK but writing the log failed"))
                return summary

            summary["status"] = "success"
            self._log_event("run", "success",
                            "Index + PK/unique constraint build complete",
                            {"shard": self.shard_name, "indexes": idx_ok, "constraints": con_ok})
            return summary
        finally:
            self.close()


def build_indexes_from_env() -> int:
    worker = _postindex_from_env()
    summary = worker.run()
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary["status"] == "success" else 1


def _postindex_from_env() -> "PgCyanPostIndex":
    return PgCyanPostIndex(
        source_pguri=os.getenv("PGCOPYDB_SOURCE_PGURI") or None,
        target_pguri=os.getenv("PGCOPYDB_TARGET_PGURI") or None,
        filter_file=os.getenv("FILTER_FILE") or None,
        plan_dir=os.getenv("PGCYAN_PLAN_DIR", PgCyanPostIndex.DEFAULT_PLAN_DIR),
        shard_name=os.getenv("SHARD_NAME") or None,
        output_dir=os.getenv("PG_OUTPUT_DIR") or None,
    )


if __name__ == "__main__":
    sys.exit(build_indexes_from_env())
