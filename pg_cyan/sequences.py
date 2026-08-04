"""
Target sequence sync -- run after the data copy, before CDC.

The sharded copy loads rows with their EXPLICIT id values (``pgcopydb copy
table-data`` -> COPY), which never advances the owning identity/serial SEQUENCE.
So after the copy every target sequence is stuck at its start
(``pg_sequence_last_value`` is NULL / ``is_called`` false), and the next
``nextval`` returns 1. That breaks two things:
  * CDC: apply sets ``GENERATED ALWAYS AS IDENTITY`` columns to DEFAULT, so a
    replicated INSERT draws ``id`` from the sequence -> gets 1 -> COLLIDES with the
    already-copied row id=1; the conflict is swallowed on commit and the change is
    silently dropped.
  * Post-cutover, the first application INSERT into any such table collides too.

``pgcopydb clone`` handles this as its "STEP 9: reset sequences"; our custom copy
skips it, so this stage does it: read each source sequence's value and ``setval``
it on the target, so target sequences match the source (next ids continue past the
copied rows).

CUSTOM SQL, not ``pgcopydb copy sequences`` -- pgcopydb's per-object ``copy``
sub-commands no-op in our standalone per-pod ``--dir``. This is a
single coordinated pass (sequences are cheap; not sharded).

CAVEAT (online source): with pgcopydb's transform-to-DEFAULT for identity columns,
a source that keeps taking writes during the migration can still diverge ids for
changes in the gap between the snapshot and this sync (and thus break FK
references). A frozen-source cutover is unaffected. Deeper id preservation is a
separate concern.

Output (to ``--output-dir`` / ``$PG_OUTPUT_DIR``), the usual two-file shape:
  * ``target-pg-sequences-log.json``        -- on success.
  * ``target-pg-sequences-log-errors.json`` -- on any failure.

Follows the ``PgCyanForeignKeys`` pattern: ``_log_event`` logging, graceful
degradation (``run()`` never raises), ``psycopg2.sql`` composition.
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

#: Every user sequence + its current value on the source. ``last_value`` is NULL
#: when the sequence has never been advanced (nothing to sync).
_SELECT_SEQUENCES = """
    SELECT schemaname, sequencename, last_value
    FROM pg_sequences
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY schemaname, sequencename
"""


class PgCyanSequences:
    """Sets each target sequence to its source value (post-copy sequence sync)."""

    LOG_OUTPUT_FILENAME = "target-pg-sequences-log.json"
    LOG_ERROR_FILENAME = "target-pg-sequences-log-errors.json"

    DEFAULT_OUTPUT_DIR = "/work"

    def __init__(
        self,
        source_pguri: Optional[str] = None,
        target_pguri: Optional[str] = None,
        output_dir: Optional[str] = None,
    ) -> None:
        """
        Args:
            source_pguri: Source libpq URI -- read the sequence values from here.
            target_pguri: Target libpq URI -- ``setval`` them here.
            output_dir: Directory for the log files (defaults to ``/work``).
        """
        self.source_pguri = source_pguri
        self.target_pguri = target_pguri
        self.output_dir = output_dir if output_dir is not None else self.DEFAULT_OUTPUT_DIR

        self.source_conn = None
        self.target_conn = None
        self.errors: List[str] = []

    # ------------------------------------------------------------- logging

    def _log_event(self, event_type: str, status: str, message: str,
                   details: Optional[Dict] = None) -> None:
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "sequences",
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
            conn.autocommit = True
            self._log_event("connect", "success", f"Connected to {which}")
            return conn
        except PgError as e:
            self._log_event("connect", "failure", f"Could not connect to {which}",
                            {"error": str(e), "remediation": "Check the URI / network / TLS"})
            self.errors.append(f"Could not connect to {which}: {str(e)}")
            return None

    # ---------------------------------------------------------- introspect

    def fetch_source_sequences(self) -> Optional[List[Dict]]:
        """Return the source sequences as ``{schema, name, last_value}``.

        ``None`` on an introspection ERROR (distinct from an empty list, which
        means the source genuinely has no user sequences).
        """
        try:
            with self.source_conn.cursor() as cur:
                cur.execute(_SELECT_SEQUENCES)
                rows = cur.fetchall()
            seqs = [{"schema": r[0], "name": r[1], "last_value": r[2]} for r in rows]
            self._log_event("introspect", "success",
                            f"Found {len(seqs)} sequence(s) on the source",
                            {"count": len(seqs)})
            return seqs
        except PgError as e:
            self._log_event("introspect", "failure", "Could not read source sequences",
                            {"error": str(e)})
            self.errors.append(f"Could not read source sequences: {str(e)}")
            return None

    # -------------------------------------------------------------- apply

    def sync_sequence(self, seq: Dict) -> bool:
        """``setval`` one target sequence to the source's value. Idempotent.

        Skips a source sequence that was never advanced (``last_value`` NULL) --
        there is nothing to carry over. Never raises; records the error.
        """
        schema, name, last_value = seq["schema"], seq["name"], seq["last_value"]
        key = f"{schema}.{name}"
        if last_value is None:
            self._log_event("sync", "success", f"{key} never advanced on source; skipping",
                            {"sequence": key})
            return True
        qualified = f"{schema}.{name}"
        try:
            with self.target_conn.cursor() as cur:
                # setval(regclass, bigint, is_called=true): next nextval = value+1.
                cur.execute(
                    sql.SQL("SELECT setval({}, {}, true)").format(
                        sql.Literal(qualified), sql.Literal(int(last_value))))
            return True
        except PgError as e:
            self._log_event("sync", "failure", f"Could not setval {key}",
                            {"sequence": key, "last_value": last_value, "error": str(e),
                             "remediation": "Confirm the sequence exists on the target and the role may setval it"})
            self.errors.append(f"Could not setval {key}: {str(e)}")
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
            "component": "sequences",
            "status": "failure",
            "message": message,
            "errors": self.errors or ["Unknown error"],
            "remediation": "Target sequences unsynced -> CDC/post-cutover inserts into identity tables collide (id starts at 1). Fix the cause and re-run.",
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
        Read the source sequences and setval each on the target. Never raises.
        """
        summary = {
            "status": "failure",
            "log_file": None,
            "log_error_file": None,
            "sequences_total": 0,
            "sequences_synced": 0,
            "errors": self.errors,
        }
        try:
            self.source_conn = self._connect("source", self.source_pguri)
            self.target_conn = self._connect("target", self.target_pguri)
            if self.source_conn is None or self.target_conn is None:
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("Could not connect to source and/or target"))
                return summary

            seqs = self.fetch_source_sequences()
            if seqs is None:                       # introspection ERROR, not "0 sequences"
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("Could not read the source's sequences"))
                return summary
            summary["sequences_total"] = len(seqs)
            synced = sum(1 for s in seqs if self.sync_sequence(s))
            summary["sequences_synced"] = synced

            if synced < len(seqs):
                self._log_event("run", "failure",
                                f"Only {synced}/{len(seqs)} sequences synced",
                                {"sequences_total": len(seqs), "sequences_synced": synced})
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME,
                    self._build_error(f"{len(seqs) - synced} sequence(s) failed to sync"))
                return summary

            payload = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "component": "sequences",
                "status": "completed",
                "message": ("Target sequences synced to their source values; "
                            "next ids continue past the copied rows."),
                "sequences_total": len(seqs),
                "sequences_synced": synced,
            }
            summary["log_file"] = self._write_json(self.LOG_OUTPUT_FILENAME, payload)
            if not summary["log_file"]:
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("Sequences synced but writing the log failed"))
                return summary

            summary["status"] = "success"
            self._log_event("run", "success", "Sequence sync complete",
                            {"sequences_synced": synced})
            return summary
        finally:
            self.close()


def copy_sequences_from_env() -> int:
    worker = _sequences_from_env()
    summary = worker.run()
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary["status"] == "success" else 1


def _sequences_from_env() -> "PgCyanSequences":
    return PgCyanSequences(
        source_pguri=os.getenv("PGCOPYDB_SOURCE_PGURI") or None,
        target_pguri=os.getenv("PGCOPYDB_TARGET_PGURI") or None,
        output_dir=os.getenv("PG_OUTPUT_DIR") or None,
    )


if __name__ == "__main__":
    sys.exit(copy_sequences_from_env())
