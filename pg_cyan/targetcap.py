"""
Target-capacity collector (Postgres-catalog half): the target's connection headroom.

The capacity-aware planner reads ``target-capacity.json`` for the target's
**write ceiling** (``write_throughput_bytes_per_sec`` -> the ``target_write`` term
of C*) and its **connection headroom** (``max_connections`` -
``current_connections`` -> the ``target_connections`` term, which bounds total
``table_jobs`` across shards). This collector fills what the Postgres catalog can
answer directly on the TARGET: ``max_connections`` / ``current_connections`` from
``pg_settings`` + ``pg_stat_activity``.

The write ceiling is a provisioned-storage/SKU property (the Azure API, or the
operator's declared inputs), not a catalog fact — so this collector **merges**
into any existing ``target-capacity.json`` rather than overwriting it, and a
measured value overrides a declared one. It connects to the TARGET database
(``PGCOPYDB_TARGET_PGURI`` / ``PG_TARGET_*``), unlike the source collectors.

Like the other collectors it is **optional**: any failure preserves the existing
``target-capacity.json`` (the fallback), writes an errors file, and still exits 0
-- it never breaks the walk.

See also: pg_cyan/capacity.py, pg_cyan/capacity_contract.py,
pg_cyan/sourcecap.py (the source mirror; the write-ceiling cloud half mirrors #154).
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import Error as PgError

from pg_cyan import capacity_contract
from pg_cyan.postgres.conn import target_conn_defaults

logger = logging.getLogger(__name__)


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


class PgCyanTargetCapacity:
    """Fills the Postgres-catalog fields of ``target-capacity.json`` (merge, not clobber)."""

    OUTPUT_FILENAME = "target-capacity.json"
    ERROR_FILENAME = "target-capacity-errors.json"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        sslmode: Optional[str] = None,
        timeout: int = 30,
        output_dir: str = ".",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.sslmode = sslmode
        self.timeout = timeout
        self.output_dir = output_dir
        self.conn = None
        self.errors: List[str] = []

    def _log_event(self, event_type, status, message, details=None) -> None:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "target-capacity",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }))

    def connect(self) -> bool:
        try:
            kwargs = dict(host=self.host, port=self.port, user=self.user,
                          password=self.password, database=self.database,
                          connect_timeout=self.timeout)
            if self.sslmode:
                kwargs["sslmode"] = self.sslmode
            self.conn = psycopg2.connect(**kwargs)
            return True
        except (PgError, OSError) as e:
            self._log_event("connection", "failure", "Failed to connect to the target",
                            {"host": self.host, "port": self.port, "error": str(e),
                             "remediation": "Check host, port, credentials, and connectivity"})
            self.errors.append(f"Connection failed: {e}")
            return False

    def measure_connections(self) -> Optional[Tuple[int, int]]:
        """(max_connections, current_connections) from the target catalog, or None."""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT current_setting('max_connections')::int, "
                        "(SELECT count(*) FROM pg_stat_activity)")
            max_conns, current = cur.fetchone()
            cur.close()
            return int(max_conns), int(current)
        except PgError as e:
            self._log_event("measure_connections", "warning",
                            "Could not read connection settings", {"error": str(e)})
            self.errors.append(f"Connection metrics failed: {e}")
            return None

    def _read_existing(self) -> Dict:
        """The current target-capacity.json (declared/cloud), or {} if absent/bad."""
        path = os.path.join(self.output_dir, self.OUTPUT_FILENAME)
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _write_json(self, filename: str, payload: Dict) -> Optional[str]:
        path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            return path
        except OSError as e:
            self._log_event("write_output", "failure", f"Failed to write {filename}",
                            {"path": path, "error": str(e)})
            self.errors.append(f"Failed to write {filename}: {e}")
            return None

    def _degrade(self) -> Dict:
        """Preserve any existing artifact, write the errors file, stay degraded."""
        error_path = self._write_json(self.ERROR_FILENAME, {
            "component": "target-capacity", "status": "failure",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "errors": self.errors or ["target-capacity probe produced nothing usable"],
            "remediation": "Any existing target-capacity.json (declared/cloud) stays "
                           "in effect; the planner degrades the affected term otherwise.",
        })
        return {"status": "degraded", "component": "target-capacity",
                "output_file": None, "error_file": error_path, "errors": self.errors}

    def collect(self) -> Dict:
        """
        Measure the target's connection headroom, merge into target-capacity.json,
        or degrade. Never raises; a failure preserves the existing artifact.
        """
        if not self.connect():
            return self._degrade()

        conns = self.measure_connections()
        self.close()

        if conns is None:
            self.errors.append("No target-capacity fields could be measured")
            return self._degrade()

        merged = dict(self._read_existing())
        merged["max_connections"], merged["current_connections"] = conns
        merged["component"] = "target-capacity"
        merged["status"] = "success"
        merged["timestamp"] = datetime.utcnow().isoformat() + "Z"
        payload = capacity_contract.stamp(merged)

        contract_errors = capacity_contract.validate(self.OUTPUT_FILENAME, payload)
        if contract_errors:
            self.errors.extend(contract_errors)
            self._log_event("validate", "warning",
                            "Merged target-capacity failed contract validation",
                            {"errors": contract_errors})
            return self._degrade()

        output_path = self._write_json(self.OUTPUT_FILENAME, payload)
        if output_path is None:
            return self._degrade()
        self._log_event("collect", "success",
                        "Wrote target-capacity (catalog fields merged)",
                        {"max_connections": merged["max_connections"],
                         "current_connections": merged["current_connections"]})
        return {"status": "success", "component": "target-capacity",
                "output_file": output_path, "error_file": None, "errors": self.errors}

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None


def collect_target_capacity_from_env() -> int:
    """
    Run the target-capacity collector from environment/CLI settings.

    Optional collector: returns 0 on success AND on graceful degradation (must not
    break the walk); 2 only on interrupt. Any existing target-capacity.json
    survives a degraded run.
    """
    try:
        defaults = target_conn_defaults()
        collector = PgCyanTargetCapacity(
            host=os.getenv("PG_TARGET_HOST", defaults["host"]),
            port=int(os.getenv("PG_TARGET_PORT", str(defaults["port"]))),
            user=os.getenv("PG_TARGET_USER", defaults["user"]),
            password=os.getenv("PG_TARGET_PASSWORD", defaults["password"]),
            database=os.getenv("PG_TARGET_DATABASE", defaults["database"]),
            sslmode=defaults["sslmode"],
            timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
            output_dir=os.getenv("PG_OUTPUT_DIR", "."),
        )
        summary = collector.collect()
        print(json.dumps(summary, indent=2, default=str))
        return 0
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Target-capacity collection interrupted by user",
        }))
        return 2


if __name__ == "__main__":
    sys.exit(collect_target_capacity_from_env())
