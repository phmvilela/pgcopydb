"""
Source-capacity collector (Postgres-catalog half): connections + WAL generation.

The capacity-aware planner reads ``source-capacity.json`` for two things: the
source **connection headroom** (``max_connections`` - ``current_connections`` ->
the ``source_connections`` term of C*) and the **WAL-retention deadline**
(``wal_disk_free_bytes`` / ``wal_generation_bytes_per_sec`` -> the feasibility
gate). This collector fills what the Postgres catalog can answer directly:

* ``max_connections`` / ``current_connections`` -- from ``pg_settings`` and
  ``pg_stat_activity``.
* ``wal_generation_bytes_per_sec`` -- measured by sampling ``pg_current_wal_lsn()``
  twice over a short interval; this is the live write rate the WAL deadline needs.

The other two fields -- ``read_throughput_bytes_per_sec`` (the source read
ceiling, a provisioned-storage/SKU property) and ``wal_disk_free_bytes`` (WAL disk
free, not exposed via SQL) -- are the **cloud** half: they come from the Azure
API or the operator's declared inputs, not from here. So this collector **merges**
into any existing ``source-capacity.json`` (declared bundle or a cloud collector)
rather than overwriting it, and a measured value overrides a declared one. When a
WAL rate is measured but no ``wal_disk_free_bytes`` is present to pair it with, the
rate is omitted from the artifact (it can't feed the deadline alone) and logged,
so the artifact stays contract-valid.

Like the network probe it is an **optional** collector: any failure preserves the
existing ``source-capacity.json`` (the fallback), writes an errors file, and still
exits 0 -- it never breaks the walk.

See also: pg_cyan/collect/capacity.py, pg_cyan/collect/feasibility.py, pg_cyan/collect/capacity_contract.py,
pg_cyan/collect/netprobe.py (the optional-collector pattern this follows).
"""
import json
import logging
import os
import sys
from datetime import datetime
from time import perf_counter, sleep
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import Error as PgError

from pg_cyan.collect import capacity_contract
from pg_cyan.postgres.conn import source_conn_defaults

logger = logging.getLogger(__name__)


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


class PgCyanSourceCapacity:
    """Fills the Postgres-catalog fields of ``source-capacity.json`` (merge, not clobber)."""

    OUTPUT_FILENAME = "source-capacity.json"
    ERROR_FILENAME = "source-capacity-errors.json"

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
        wal_sample_seconds: float = 2.0,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.sslmode = sslmode
        self.timeout = timeout
        self.output_dir = output_dir
        self.wal_sample_seconds = max(0.1, float(wal_sample_seconds))
        self.conn = None
        self.errors: List[str] = []

    def _log_event(self, event_type, status, message, details=None) -> None:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-capacity",
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
            self._log_event("connection", "failure", "Failed to connect to the source",
                            {"host": self.host, "port": self.port, "error": str(e),
                             "remediation": "Check host, port, credentials, and connectivity"})
            self.errors.append(f"Connection failed: {e}")
            return False

    # ------------------------------------------------------------- measurement

    def measure_connections(self) -> Optional[Tuple[int, int]]:
        """(max_connections, current_connections) from the catalog, or None."""
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

    def measure_wal_rate(self) -> Optional[float]:
        """
        Live WAL generation rate in bytes/sec, or None.

        Samples ``pg_current_wal_lsn()`` twice over ``wal_sample_seconds`` and
        divides the byte delta by the measured interval. Returns None on a standby
        (``pg_current_wal_lsn`` errors in recovery) or when the source is idle
        (a non-positive rate can't feed the deadline).
        """
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT pg_current_wal_lsn()")
            lsn1 = cur.fetchone()[0]
            t0 = perf_counter()
            sleep(self.wal_sample_seconds)
            elapsed = max(perf_counter() - t0, 1e-9)
            cur.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s)", (lsn1,))
            delta = float(cur.fetchone()[0])
            cur.close()
            rate = delta / elapsed
            return rate if rate > 0 else None
        except PgError as e:
            self._log_event("measure_wal_rate", "warning",
                            "Could not sample the WAL generation rate", {"error": str(e)})
            return None

    # ------------------------------------------------------------------ output

    def _read_existing(self) -> Dict:
        """The current source-capacity.json (declared/cloud), or {} if absent/bad."""
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
            "component": "source-capacity", "status": "failure",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "errors": self.errors or ["source-capacity probe produced nothing usable"],
            "remediation": "Any existing source-capacity.json (declared/cloud) stays "
                           "in effect; the planner degrades the affected terms otherwise.",
        })
        return {"status": "degraded", "component": "source-capacity",
                "output_file": None, "error_file": error_path, "errors": self.errors}

    def collect(self) -> Dict:
        """
        Measure the catalog fields, merge into source-capacity.json, or degrade.

        Never raises. On success writes the merged artifact (measured values
        overriding declared/cloud ones); on failure preserves the existing file.
        """
        if not self.connect():
            return self._degrade()

        conns = self.measure_connections()
        wal_rate = self.measure_wal_rate()
        self.close()

        merged = dict(self._read_existing())
        if conns is not None:
            merged["max_connections"], merged["current_connections"] = conns
        if wal_rate is not None:
            merged["wal_generation_bytes_per_sec"] = round(wal_rate, 2)

        # A WAL rate is only useful paired with the disk-free figure (the cloud/
        # declared half). Unpaired, drop it so the artifact stays contract-valid.
        if (_is_number(merged.get("wal_generation_bytes_per_sec"))
                and not _is_number(merged.get("wal_disk_free_bytes"))):
            self._log_event("wal_rate", "warning",
                            "Measured a WAL rate but no wal_disk_free_bytes to pair it "
                            "with; omitting it from the artifact",
                            {"wal_generation_bytes_per_sec": merged["wal_generation_bytes_per_sec"],
                             "remediation": "Provide wal_disk_free_bytes via the cloud "
                                            "collector or declared inputs to enable the WAL deadline"})
            merged.pop("wal_generation_bytes_per_sec", None)

        if conns is None and not _is_number(merged.get("wal_generation_bytes_per_sec")):
            self.errors.append("No source-capacity fields could be measured")
            return self._degrade()

        merged["component"] = "source-capacity"
        merged["status"] = "success"
        merged["timestamp"] = datetime.utcnow().isoformat() + "Z"
        payload = capacity_contract.stamp(merged)

        contract_errors = capacity_contract.validate(self.OUTPUT_FILENAME, payload)
        if contract_errors:
            self.errors.extend(contract_errors)
            self._log_event("validate", "warning",
                            "Merged source-capacity failed contract validation",
                            {"errors": contract_errors})
            return self._degrade()

        output_path = self._write_json(self.OUTPUT_FILENAME, payload)
        if output_path is None:
            return self._degrade()
        self._log_event("collect", "success",
                        "Wrote source-capacity (catalog fields merged)",
                        {"max_connections": merged.get("max_connections"),
                         "current_connections": merged.get("current_connections"),
                         "wal_generation_bytes_per_sec": merged.get("wal_generation_bytes_per_sec")})
        return {"status": "success", "component": "source-capacity",
                "output_file": output_path, "error_file": None, "errors": self.errors}

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None


def collect_source_capacity_from_env() -> int:
    """
    Run the source-capacity collector from environment/CLI settings.

    Optional collector: returns 0 on success AND on graceful degradation (must not
    break the walk); 2 only on interrupt. Any existing source-capacity.json
    survives a degraded run.
    """
    try:
        defaults = source_conn_defaults()
        collector = PgCyanSourceCapacity(
            host=os.getenv("PG_SOURCE_HOST", defaults["host"]),
            port=int(os.getenv("PG_SOURCE_PORT", str(defaults["port"]))),
            user=os.getenv("PG_SOURCE_USER", defaults["user"]),
            password=os.getenv("PG_SOURCE_PASSWORD", defaults["password"]),
            database=os.getenv("PG_SOURCE_DATABASE", defaults["database"]),
            sslmode=defaults["sslmode"],
            timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
            output_dir=os.getenv("PG_OUTPUT_DIR", "."),
            wal_sample_seconds=float(os.getenv("PGCYAN_WAL_SAMPLE_SECONDS", "2")),
        )
        summary = collector.collect()
        print(json.dumps(summary, indent=2, default=str))
        return 0
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Source-capacity collection interrupted by user",
        }))
        return 2


if __name__ == "__main__":
    sys.exit(collect_source_capacity_from_env())
