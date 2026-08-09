"""
Source database metrics collection for PostgreSQL migration.

Connects to the source database and captures metrics useful for planning the
migration pipeline: server version, object counts (tables, partitions,
views, indexes, sequences, functions, procedures, triggers, extensions),
database size, and per-table sizes -- reported as separate heap and index
figures, per table and as database-wide totals, so the migration planner can
size the network-bound COPY (heap) apart from the index build.

Follows the ``PgCyanParameters`` / ``PgCyanPreflight`` orchestrator pattern
(see ``parameters.py``, ``preflight.py``, and ``CLAUDE.md``): one class per
stage, connection/state on the instance, structured JSON logging via
``_log_event``, and graceful degradation -- nothing here raises to the
caller. On success a non-empty ``source-pg-metrics.json`` is written; on any
failure a ``source-pg-metrics-errors.json`` with an error message is written
instead.

Object counts assume PostgreSQL 11+ (``pg_proc.prokind`` distinguishes
functions from procedures; stored procedures don't exist before PG11).
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from psycopg2 import Error as PgError

# Structured JSON logging (mirrors preflight/parameters).
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)

# Schemas excluded from object/table counts -- system catalogs, not
# migration-relevant user objects.
_SYSTEM_SCHEMAS = ("pg_catalog", "information_schema")

_OBJECT_COUNTS_QUERY = """
    SELECT
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'r' AND n.nspname NOT IN %(schemas)s) AS tables,
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'p' AND n.nspname NOT IN %(schemas)s) AS partitioned_tables,
        (SELECT count(*) FROM pg_inherits i
         JOIN pg_class c ON c.oid = i.inhrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname NOT IN %(schemas)s) AS partitions,
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'v' AND n.nspname NOT IN %(schemas)s) AS views,
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'm' AND n.nspname NOT IN %(schemas)s) AS materialized_views,
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'i' AND n.nspname NOT IN %(schemas)s) AS indexes,
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'S' AND n.nspname NOT IN %(schemas)s) AS sequences,
        (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE p.prokind = 'f' AND n.nspname NOT IN %(schemas)s) AS functions,
        (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE p.prokind = 'p' AND n.nspname NOT IN %(schemas)s) AS procedures,
        (SELECT count(*) FROM pg_trigger t
         JOIN pg_class c ON c.oid = t.tgrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE NOT t.tgisinternal AND n.nspname NOT IN %(schemas)s) AS triggers,
        (SELECT count(*) FROM pg_extension) AS extensions;
"""

# Heap and index bytes are reported separately: the planner bin-packs copy
# shards by heap bytes (the copy step, network-bound COPY) but sizes the index-build
# work (the post-data steps) by index bytes, so a single conflated size makes both splits
# wrong. ``pg_table_size`` (heap + TOAST + FSM/VM) plus ``pg_indexes_size`` sum
# to the ``pg_total_relation_size`` figure.
#
# The planner's cost model estimates copy *time*, which needs more than raw
# bytes, so each row also carries (see also: pg_cyan/cost_model.py):
#   * ``row_estimate``     -- ``reltuples`` (bounds narrow, high-row tables);
#   * ``dead_tuple_ratio`` -- ``n_dead_tup / (n_live+n_dead)`` from the stats
#     collector, so a bloated heap is not mistaken for copy volume;
#   * ``toast_bytes``      -- the TOAST relation's size (a subset of heap_bytes),
#     so detoast-heavy tables can be surcharged;
#   * ``avg_row_width``    -- summed ``pg_stats.avg_width``.
# All are optional to the planner and degrade gracefully when NULL/absent.
_TABLE_SIZES_QUERY = """
    SELECT
        n.nspname AS schema,
        c.relname AS name,
        c.relkind AS relkind,
        pg_table_size(c.oid) AS heap_bytes,
        pg_size_pretty(pg_table_size(c.oid)) AS heap_pretty,
        pg_indexes_size(c.oid) AS index_bytes,
        pg_size_pretty(pg_indexes_size(c.oid)) AS index_pretty,
        c.reltuples AS row_estimate,
        CASE
            WHEN (COALESCE(st.n_live_tup, 0) + COALESCE(st.n_dead_tup, 0)) > 0
            THEN COALESCE(st.n_dead_tup, 0)::float8
                 / (COALESCE(st.n_live_tup, 0) + COALESCE(st.n_dead_tup, 0))
            ELSE 0.0
        END AS dead_tuple_ratio,
        CASE WHEN c.reltoastrelid <> 0
             THEN pg_total_relation_size(c.reltoastrelid) ELSE 0 END AS toast_bytes,
        (SELECT COALESCE(sum(s.avg_width), 0) FROM pg_stats s
         WHERE s.schemaname = n.nspname AND s.tablename = c.relname) AS avg_row_width
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
    WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN %(schemas)s
    ORDER BY pg_table_size(c.oid) DESC, pg_indexes_size(c.oid) DESC;
"""


class PgCyanMetrics:
    """Collects source database metrics useful for migration planning."""

    #: Written on the happy path (non-empty).
    OUTPUT_FILENAME = "source-pg-metrics.json"
    #: Written when anything goes wrong, with an error message.
    ERROR_FILENAME = "source-pg-metrics-errors.json"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        timeout: int = 30,
        output_dir: str = ".",
        sslmode: Optional[str] = None,
    ):
        """
        Initialize the metrics collector with connection parameters.

        Args:
            host: PostgreSQL server hostname or IP
            port: PostgreSQL server port
            user: Database user
            password: User password
            database: Source database name
            timeout: Connection timeout in seconds
            output_dir: Directory the JSON output/error file is written to
            sslmode: libpq sslmode (e.g. ``require``); omitted when ``None`` so
                libpq/psycopg2 applies its own default.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.output_dir = output_dir
        self.sslmode = sslmode
        self.conn = None
        self.errors: List[str] = []

    def _log_event(
        self,
        event_type: str,
        status: str,
        message: str,
        details: Optional[Dict] = None,
    ) -> None:
        """
        Log a structured JSON event.

        Args:
            event_type: Type of event (e.g., 'connection', 'fetch_metrics')
            status: 'success', 'warning', or 'failure'
            message: Human-readable message
            details: Additional context dict
        """
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-pg-metrics",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }
        logger.info(json.dumps(event))

    def connect(self) -> bool:
        """
        Establish PostgreSQL connection with graceful error handling.

        Returns:
            True if connected, False otherwise
        """
        try:
            conn_kwargs = dict(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=self.timeout,
            )
            if self.sslmode:
                conn_kwargs["sslmode"] = self.sslmode
            self.conn = psycopg2.connect(**conn_kwargs)
            self._log_event(
                "connection",
                "success",
                f"Connected to {self.host}:{self.port}/{self.database}",
            )
            return True
        except PgError as e:
            self._log_event(
                "connection",
                "failure",
                "Failed to connect to PostgreSQL",
                {
                    "host": self.host,
                    "port": self.port,
                    "database": self.database,
                    "error": str(e),
                    "remediation": "Check host, port, credentials, and network connectivity",
                },
            )
            self.errors.append(f"Connection failed: {str(e)}")
            return False
        except Exception as e:
            self._log_event(
                "connection",
                "failure",
                "Unexpected error during connection",
                {
                    "error": str(e),
                    "remediation": "Check system resources and environment variables",
                },
            )
            self.errors.append(f"Unexpected connection error: {str(e)}")
            return False

    def fetch_metrics(self) -> Optional[Dict]:
        """
        Collect version, object-count, database-size, and table-size metrics.

        Returns:
            A metrics dict, or ``None`` if any query could not be run. Never
            raises to the caller.
        """
        if not self.conn:
            self._log_event(
                "fetch_metrics",
                "failure",
                "No database connection available",
                {"remediation": "Run connect() first"},
            )
            self.errors.append("Cannot fetch metrics: no database connection")
            return None

        try:
            cursor = self.conn.cursor()

            cursor.execute("SELECT version(), current_setting('server_version'), "
                            "current_setting('server_version_num')::int;")
            version_string, server_version, server_version_num = cursor.fetchone()

            cursor.execute(
                "SELECT pg_database_size(current_database()), "
                "pg_size_pretty(pg_database_size(current_database()));"
            )
            db_size_bytes, db_size_pretty = cursor.fetchone()

            cursor.execute(_OBJECT_COUNTS_QUERY, {"schemas": _SYSTEM_SCHEMAS})
            object_columns = [desc[0] for desc in cursor.description]
            object_counts = dict(zip(object_columns, cursor.fetchone()))

            cursor.execute(_TABLE_SIZES_QUERY, {"schemas": _SYSTEM_SCHEMAS})
            table_columns = [desc[0] for desc in cursor.description]
            tables = [dict(zip(table_columns, row)) for row in cursor.fetchall()]

            cursor.close()

            metrics = {
                "version": {
                    "version_string": version_string,
                    "server_version": server_version,
                    "server_version_num": server_version_num,
                },
                "database_size": {
                    "size_bytes": db_size_bytes,
                    "size_pretty": db_size_pretty,
                },
                "object_counts": object_counts,
                "tables": tables,
                "table_totals": self._table_totals(tables),
            }
            self._log_event(
                "fetch_metrics",
                "success",
                f"Collected metrics for {len(tables)} table(s)",
                {"table_count": len(tables)},
            )
            return metrics
        except PgError as e:
            self._log_event(
                "fetch_metrics",
                "failure",
                "Failed to query source metrics",
                {
                    "error": str(e),
                    "remediation": "Check PostgreSQL logs and that the role can read system catalogs",
                },
            )
            self.errors.append(f"Metrics query failed: {str(e)}")
            return None

    @staticmethod
    def _table_totals(tables: List[Dict]) -> Dict[str, int]:
        """
        Sum the per-table heap/index figures into database-wide totals.

        Reported alongside the per-table breakdown so the planner can size the
        network-bound COPY (the copy step, heap bytes) separately from the index build
        (the post-data steps, index bytes). ``heap_bytes + index_bytes`` equals
        ``total_bytes``, which matches the conflated ``pg_total_relation_size``
        sum.
        """
        heap_bytes = sum(t.get("heap_bytes") or 0 for t in tables)
        index_bytes = sum(t.get("index_bytes") or 0 for t in tables)
        return {
            "heap_bytes": heap_bytes,
            "index_bytes": index_bytes,
            "total_bytes": heap_bytes + index_bytes,
        }

    def _build_output(self, metrics: Dict) -> Dict:
        """Assemble the happy-path payload written to ``source-pg-metrics.json``."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-pg-metrics",
            "status": "success",
            "source": {
                "host": self.host,
                "port": self.port,
                "database": self.database,
            },
            **metrics,
        }

    def _build_error(self) -> Dict:
        """Assemble the payload written to ``source-pg-metrics-errors.json``."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-pg-metrics",
            "status": "failure",
            "message": "Failed to collect source database metrics",
            "errors": self.errors or ["Unknown error"],
            "remediation": "Check the connection settings and that the role can read system catalogs",
        }

    def _write_json(self, filename: str, payload: Dict) -> Optional[str]:
        """
        Write ``payload`` as pretty JSON into ``output_dir/filename``.

        Returns:
            The path written on success, or ``None`` if the write itself failed.
        """
        path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, default=str)
            return path
        except OSError as e:
            self._log_event(
                "write_output",
                "failure",
                f"Failed to write {filename}",
                {"path": path, "error": str(e)},
            )
            self.errors.append(f"Failed to write {filename}: {str(e)}")
            return None

    def collect(self) -> Dict:
        """
        Connect, collect metrics, and write the output (or error) JSON file.

        Never raises. Returns a summary dict:
            {
              "status": "success" | "failure",
              "output_file": <path or None>,
              "error_file": <path or None>,
              "table_count": <int>,
              "errors": [...],
            }
        """
        summary = {
            "status": "failure",
            "output_file": None,
            "error_file": None,
            "table_count": 0,
            "errors": self.errors,
        }

        metrics: Optional[Dict] = None
        if self.connect():
            metrics = self.fetch_metrics()

        # Happy path: metrics were collected.
        if metrics:
            output_path = self._write_json(
                self.OUTPUT_FILENAME, self._build_output(metrics)
            )
            if output_path:
                summary.update(
                    status="success",
                    output_file=output_path,
                    table_count=len(metrics.get("tables", [])),
                )
                self._log_event(
                    "collect",
                    "success",
                    f"Wrote source metrics to {self.OUTPUT_FILENAME}",
                    {"path": output_path},
                )
                return summary
            # Writing the happy-path file failed -> fall through to error file.

        # Failure path: connection, query, empty result, or write failed.
        if metrics is not None and not metrics:
            self.errors.append("No metrics were returned")
        error_path = self._write_json(self.ERROR_FILENAME, self._build_error())
        summary.update(status="failure", error_file=error_path)
        self._log_event(
            "collect",
            "failure",
            f"Wrote error report to {self.ERROR_FILENAME}",
            {"path": error_path, "errors": self.errors},
        )
        return summary

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
                self._log_event("connection", "success", "Connection closed gracefully")
            except Exception as e:
                self._log_event(
                    "connection",
                    "warning",
                    "Error closing connection",
                    {"error": str(e)},
                )


def collect_metrics_from_env() -> int:
    """
    Collect source metrics using environment variables.

    Returns:
        0 if the output file was written, 1 otherwise.
    """
    try:
        collector = PgCyanMetrics(
            host=os.getenv("PG_SOURCE_HOST", "localhost"),
            port=int(os.getenv("PG_SOURCE_PORT", "5432")),
            user=os.getenv("PG_SOURCE_USER", "postgres"),
            password=os.getenv("PG_SOURCE_PASSWORD", ""),
            database=os.getenv("PG_SOURCE_DATABASE", "postgres"),
            timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
            output_dir=os.getenv("PG_OUTPUT_DIR", "."),
        )
        summary = collector.collect()
        collector.close()
        print(json.dumps(summary, indent=2, default=str))
        return 0 if summary["status"] == "success" else 1
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Metrics collection interrupted by user",
        }))
        return 2
    except Exception as e:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "failure",
            "message": "Fatal error during metrics collection",
            "error": str(e),
        }))
        return 1


if __name__ == "__main__":
    sys.exit(collect_metrics_from_env())
