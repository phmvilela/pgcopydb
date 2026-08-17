"""
Source large-object inventory for PostgreSQL migration.

Connects to the source database and counts the rows in
``pg_largeobject_metadata`` -- i.e. how many PostgreSQL large objects (``lo`` /
``pg_largeobject``) live in the database. The migration planner needs this
because large objects are stored outside regular table storage and are copied
by a separate mechanism (``pg_dump``'s ``-b``/``--large-objects`` path, not the
per-table ``COPY``); a non-zero count means the copy plan must account for them.

Follows the ``PgCyanParameters`` / ``PgCyanMetrics`` orchestrator pattern (see
``parameters.py``, ``metrics.py``, and ``CLAUDE.md``): one class per stage,
connection/state on the instance, structured JSON logging via ``_log_event``,
and graceful degradation -- nothing here raises to the caller. On success a
non-empty ``source-pg-largeobjects.json`` is written; on any failure
(connection, query, or write) a ``source-pg-largeobjects-errors.json`` with an
error message is written instead.

Note: a count of **zero** is a valid happy-path result (a database may simply
have no large objects) -- only connection/query/write problems are failures.
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from psycopg2 import Error as PgError

# Structured JSON logging (mirrors preflight/parameters/metrics).
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)


class PgCyanLobs:
    """Counts the source database's large objects and writes the count to JSON."""

    #: Written on the happy path (non-empty).
    OUTPUT_FILENAME = "source-pg-largeobjects.json"
    #: Written when anything goes wrong, with an error message.
    ERROR_FILENAME = "source-pg-largeobjects-errors.json"

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
        Initialize the large-object collector with connection parameters.

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
            event_type: Type of event (e.g., 'connection', 'fetch_lobs')
            status: 'success', 'warning', or 'failure'
            message: Human-readable message
            details: Additional context dict
        """
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-pg-largeobjects",
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

    def fetch_lob_count(self) -> Optional[int]:
        """
        Count the rows in ``pg_largeobject_metadata``.

        Returns:
            The large-object count (``>= 0``), or ``None`` if the query could
            not be run. A count of zero is a valid result, not a failure.
            Never raises to the caller.
        """
        if not self.conn:
            self._log_event(
                "fetch_lobs",
                "failure",
                "No database connection available",
                {"remediation": "Run connect() first"},
            )
            self.errors.append("Cannot count large objects: no database connection")
            return None

        try:
            cursor = self.conn.cursor()
            # Static query, no interpolation -> plain SQL is fine here.
            cursor.execute("SELECT count(*) FROM pg_largeobject_metadata;")
            count = cursor.fetchone()[0]
            cursor.close()

            self._log_event(
                "fetch_lobs",
                "success",
                f"Counted {count} large object(s)",
                {"lob_count": count},
            )
            return count
        except PgError as e:
            self._log_event(
                "fetch_lobs",
                "failure",
                "Failed to query pg_largeobject_metadata",
                {
                    "error": str(e),
                    "remediation": "Check PostgreSQL logs and that the role can read pg_largeobject_metadata",
                },
            )
            self.errors.append(f"Large-object query failed: {str(e)}")
            return None

    def _build_output(self, lob_count: int) -> Dict:
        """Assemble the happy-path payload written to ``source-pg-largeobjects.json``."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-pg-largeobjects",
            "status": "success",
            "source": {
                "host": self.host,
                "port": self.port,
                "database": self.database,
            },
            "lob_count": lob_count,
        }

    def _build_error(self) -> Dict:
        """Assemble the payload written to ``source-pg-largeobjects-errors.json``."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-pg-largeobjects",
            "status": "failure",
            "message": "Failed to count source large objects",
            "errors": self.errors or ["Unknown error"],
            "remediation": "Check the connection settings and that the role can read pg_largeobject_metadata",
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
        Connect, count large objects, and write the output (or error) JSON file.

        Never raises. Returns a summary dict:
            {
              "status": "success" | "failure",
              "output_file": <path or None>,
              "error_file": <path or None>,
              "lob_count": <int or None>,
              "errors": [...],
            }
        """
        summary = {
            "status": "failure",
            "output_file": None,
            "error_file": None,
            "lob_count": None,
            "errors": self.errors,
        }

        count: Optional[int] = None
        if self.connect():
            count = self.fetch_lob_count()

        # Happy path: a count was retrieved. Zero is valid, so guard on
        # ``is not None`` rather than truthiness.
        if count is not None:
            output_path = self._write_json(
                self.OUTPUT_FILENAME, self._build_output(count)
            )
            if output_path:
                summary.update(
                    status="success",
                    output_file=output_path,
                    lob_count=count,
                )
                self._log_event(
                    "collect",
                    "success",
                    f"Wrote large-object count ({count}) to {self.OUTPUT_FILENAME}",
                    {"path": output_path, "lob_count": count},
                )
                return summary
            # Writing the happy-path file failed -> fall through to error file.

        # Failure path: connection, query, or write failed.
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


def collect_lobs_from_env() -> int:
    """
    Count source large objects using environment variables.

    Returns:
        0 if the output file was written, 1 otherwise.
    """
    try:
        collector = PgCyanLobs(
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
            "message": "Large-object collection interrupted by user",
        }))
        return 2
    except Exception as e:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "failure",
            "message": "Fatal error during large-object collection",
            "error": str(e),
        }))
        return 1


if __name__ == "__main__":
    sys.exit(collect_lobs_from_env())
