"""
Source instance parameter inventory for PostgreSQL migration.

Connects to the source database and captures **all instance-level configuration
parameters** (the server's GUC settings, as reported by ``pg_settings``) into a
single JSON file. The planner and the preflight gates rely on this inventory:
several of the source-side refusals in ``scripts/pgcyan-design.md`` (§4) —
``wal_level``, ``max_replication_slots``, ``max_wal_senders``,
``max_worker_processes``, ``track_commit_timestamp``, ``max_slot_wal_keep_size`` —
are decided from exactly these values.

Follows the ``PgCyanPreflight`` orchestrator pattern (see ``preflight.py`` and
``CLAUDE.md``): one class per stage, connection/state on the instance, structured
JSON logging via ``_log_event``, and graceful degradation — nothing here raises
to the caller. On success a non-empty ``pg-source-parameters.json`` is written;
on any failure a ``pg-source-parameters-errors.json`` with an error message is
written instead.
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from psycopg2 import Error as PgError

# Structured JSON logging (mirrors preflight).
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)


class PgCyanParameters:
    """Exports the source instance's configuration parameters to JSON."""

    #: Written on the happy path (non-empty).
    OUTPUT_FILENAME = "pg-source-parameters.json"
    #: Written when anything goes wrong, with an error message.
    ERROR_FILENAME = "pg-source-parameters-errors.json"

    # Columns pulled from pg_settings. Rich enough for the planner to reason
    # about restart-context, source of the value, and pending changes.
    _PARAM_COLUMNS = (
        "name",
        "setting",
        "unit",
        "category",
        "context",
        "vartype",
        "source",
        "min_val",
        "max_val",
        "enumvals",
        "boot_val",
        "reset_val",
        "pending_restart",
        "short_desc",
    )

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        timeout: int = 30,
        output_dir: str = ".",
    ):
        """
        Initialize the parameter exporter with connection parameters.

        Args:
            host: PostgreSQL server hostname or IP
            port: PostgreSQL server port
            user: Database user
            password: User password
            database: Source database name
            timeout: Connection timeout in seconds
            output_dir: Directory the JSON output/error file is written to
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.output_dir = output_dir
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
            event_type: Type of event (e.g., 'connection', 'fetch_parameters')
            status: 'success', 'warning', or 'failure'
            message: Human-readable message
            details: Additional context dict
        """
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-parameters",
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
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=self.timeout,
            )
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

    def fetch_parameters(self) -> Optional[List[Dict]]:
        """
        Read every instance-level parameter from ``pg_settings``.

        Returns:
            A list of parameter dicts (one per setting), or ``None`` if the
            query could not be run. Never raises to the caller.
        """
        if not self.conn:
            self._log_event(
                "fetch_parameters",
                "failure",
                "No database connection available",
                {"remediation": "Run connect() first"},
            )
            self.errors.append("Cannot fetch parameters: no database connection")
            return None

        try:
            cursor = self.conn.cursor()
            # Static query, no interpolation -> plain SQL is fine here.
            cursor.execute(
                "SELECT {cols} FROM pg_settings ORDER BY name;".format(
                    cols=", ".join(self._PARAM_COLUMNS)
                )
            )
            rows = cursor.fetchall()
            cursor.close()

            parameters = [
                dict(zip(self._PARAM_COLUMNS, row)) for row in rows
            ]
            self._log_event(
                "fetch_parameters",
                "success",
                f"Read {len(parameters)} instance-level parameter(s)",
                {"count": len(parameters)},
            )
            return parameters
        except PgError as e:
            self._log_event(
                "fetch_parameters",
                "failure",
                "Failed to query pg_settings",
                {
                    "error": str(e),
                    "remediation": "Check PostgreSQL logs and that the role can read pg_settings",
                },
            )
            self.errors.append(f"Parameter query failed: {str(e)}")
            return None

    def _build_output(self, parameters: List[Dict]) -> Dict:
        """Assemble the happy-path payload written to ``pg-source-parameters.json``."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-parameters",
            "status": "success",
            "source": {
                "host": self.host,
                "port": self.port,
                "database": self.database,
            },
            "parameter_count": len(parameters),
            "parameters": parameters,
        }

    def _build_error(self) -> Dict:
        """Assemble the payload written to ``pg-source-parameters-errors.json``."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-parameters",
            "status": "failure",
            "message": "Failed to export source instance parameters",
            "errors": self.errors or ["Unknown error"],
            "remediation": "Check the connection settings and that the role can read pg_settings",
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

    def export(self) -> Dict:
        """
        Connect, read parameters, and write the output (or error) JSON file.

        Never raises. Returns a summary dict:
            {
              "status": "success" | "failure",
              "output_file": <path or None>,
              "error_file": <path or None>,
              "parameter_count": <int>,
              "errors": [...],
            }
        """
        summary = {
            "status": "failure",
            "output_file": None,
            "error_file": None,
            "parameter_count": 0,
            "errors": self.errors,
        }

        parameters: Optional[List[Dict]] = None
        if self.connect():
            parameters = self.fetch_parameters()

        # Happy path: a non-empty parameter set was retrieved.
        if parameters:
            output_path = self._write_json(
                self.OUTPUT_FILENAME, self._build_output(parameters)
            )
            if output_path:
                summary.update(
                    status="success",
                    output_file=output_path,
                    parameter_count=len(parameters),
                )
                self._log_event(
                    "export",
                    "success",
                    f"Wrote {len(parameters)} parameter(s) to {self.OUTPUT_FILENAME}",
                    {"path": output_path},
                )
                return summary
            # Writing the happy-path file failed -> fall through to error file.

        # Failure path: connection, query, empty result, or write failed.
        if parameters is not None and not parameters:
            self.errors.append("No instance-level parameters were returned")
        error_path = self._write_json(self.ERROR_FILENAME, self._build_error())
        summary.update(status="failure", error_file=error_path)
        self._log_event(
            "export",
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


def export_parameters_from_env() -> int:
    """
    Export source parameters using environment variables.

    Returns:
        0 if the output file was written, 1 otherwise.
    """
    try:
        exporter = PgCyanParameters(
            host=os.getenv("PG_SOURCE_HOST", "localhost"),
            port=int(os.getenv("PG_SOURCE_PORT", "5432")),
            user=os.getenv("PG_SOURCE_USER", "postgres"),
            password=os.getenv("PG_SOURCE_PASSWORD", ""),
            database=os.getenv("PG_SOURCE_DATABASE", "postgres"),
            timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
            output_dir=os.getenv("PG_OUTPUT_DIR", "."),
        )
        summary = exporter.export()
        exporter.close()
        print(json.dumps(summary, indent=2, default=str))
        return 0 if summary["status"] == "success" else 1
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Parameter export interrupted by user",
        }))
        return 2
    except Exception as e:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "failure",
            "message": "Fatal error during parameter export",
            "error": str(e),
        }))
        return 1


if __name__ == "__main__":
    sys.exit(export_parameters_from_env())
