"""
Preflight checks for PostgreSQL migration readiness.

Implements 'Safety First': graceful degradation on connection errors,
structured JSON logging, and comprehensive diagnostics.
"""
import json
import logging
import sys
import os
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql, Error as PgError

# Configure structured JSON logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)


class PgCyanPreflight:
    """PostgreSQL migration preflight checker."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        timeout: int = 30
    ):
        """
        Initialize preflight checker with connection parameters.
        
        Args:
            host: PostgreSQL server hostname or IP
            port: PostgreSQL server port
            user: Database user
            password: User password
            database: Target database name
            timeout: Connection timeout in seconds
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.conn = None
        self.checks_passed = 0
        self.checks_failed = 0
        self.warnings = []
        self.errors = []

    def _log_event(
        self,
        event_type: str,
        status: str,
        message: str,
        details: Optional[Dict] = None
    ) -> None:
        """
        Log a structured JSON event.
        
        Args:
            event_type: Type of check (e.g., 'long_running_tx', 'primary_key')
            status: 'success', 'warning', or 'failure'
            message: Human-readable message
            details: Additional context dict
        """
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "preflight",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {}
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
                connect_timeout=self.timeout
            )
            self._log_event(
                "connection",
                "success",
                f"Connected to {self.host}:{self.port}/{self.database}"
            )
            return True
        except PgError as e:
            self._log_event(
                "connection",
                "failure",
                f"Failed to connect to PostgreSQL",
                {
                    "host": self.host,
                    "port": self.port,
                    "database": self.database,
                    "error": str(e),
                    "remediation": "Check host, port, credentials, and network connectivity"
                }
            )
            self.errors.append(f"Connection failed: {str(e)}")
            self.checks_failed += 1
            return False
        except Exception as e:
            self._log_event(
                "connection",
                "failure",
                f"Unexpected error during connection",
                {
                    "error": str(e),
                    "remediation": "Check system resources and environment variables"
                }
            )
            self.errors.append(f"Unexpected connection error: {str(e)}")
            self.checks_failed += 1
            return False

    def check_long_running_transactions(
        self,
        threshold_minutes: int = 5
    ) -> bool:
        """
        Check for long-running transactions that might block migration.
        
        Args:
            threshold_minutes: Alert threshold in minutes
            
        Returns:
            True if check passed, False otherwise
        """
        if not self.conn:
            self._log_event(
                "long_running_tx",
                "failure",
                "No database connection available",
                {"remediation": "Run connect() first"}
            )
            return False

        try:
            cursor = self.conn.cursor()
            query = sql.SQL("""
                SELECT
                    pid,
                    usename,
                    query_start,
                    EXTRACT(EPOCH FROM (NOW() - query_start)) as duration_seconds,
                    query
                FROM pg_stat_activity
                WHERE query_start < NOW() - INTERVAL '%s minutes'
                AND state NOT IN ('idle', 'idle in transaction (aborted)')
                ORDER BY query_start ASC;
            """)
            
            cursor.execute(query.format(sql.Literal(threshold_minutes)))
            long_txs = cursor.fetchall()
            cursor.close()

            if not long_txs:
                self._log_event(
                    "long_running_tx",
                    "success",
                    f"No transactions running longer than {threshold_minutes} minutes"
                )
                self.checks_passed += 1
                return True
            else:
                details = {
                    "count": len(long_txs),
                    "threshold_minutes": threshold_minutes,
                    "transactions": [
                        {
                            "pid": tx[0],
                            "user": tx[1],
                            "started_at": str(tx[2]),
                            "duration_seconds": float(tx[3]),
                            "query": tx[4][:100] if tx[4] else None  # Truncate for safety
                        }
                        for tx in long_txs
                    ],
                    "remediation": "Cancel long-running transactions with pg_terminate_backend(pid) if safe to do so"
                }
                self._log_event(
                    "long_running_tx",
                    "warning",
                    f"Found {len(long_txs)} long-running transaction(s)",
                    details
                )
                self.warnings.append(f"Long-running transactions detected: {len(long_txs)}")
                self.checks_passed += 1  # This is a warning, not a blocking failure
                return True

        except PgError as e:
            self._log_event(
                "long_running_tx",
                "failure",
                "Failed to query long-running transactions",
                {
                    "error": str(e),
                    "remediation": "Check PostgreSQL logs and user permissions"
                }
            )
            self.errors.append(f"Long-running TX check failed: {str(e)}")
            self.checks_failed += 1
            return False

    def check_primary_keys(self) -> bool:
        """
        Check all tables for Primary Key, Unique Key, or REPLICA IDENTITY FULL.
        
        Per pg_cyan principles, every table must be replication-safe.
        
        Returns:
            True if all tables have pk/uk/replica_identity_full, False otherwise
        """
        if not self.conn:
            self._log_event(
                "primary_key",
                "failure",
                "No database connection available",
                {"remediation": "Run connect() first"}
            )
            return False

        try:
            cursor = self.conn.cursor()
            
            # Query for tables lacking PK, UK, or REPLICA IDENTITY FULL
            query = """
                SELECT
                    schemaname,
                    tablename
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                AND NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = (schemaname || '.' || tablename)::regclass
                    AND contype IN ('p', 'u')
                )
                AND NOT EXISTS (
                    SELECT 1 FROM pg_class
                    WHERE oid = (schemaname || '.' || tablename)::regclass
                    AND relreplident = 'f'  -- REPLICA IDENTITY FULL
                )
                ORDER BY schemaname, tablename;
            """
            
            cursor.execute(query)
            non_compliant = cursor.fetchall()
            cursor.close()

            if not non_compliant:
                self._log_event(
                    "primary_key",
                    "success",
                    "All tables have Primary Key, Unique Key, or REPLICA IDENTITY FULL"
                )
                self.checks_passed += 1
                return True
            else:
                details = {
                    "count": len(non_compliant),
                    "tables": [
                        {"schema": tbl[0], "name": tbl[1]}
                        for tbl in non_compliant
                    ],
                    "remediation": (
                        "Add Primary Key to each table, or set REPLICA IDENTITY FULL: "
                        "ALTER TABLE table_name REPLICA IDENTITY FULL;"
                    )
                }
                self._log_event(
                    "primary_key",
                    "failure",
                    f"Found {len(non_compliant)} table(s) without PK/UK/REPLICA IDENTITY FULL",
                    details
                )
                self.errors.append(f"Primary key check failed: {len(non_compliant)} non-compliant tables")
                self.checks_failed += 1
                return False

        except PgError as e:
            self._log_event(
                "primary_key",
                "failure",
                "Failed to query table constraints",
                {
                    "error": str(e),
                    "remediation": "Check PostgreSQL logs and user permissions"
                }
            )
            self.errors.append(f"Primary key check failed: {str(e)}")
            self.checks_failed += 1
            return False

    def run_all_checks(self) -> Dict:
        """
        Run all preflight checks and return summary.
        
        Returns:
            Dictionary with results, checks passed/failed, warnings, errors
        """
        results = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "checks": {}
        }

        # Connect
        if not self.connect():
            results["summary"] = {
                "total_passed": self.checks_passed,
                "total_failed": self.checks_failed,
                "warnings": len(self.warnings),
                "errors": self.errors,
                "ready_for_migration": False,
                "remediation": "Cannot proceed until database connection is established"
            }
            return results

        # Run checks
        results["checks"]["long_running_transactions"] = self.check_long_running_transactions()
        results["checks"]["primary_keys"] = self.check_primary_keys()

        # Summarize
        all_passed = all(results["checks"].values())
        results["summary"] = {
            "total_passed": self.checks_passed,
            "total_failed": self.checks_failed,
            "warnings": len(self.warnings),
            "errors": self.errors,
            "ready_for_migration": all_passed and self.checks_failed == 0,
        }

        return results

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
                self._log_event(
                    "connection",
                    "success",
                    "Connection closed gracefully"
                )
            except Exception as e:
                self._log_event(
                    "connection",
                    "warning",
                    "Error closing connection",
                    {"error": str(e)}
                )


def run_preflight_from_env() -> int:
    """
    Run preflight checks using environment variables.
    
    Returns:
        0 if ready for migration, 1 otherwise
    """
    try:
        preflight = PgCyanPreflight(
            host=os.getenv("PG_SOURCE_HOST", "localhost"),
            port=int(os.getenv("PG_SOURCE_PORT", "5432")),
            user=os.getenv("PG_SOURCE_USER", "postgres"),
            password=os.getenv("PG_SOURCE_PASSWORD", ""),
            database=os.getenv("PG_SOURCE_DATABASE", "postgres"),
            timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30"))
        )

        results = preflight.run_all_checks()
        
        # Output results
        print(json.dumps(results, indent=2, default=str))
        
        preflight.close()
        
        return 0 if results["summary"]["ready_for_migration"] else 1

    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Preflight checks interrupted by user"
        }))
        return 2
    except Exception as e:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "failure",
            "message": "Fatal error during preflight",
            "error": str(e)
        }))
        return 1


if __name__ == "__main__":
    sys.exit(run_preflight_from_env())
