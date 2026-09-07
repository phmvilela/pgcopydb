"""
Target-capacity collector (Azure cloud half): the target's write-throughput ceiling.

Follow-up to #145 (the Postgres-catalog half, ``targetcap.py``) and the target-side
mirror of #154 (the source's read-throughput/WAL-disk cloud half). ``target-capacity.json``
has one field the catalog cannot answer: ``write_throughput_bytes_per_sec`` -- a
provisioned-storage/SKU property of an Azure Database for PostgreSQL **Flexible
Server** target. This collector reads it via
``pg_cyan/cloud/azure/postgres_flexible_server.py`` (the same wrapper #154 built --
no new Azure API surface here) and **merges** it into any existing
``target-capacity.json`` (declared bundle or #145's catalog run), same merge/
override discipline as the catalog half: a measured value overrides a declared one.

``write_throughput_bytes_per_sec`` is only derived when the SDK itself reports a
provisioned throughput for the server's storage tier -- there is no verified
IOPS-x-IO-size formula to fall back on (same caveat as #154), so an unsupported
tier simply omits the field rather than guess.

Unlike #154's ``wal_disk_free_bytes``, there is no paired field to wait on here --
no pairing self-correction is needed, and no Monitor metric / ``storage_used``
query either, just the server resource's own ``storage.throughput``. So this
collector needs only Azure ``Reader`` on the target server resource, not
``Monitoring Reader``.

The collector needs an explicit ``PGCYAN_TARGET_AZURE_RESOURCE_ID`` -- there is no
way to derive a server's Azure resource id from its Postgres hostname. Like every
other collector in this package it is **optional**: a missing resource id, a
credential failure, or any Azure API error preserves the existing
``target-capacity.json``, writes ``target-capacity-azure-errors.json``, and still
exits 0 -- it never breaks the walk.

See also: pg_cyan/collect/targetcap.py (the catalog half), pg_cyan/collect/sourcecap_azure.py
(the source-side mirror this follows), pg_cyan/collect/capacity_contract.py,
pg_cyan/cloud/azure/postgres_flexible_server.py (the Azure SDK wrapper this calls).
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

from pg_cyan.cloud.azure import postgres_flexible_server as azure_pg
from pg_cyan.collect import capacity_contract

logger = logging.getLogger(__name__)

_BYTES_PER_MIB = 1024 ** 2


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


class PgCyanTargetCapacityAzure:
    """Fills the Azure-cloud field of ``target-capacity.json`` (merge, not clobber)."""

    OUTPUT_FILENAME = "target-capacity.json"
    ERROR_FILENAME = "target-capacity-azure-errors.json"

    def __init__(self, resource_id: Optional[str], output_dir: str = ".",
                 timeout: int = 30):
        self.resource_id = resource_id
        self.output_dir = output_dir
        self.timeout = timeout
        self.errors: List[str] = []

    def _log_event(self, event_type, status, message, details=None) -> None:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "target-capacity-azure",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }))

    # ------------------------------------------------------------- measurement

    def _credential(self):
        try:
            return azure_pg.resolve_credential()
        except Exception as e:
            self._log_event("credential", "failure", "Could not resolve an Azure credential",
                            {"error": str(e),
                             "remediation": "Run with Azure Workload Identity configured on "
                                            "the collect ServiceAccount (DefaultAzureCredential)"})
            self.errors.append(f"Credential resolution failed: {e}")
            return None

    def measure_storage_profile(self, credential) -> Optional[Dict]:
        """``{storage_size_gb, throughput_mibps?}`` from the server resource, or None."""
        profile = azure_pg.get_storage_profile(self.resource_id, credential,
                                                timeout=self.timeout)
        if profile is None:
            self._log_event("storage_profile", "warning",
                            "Could not read the server's storage profile from Azure",
                            {"resource_id": self.resource_id,
                             "remediation": "Check the resource id and that the collect "
                                            "identity has Reader on the target server"})
        return profile

    # ------------------------------------------------------------------ output

    def _read_existing(self) -> Dict:
        """The current target-capacity.json (declared/catalog), or {} if absent/bad."""
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
            "component": "target-capacity-azure", "status": "failure",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "errors": self.errors or ["target-capacity-azure probe produced nothing usable"],
            "remediation": "Any existing target-capacity.json (declared/catalog) stays "
                           "in effect; the planner degrades the target_write term otherwise.",
        })
        return {"status": "degraded", "component": "target-capacity-azure",
                "output_file": None, "error_file": error_path, "errors": self.errors}

    def collect(self) -> Dict:
        """
        Measure the write-throughput ceiling, merge into target-capacity.json, or degrade.

        Never raises. On success writes the merged artifact (measured value
        overriding a declared one); on failure preserves the existing file.
        """
        if not self.resource_id:
            self._log_event("resource_id", "failure",
                            "PGCYAN_TARGET_AZURE_RESOURCE_ID not set",
                            {"remediation": "Set PGCYAN_TARGET_AZURE_RESOURCE_ID to the "
                                            "target server's Azure resource id, or provide "
                                            "write_throughput_bytes_per_sec via declared "
                                            "capacity inputs instead"})
            self.errors.append("PGCYAN_TARGET_AZURE_RESOURCE_ID not set")
            return self._degrade()

        credential = self._credential()
        if credential is None:
            return self._degrade()

        profile = self.measure_storage_profile(credential)
        throughput_mibps = (profile or {}).get("throughput_mibps")
        if not _is_number(throughput_mibps):
            self.errors.append("No Azure target-capacity fields could be measured")
            return self._degrade()

        merged = dict(self._read_existing())
        merged["write_throughput_bytes_per_sec"] = round(throughput_mibps * _BYTES_PER_MIB, 2)

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
                        "Wrote target-capacity (Azure cloud field merged)",
                        {"write_throughput_bytes_per_sec": merged.get("write_throughput_bytes_per_sec")})
        return {"status": "success", "component": "target-capacity-azure",
                "output_file": output_path, "error_file": None, "errors": self.errors}


def collect_target_capacity_azure_from_env() -> int:
    """
    Run the Azure target-capacity collector from environment/CLI settings.

    Optional collector: returns 0 on success AND on graceful degradation (must not
    break the walk); 2 only on interrupt. Any existing target-capacity.json
    survives a degraded run.
    """
    try:
        collector = PgCyanTargetCapacityAzure(
            resource_id=os.getenv("PGCYAN_TARGET_AZURE_RESOURCE_ID"),
            output_dir=os.getenv("PG_OUTPUT_DIR", "."),
            timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
        )
        summary = collector.collect()
        print(json.dumps(summary, indent=2, default=str))
        return 0
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Azure target-capacity collection interrupted by user",
        }))
        return 2


if __name__ == "__main__":
    sys.exit(collect_target_capacity_azure_from_env())
