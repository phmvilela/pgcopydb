"""
Source-capacity collector (Azure cloud half): read-throughput ceiling + WAL disk free.

Follow-up to #144 (the Postgres-catalog half). ``source-capacity.json`` has two
fields the catalog cannot answer: ``read_throughput_bytes_per_sec`` (the source
read-throughput ceiling -- a provisioned-storage/SKU property) and
``wal_disk_free_bytes`` (free space on the server's storage volume, not exposed
via SQL). Both live in the Azure control plane for an Azure Database for
PostgreSQL **Flexible Server** source -- this collector reads them via
``pg_cyan/cloud/azure/postgres_flexible_server.py`` and **merges** them into any
existing ``source-capacity.json`` (declared bundle or #144's catalog run), same
merge/override discipline as the catalog half: a measured value overrides a
declared one.

``read_throughput_bytes_per_sec`` is only derived when the SDK itself reports a
provisioned throughput for the server's storage tier -- there is no verified
IOPS-x-IO-size formula to fall back on, so an unsupported tier simply omits the
field rather than guess. ``wal_disk_free_bytes`` is derived from the provisioned
storage size (on the server resource) minus the ``storage_used`` Monitor metric;
these two sub-measurements degrade independently, so e.g. Reader access without
Monitoring Reader still yields a partial artifact.

``wal_disk_free_bytes`` also needs ``wal_generation_bytes_per_sec`` already on the
artifact to survive the contract's pairing rule (one alone can't feed the
WAL-retention deadline) -- if the catalog collector hasn't supplied a rate yet
(declared, or a prior ``source-capacity`` run), this collector drops its own
measured free-bytes figure rather than fail the whole write, the same
self-correction ``sourcecap.py`` applies to an unpaired rate. In a fully
automated run with nothing declared, this means the pair may not complete in a
single collect pass -- an operator declaring one half breaks the tie.

The collector needs an explicit ``PGCYAN_SOURCE_AZURE_RESOURCE_ID`` -- there is no
way to derive a server's Azure resource id from its Postgres hostname. Like every
other collector in this package it is **optional**: a missing resource id, a
credential failure, or any Azure API error preserves the existing
``source-capacity.json``, writes ``source-capacity-azure-errors.json``, and still
exits 0 -- it never breaks the walk.

See also: pg_cyan/collect/sourcecap.py (the catalog half), pg_cyan/collect/capacity_contract.py,
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
_BYTES_PER_GIB = 1024 ** 3


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


class PgCyanSourceCapacityAzure:
    """Fills the Azure-cloud fields of ``source-capacity.json`` (merge, not clobber)."""

    OUTPUT_FILENAME = "source-capacity.json"
    ERROR_FILENAME = "source-capacity-azure-errors.json"

    def __init__(self, resource_id: Optional[str], output_dir: str = ".",
                 timeout: int = 30):
        self.resource_id = resource_id
        self.output_dir = output_dir
        self.timeout = timeout
        self.errors: List[str] = []

    def _log_event(self, event_type, status, message, details=None) -> None:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "source-capacity-azure",
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
                                            "identity has Reader on the source server"})
        return profile

    def measure_storage_used(self, credential) -> Optional[float]:
        """The latest ``storage_used`` Monitor metric value (bytes), or None."""
        used = azure_pg.get_storage_used_bytes(self.resource_id, credential,
                                               timeout=self.timeout)
        if used is None:
            self._log_event("storage_used", "warning",
                            "Could not read the storage_used metric from Azure Monitor",
                            {"resource_id": self.resource_id,
                             "remediation": "Check the collect identity has Monitoring "
                                            "Reader on the source server"})
        return used

    # ------------------------------------------------------------------ output

    def _read_existing(self) -> Dict:
        """The current source-capacity.json (declared/catalog), or {} if absent/bad."""
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
            "component": "source-capacity-azure", "status": "failure",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "errors": self.errors or ["source-capacity-azure probe produced nothing usable"],
            "remediation": "Any existing source-capacity.json (declared/catalog) stays "
                           "in effect; the planner degrades the affected terms otherwise.",
        })
        return {"status": "degraded", "component": "source-capacity-azure",
                "output_file": None, "error_file": error_path, "errors": self.errors}

    def collect(self) -> Dict:
        """
        Measure the Azure fields, merge into source-capacity.json, or degrade.

        Never raises. On success writes the merged artifact (measured values
        overriding declared ones); on failure preserves the existing file.
        """
        if not self.resource_id:
            self._log_event("resource_id", "failure",
                            "PGCYAN_SOURCE_AZURE_RESOURCE_ID not set",
                            {"remediation": "Set PGCYAN_SOURCE_AZURE_RESOURCE_ID to the "
                                            "source server's Azure resource id, or provide "
                                            "read_throughput_bytes_per_sec / "
                                            "wal_disk_free_bytes via declared capacity "
                                            "inputs instead"})
            self.errors.append("PGCYAN_SOURCE_AZURE_RESOURCE_ID not set")
            return self._degrade()

        credential = self._credential()
        if credential is None:
            return self._degrade()

        profile = self.measure_storage_profile(credential)
        storage_used = self.measure_storage_used(credential)

        if profile is None and storage_used is None:
            self.errors.append("No Azure source-capacity fields could be measured")
            return self._degrade()

        merged = dict(self._read_existing())

        throughput_mibps = (profile or {}).get("throughput_mibps")
        if _is_number(throughput_mibps):
            merged["read_throughput_bytes_per_sec"] = round(
                throughput_mibps * _BYTES_PER_MIB, 2)

        storage_size_gb = (profile or {}).get("storage_size_gb")
        if _is_number(storage_size_gb) and _is_number(storage_used):
            free_bytes = storage_size_gb * _BYTES_PER_GIB - storage_used
            merged["wal_disk_free_bytes"] = round(max(free_bytes, 0.0), 2)
        elif _is_number(storage_size_gb) and not _is_number(storage_used):
            self._log_event("wal_disk_free", "warning",
                            "Have the provisioned storage size but not storage_used; "
                            "omitting wal_disk_free_bytes", {})

        # WAL feasibility needs both halves together (see the pairing rule in
        # pg_cyan/collect/capacity_contract.py) -- a free-bytes figure alone can't
        # feed the deadline. Drop it if nothing has already supplied the rate
        # (declared, or a prior catalog run) so the artifact stays contract-valid;
        # this mirrors sourcecap.py's symmetric drop of an unpaired WAL rate.
        if (_is_number(merged.get("wal_disk_free_bytes"))
                and not _is_number(merged.get("wal_generation_bytes_per_sec"))):
            self._log_event("wal_disk_free", "warning",
                            "Measured wal_disk_free_bytes but no "
                            "wal_generation_bytes_per_sec to pair it with; omitting "
                            "it from the artifact",
                            {"wal_disk_free_bytes": merged["wal_disk_free_bytes"],
                             "remediation": "Provide wal_generation_bytes_per_sec via "
                                            "the catalog collector (sourcecap.py) or "
                                            "declared inputs to enable the WAL deadline"})
            merged.pop("wal_disk_free_bytes", None)

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
                        "Wrote source-capacity (Azure cloud fields merged)",
                        {"read_throughput_bytes_per_sec": merged.get("read_throughput_bytes_per_sec"),
                         "wal_disk_free_bytes": merged.get("wal_disk_free_bytes")})
        return {"status": "success", "component": "source-capacity-azure",
                "output_file": output_path, "error_file": None, "errors": self.errors}


def collect_source_capacity_azure_from_env() -> int:
    """
    Run the Azure source-capacity collector from environment/CLI settings.

    Optional collector: returns 0 on success AND on graceful degradation (must not
    break the walk); 2 only on interrupt. Any existing source-capacity.json
    survives a degraded run.
    """
    try:
        collector = PgCyanSourceCapacityAzure(
            resource_id=os.getenv("PGCYAN_SOURCE_AZURE_RESOURCE_ID"),
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
            "message": "Azure source-capacity collection interrupted by user",
        }))
        return 2


if __name__ == "__main__":
    sys.exit(collect_source_capacity_azure_from_env())
