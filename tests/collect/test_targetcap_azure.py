"""
DB/network-free tests for the target-capacity Azure collector
(pg_cyan/collect/targetcap_azure.py).

The Azure credential + SDK calls are stubbed; these pin the behaviour that
matters: a missing resource id degrades (never breaks the walk),
write_throughput_bytes_per_sec merges into an existing target-capacity.json
without clobbering declared/catalog fields, and total failure preserves the
existing artifact. Unlike the source-side mirror (#154) there is no paired
field to wait on, so there is no drop-if-unpaired case here.
"""
import json

from pg_cyan.collect import capacity_contract as cc
from pg_cyan.collect.targetcap_azure import (
    PgCyanTargetCapacityAzure,
    collect_target_capacity_azure_from_env,
)


def _collector(tmp_path, resource_id="/subscriptions/s/resourceGroups/rg/providers/"
                                      "Microsoft.DBforPostgreSQL/flexibleServers/tgt",
               profile=None, credential_ok=True):
    c = PgCyanTargetCapacityAzure(resource_id=resource_id, output_dir=str(tmp_path))
    c._credential = lambda: object() if credential_ok else None
    c.measure_storage_profile = lambda credential: profile
    return c


def _declared(tmp_path, **fields):
    (tmp_path / "target-capacity.json").write_text(json.dumps(fields))


def test_missing_resource_id_degrades_without_breaking_the_walk(tmp_path):
    declared = {"max_connections": 100}
    _declared(tmp_path, **declared)
    c = _collector(tmp_path, resource_id=None)
    summary = c.collect()

    assert summary["status"] == "degraded"
    assert json.loads((tmp_path / "target-capacity.json").read_text()) == declared
    assert (tmp_path / "target-capacity-azure-errors.json").exists()


def test_merges_write_throughput_and_preserves_declared(tmp_path):
    _declared(tmp_path, max_connections=200, current_connections=5)
    c = _collector(tmp_path, profile={"storage_size_gb": 512, "throughput_mibps": 250})
    summary = c.collect()

    assert summary["status"] == "success"
    art = json.loads((tmp_path / "target-capacity.json").read_text())
    assert cc.validate("target-capacity.json", art) == []
    assert art["write_throughput_bytes_per_sec"] == 250 * 1024 ** 2
    # Declared/catalog fields survived the merge.
    assert art["max_connections"] == 200 and art["current_connections"] == 5
    assert art["_contract_version"] == cc.CONTRACT_VERSION


def test_throughput_absent_from_profile_degrades(tmp_path):
    # storage_size_gb present but no throughput_mibps -- the SDK doesn't report
    # it for this tier; nothing to merge.
    c = _collector(tmp_path, profile={"storage_size_gb": 128})
    summary = c.collect()
    assert summary["status"] == "degraded"
    assert not (tmp_path / "target-capacity.json").exists()


def test_credential_failure_preserves_existing_and_writes_errors(tmp_path):
    declared = {"write_throughput_bytes_per_sec": 111}
    _declared(tmp_path, **declared)
    c = _collector(tmp_path, credential_ok=False)
    summary = c.collect()

    assert summary["status"] == "degraded"
    assert json.loads((tmp_path / "target-capacity.json").read_text()) == declared
    assert (tmp_path / "target-capacity-azure-errors.json").exists()


def test_nothing_measurable_degrades(tmp_path):
    c = _collector(tmp_path, profile=None)
    summary = c.collect()
    assert summary["status"] == "degraded"
    assert not (tmp_path / "target-capacity.json").exists()
    assert (tmp_path / "target-capacity-azure-errors.json").exists()


def test_from_env_returns_zero_even_when_degraded(tmp_path, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", str(tmp_path))
    monkeypatch.delenv("PGCYAN_TARGET_AZURE_RESOURCE_ID", raising=False)
    assert collect_target_capacity_azure_from_env() == 0
    assert (tmp_path / "target-capacity-azure-errors.json").exists()
