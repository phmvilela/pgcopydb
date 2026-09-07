"""
DB/network-free tests for the source-capacity Azure collector
(pg_cyan/collect/sourcecap_azure.py).

The Azure credential + SDK calls are stubbed; these pin the behaviour that
matters: a missing resource id degrades (never breaks the walk), the two Azure
fields merge into an existing source-capacity.json without clobbering
declared/catalog fields, a partial measurement (storage size but no
storage_used, or vice versa) still yields whatever was derivable, an unpaired
wal_disk_free_bytes is dropped to stay contract-valid (mirroring sourcecap.py's
symmetric drop of an unpaired WAL rate), and total failure preserves the
existing artifact.
"""
import json

from pg_cyan.collect import capacity_contract as cc
from pg_cyan.collect.sourcecap_azure import (
    PgCyanSourceCapacityAzure,
    collect_source_capacity_azure_from_env,
)


def _collector(tmp_path, resource_id="/subscriptions/s/resourceGroups/rg/providers/"
                                      "Microsoft.DBforPostgreSQL/flexibleServers/src",
               profile=None, storage_used=None, credential_ok=True):
    c = PgCyanSourceCapacityAzure(resource_id=resource_id, output_dir=str(tmp_path))
    c._credential = lambda: object() if credential_ok else None
    c.measure_storage_profile = lambda credential: profile
    c.measure_storage_used = lambda credential: storage_used
    return c


def _declared(tmp_path, **fields):
    (tmp_path / "source-capacity.json").write_text(json.dumps(fields))


def test_missing_resource_id_degrades_without_breaking_the_walk(tmp_path):
    declared = {"max_connections": 100}
    _declared(tmp_path, **declared)
    c = _collector(tmp_path, resource_id=None)
    summary = c.collect()

    assert summary["status"] == "degraded"
    assert json.loads((tmp_path / "source-capacity.json").read_text()) == declared
    assert (tmp_path / "source-capacity-azure-errors.json").exists()


def test_merges_both_azure_fields_when_rate_already_paired(tmp_path):
    # A rate is already on file (declared, or a prior catalog run) -- the
    # measured free-bytes figure can pair with it and survive.
    _declared(tmp_path, max_connections=200, current_connections=5,
              wal_generation_bytes_per_sec=1_000_000.0)
    c = _collector(tmp_path,
                    profile={"storage_size_gb": 512, "throughput_mibps": 250},
                    storage_used=100 * 1024 ** 3)
    summary = c.collect()

    assert summary["status"] == "success"
    art = json.loads((tmp_path / "source-capacity.json").read_text())
    assert cc.validate("source-capacity.json", art) == []
    assert art["read_throughput_bytes_per_sec"] == 250 * 1024 ** 2
    assert art["wal_disk_free_bytes"] == (512 - 100) * 1024 ** 3
    # Declared/catalog fields survived the merge.
    assert art["max_connections"] == 200 and art["current_connections"] == 5
    assert art["_contract_version"] == cc.CONTRACT_VERSION


def test_unpaired_wal_disk_free_is_dropped_to_stay_valid(tmp_path):
    # No rate on file yet -- a measured free-bytes figure can't feed the
    # deadline alone, so it's dropped, but the rest of the write still succeeds.
    _declared(tmp_path, max_connections=200)
    c = _collector(tmp_path, profile={"storage_size_gb": 128, "throughput_mibps": 250},
                   storage_used=64 * 1024 ** 3)
    summary = c.collect()

    assert summary["status"] == "success"
    art = json.loads((tmp_path / "source-capacity.json").read_text())
    assert cc.validate("source-capacity.json", art) == []
    assert "wal_disk_free_bytes" not in art  # omitted, not left dangling
    assert art["read_throughput_bytes_per_sec"] == 250 * 1024 ** 2
    assert art["max_connections"] == 200


def test_throughput_absent_still_writes_paired_wal_disk_free(tmp_path):
    _declared(tmp_path, wal_generation_bytes_per_sec=1_000_000.0)
    c = _collector(tmp_path, profile={"storage_size_gb": 128}, storage_used=64 * 1024 ** 3)
    summary = c.collect()

    assert summary["status"] == "success"
    art = json.loads((tmp_path / "source-capacity.json").read_text())
    assert "read_throughput_bytes_per_sec" not in art
    assert art["wal_disk_free_bytes"] == 64 * 1024 ** 3


def test_storage_used_absent_still_writes_read_throughput(tmp_path):
    c = _collector(tmp_path, profile={"storage_size_gb": 128, "throughput_mibps": 125},
                   storage_used=None)
    summary = c.collect()

    assert summary["status"] == "success"
    art = json.loads((tmp_path / "source-capacity.json").read_text())
    assert art["read_throughput_bytes_per_sec"] == 125 * 1024 ** 2
    assert "wal_disk_free_bytes" not in art


def test_credential_failure_preserves_existing_and_writes_errors(tmp_path):
    declared = {"read_throughput_bytes_per_sec": 111}
    _declared(tmp_path, **declared)
    c = _collector(tmp_path, credential_ok=False)
    summary = c.collect()

    assert summary["status"] == "degraded"
    assert json.loads((tmp_path / "source-capacity.json").read_text()) == declared
    assert (tmp_path / "source-capacity-azure-errors.json").exists()


def test_nothing_measurable_degrades(tmp_path):
    c = _collector(tmp_path, profile=None, storage_used=None)
    summary = c.collect()
    assert summary["status"] == "degraded"
    assert not (tmp_path / "source-capacity.json").exists()
    assert (tmp_path / "source-capacity-azure-errors.json").exists()


def test_from_env_returns_zero_even_when_degraded(tmp_path, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", str(tmp_path))
    monkeypatch.delenv("PGCYAN_SOURCE_AZURE_RESOURCE_ID", raising=False)
    assert collect_source_capacity_azure_from_env() == 0
    assert (tmp_path / "source-capacity-azure-errors.json").exists()
