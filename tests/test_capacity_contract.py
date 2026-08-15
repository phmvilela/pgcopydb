"""
DB-free tests for the capacity-input contract (pg_cyan/capacity_contract.py).

Covers: each artifact's required-field and type/range rules, the lenient
copylog-history shape, bundle validation + key mapping, and the version stamp.
The contract is the single source of truth shared by the static provider, the
control-plane API, and the collectors, so these pins guard all three.
"""
from pg_cyan import capacity_contract as cc


# ----------------------------------------------------------------- per-artifact

def test_valid_source_capacity_has_no_errors():
    assert cc.validate("source-capacity.json", {
        "read_throughput_bytes_per_sec": 2e8, "max_connections": 500,
        "current_connections": 10, "baseline_read_utilization": 0.2,
    }) == []


def test_source_capacity_rejects_empty_object():
    errs = cc.validate("source-capacity.json", {})
    assert any("at least one of" in e for e in errs)


def test_source_capacity_rejects_bad_types_and_ranges():
    errs = cc.validate("source-capacity.json", {
        "read_throughput_bytes_per_sec": "fast", "max_connections": -1,
        "baseline_read_utilization": 1.5,
    })
    assert any("read_throughput_bytes_per_sec must be a number" in e for e in errs)
    assert any("max_connections must be > 0" in e for e in errs)
    assert any("baseline_read_utilization" in e for e in errs)


def test_source_capacity_wal_fields_must_come_as_a_pair():
    errs = cc.validate("source-capacity.json", {
        "max_connections": 100, "wal_disk_free_bytes": 1024,
    })
    assert any("must be given together" in e for e in errs)
    assert cc.validate("source-capacity.json", {
        "max_connections": 100, "wal_disk_free_bytes": 1024,
        "wal_generation_bytes_per_sec": 512,
    }) == []


def test_network_requires_a_throughput_signal():
    assert any("at least one of" in e
               for e in cc.validate("network.json", {"rtt_ms": 20}))
    assert cc.validate("network.json", {"bandwidth_bytes_per_sec": 1e9,
                                        "rtt_ms": 20}) == []
    assert cc.validate("network.json", {"per_stream_bytes_per_sec": 5e7}) == []


def test_target_capacity_valid_and_invalid():
    assert cc.validate("target-capacity.json",
                       {"write_throughput_bytes_per_sec": 1e8, "max_connections": 200}) == []
    assert any("at least one of" in e for e in cc.validate("target-capacity.json", {}))


def test_cluster_capacity_requires_allocatable_cpu():
    assert any("allocatable_cpu is required" in e
               for e in cc.validate("cluster-capacity.json", {"node_count": 3}))
    assert cc.validate("cluster-capacity.json", {"allocatable_cpu": 32}) == []


def test_non_object_capacity_artifact_is_rejected():
    assert cc.validate("network.json", [1, 2, 3]) == ["network.json must be a JSON object"]


# ------------------------------------------------------------ copylog history

def test_history_accepts_list_and_runs_and_flat_shapes():
    assert cc.validate("copylog-history.json", [{"duration_seconds": 1, "rows": 5}]) == []
    assert cc.validate("copylog-history.json", {"observations": []}) == []
    assert cc.validate("copylog-history.json", {"runs": [{"tables": []}]}) == []


def test_history_rejects_wrong_container_and_list_fields():
    assert cc.validate("copylog-history.json", 42) != []
    assert cc.validate("copylog-history.json", {"runs": {}}) == ["runs must be a list"]
    assert cc.validate("copylog-history.json", {"observations": 5}) == ["observations must be a list"]


# ------------------------------------------------------------------- bundle

def test_validate_bundle_maps_keys_and_reports_per_artifact():
    result = cc.validate_bundle({
        "source_capacity": {"max_connections": 100},
        "network": {"rtt_ms": 10},          # missing throughput -> error
        "mystery": {"x": 1},                # unknown key
    })
    assert result["source-capacity.json"] == []
    assert result["network.json"]                       # has errors
    assert result["mystery"] == ["unknown bundle key: mystery"]


def test_validate_bundle_rejects_non_object():
    assert "<bundle>" in cc.validate_bundle([1, 2])


def test_unknown_artifact_filename():
    assert cc.validate("bogus.json", {}) == ["unknown artifact: bogus.json"]


# -------------------------------------------------------------------- stamp

def test_stamp_records_contract_version_without_clobbering():
    stamped = cc.stamp({"max_connections": 10})
    assert stamped["_contract_version"] == cc.CONTRACT_VERSION
    assert stamped["max_connections"] == 10
    # An explicit version is preserved.
    assert cc.stamp({"_contract_version": 99})["_contract_version"] == 99
