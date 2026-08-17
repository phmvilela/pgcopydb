"""
DB-free tests for the static capacity-input provider (pg_cyan/collect/capinputs.py).

Covers: exploding a valid bundle into stamped artifact files, skipping an invalid
sub-object while writing valid siblings, the missing-bundle no-op, the malformed
and all-invalid hard-failure paths, and the two-file output convention.
"""
import json

from pg_cyan.collect import capacity_contract as cc
from pg_cyan.collect.capinputs import PgCyanCapacityInputs


def _provider(tmp_path, bundle=None):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    output.mkdir()
    if bundle is not None:
        (inputs / PgCyanCapacityInputs.DECLARED_FILENAME).write_text(json.dumps(bundle))
    return PgCyanCapacityInputs(inputs_dir=str(inputs), output_dir=str(output)), output


def test_valid_bundle_explodes_into_stamped_artifacts(tmp_path):
    provider, output = _provider(tmp_path, {
        "source_capacity": {"read_throughput_bytes_per_sec": 2e8, "max_connections": 500},
        "target_capacity": {"write_throughput_bytes_per_sec": 1e8, "max_connections": 500},
        "network": {"bandwidth_bytes_per_sec": 1e9, "rtt_ms": 20},
        "cluster_capacity": {"allocatable_cpu": 32},
        "constraints": {"max_shards": 8},
    })
    summary = provider.provide()

    assert summary["status"] == "success"
    assert set(summary["written"]) == {
        "source-capacity.json", "target-capacity.json", "network.json",
        "cluster-capacity.json", "constraints.json"}
    # Each artifact landed as its canonical filename, contract-stamped.
    src = json.loads((output / "source-capacity.json").read_text())
    assert src["max_connections"] == 500
    assert src["_contract_version"] == cc.CONTRACT_VERSION
    # Manifest present, no error file.
    assert (output / PgCyanCapacityInputs.OUTPUT_FILENAME).exists()
    assert not (output / PgCyanCapacityInputs.ERROR_FILENAME).exists()


def test_invalid_sibling_is_skipped_valid_ones_written(tmp_path):
    provider, output = _provider(tmp_path, {
        "source_capacity": {"read_throughput_bytes_per_sec": 2e8, "max_connections": 500},
        "network": {"rtt_ms": 20},  # no throughput signal -> invalid
    })
    summary = provider.provide()

    assert summary["status"] == "warning"
    assert summary["written"] == ["source-capacity.json"]
    assert "network.json" in summary["skipped"]
    assert (output / "source-capacity.json").exists()
    assert not (output / "network.json").exists()  # bad input never reaches the planner


def test_missing_bundle_is_a_noop_success(tmp_path):
    provider, output = _provider(tmp_path, bundle=None)
    summary = provider.provide()

    assert summary["status"] == "success"
    assert summary["written"] == []
    assert not (output / "source-capacity.json").exists()
    # Manifest records the no-op; planner falls back to static defaults.
    manifest = json.loads((output / PgCyanCapacityInputs.OUTPUT_FILENAME).read_text())
    assert manifest["written"] == []


def test_malformed_bundle_is_a_hard_failure(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    output.mkdir()
    (inputs / PgCyanCapacityInputs.DECLARED_FILENAME).write_text("{not json")
    summary = PgCyanCapacityInputs(inputs_dir=str(inputs), output_dir=str(output)).provide()

    assert summary["status"] == "failure"
    assert summary["error_file"]
    assert (output / PgCyanCapacityInputs.ERROR_FILENAME).exists()


def test_all_invalid_declaration_fails_rather_than_misleads(tmp_path):
    provider, output = _provider(tmp_path, {
        "network": {"rtt_ms": 20},          # invalid
        "source_capacity": {},              # invalid
    })
    summary = provider.provide()

    assert summary["status"] == "failure"
    assert summary["written"] == []
    assert (output / PgCyanCapacityInputs.ERROR_FILENAME).exists()


def test_provide_never_raises_on_bad_shape(tmp_path):
    provider, _ = _provider(tmp_path, ["not", "a", "bundle"])
    # A list bundle: every entry is an unknown key -> hard failure, but no raise.
    assert provider.provide()["status"] == "failure"
