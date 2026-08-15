"""
DB-free tests for the target-capacity collector (pg_cyan/targetcap.py).

Mirrors the source collector's contract: merge the target's connection headroom
into target-capacity.json without clobbering a declared/cloud write ceiling, and
degrade gracefully (preserve the existing artifact, never break the walk).
"""
import json

from pg_cyan import capacity_contract as cc
from pg_cyan.targetcap import PgCyanTargetCapacity, collect_target_capacity_from_env


def _collector(tmp_path, conns=(200, 10), connect_ok=True):
    c = PgCyanTargetCapacity(host="tgt", port=5432, user="u", password="p",
                             database="db", output_dir=str(tmp_path))
    c.connect = lambda: connect_ok
    c.close = lambda: None
    c.measure_connections = lambda: conns
    return c


def test_merges_headroom_and_preserves_declared_write_ceiling(tmp_path):
    (tmp_path / "target-capacity.json").write_text(json.dumps(
        {"write_throughput_bytes_per_sec": 3e8, "baseline_write_utilization": 0.1}))
    c = _collector(tmp_path, conns=(400, 40))
    summary = c.collect()

    assert summary["status"] == "success"
    art = json.loads((tmp_path / "target-capacity.json").read_text())
    assert cc.validate("target-capacity.json", art) == []
    assert art["max_connections"] == 400 and art["current_connections"] == 40
    assert art["write_throughput_bytes_per_sec"] == 3e8      # declared preserved
    assert art["baseline_write_utilization"] == 0.1
    assert art["_contract_version"] == cc.CONTRACT_VERSION


def test_headroom_only_is_valid_without_declared(tmp_path):
    c = _collector(tmp_path, conns=(100, 5))
    assert c.collect()["status"] == "success"
    art = json.loads((tmp_path / "target-capacity.json").read_text())
    assert art["max_connections"] == 100
    assert cc.validate("target-capacity.json", art) == []    # max_connections satisfies


def test_connect_failure_preserves_existing_and_writes_errors(tmp_path):
    declared = {"write_throughput_bytes_per_sec": 111, "max_connections": 9}
    (tmp_path / "target-capacity.json").write_text(json.dumps(declared))
    summary = _collector(tmp_path, connect_ok=False).collect()

    assert summary["status"] == "degraded"
    assert json.loads((tmp_path / "target-capacity.json").read_text()) == declared
    assert (tmp_path / "target-capacity-errors.json").exists()


def test_nothing_measurable_degrades(tmp_path):
    summary = _collector(tmp_path, conns=None).collect()
    assert summary["status"] == "degraded"
    assert not (tmp_path / "target-capacity.json").exists()
    assert (tmp_path / "target-capacity-errors.json").exists()


def test_from_env_returns_zero_even_when_degraded(tmp_path, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr(PgCyanTargetCapacity, "connect", lambda self: False)
    assert collect_target_capacity_from_env() == 0
