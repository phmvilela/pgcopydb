"""
DB-free tests for the network collector (pg_cyan/netprobe.py).

The socket/COPY I/O is stubbed; these pin the behaviour that matters: a valid,
contract-checked network.json on success, the throughput aggregation math, and
the graceful-degradation contract -- a probe failure must preserve a declared
network.json (the fallback) and never break the walk.
"""
import json

import pytest
from psycopg2 import Error as PgError

from pg_cyan import capacity_contract as cc
from pg_cyan.netprobe import PgCyanNetworkProbe, _CountingSink, probe_network_from_env


def _probe(tmp_path, **kw):
    kw.setdefault("streams", 1)
    return PgCyanNetworkProbe(
        host="src", port=5432, user="u", password="p", database="db",
        output_dir=str(tmp_path), **kw)


def test_counting_sink_counts_bytes():
    s = _CountingSink()
    s.write("xxxxx")
    s.write("yy")
    assert s.n == 7


def test_copy_sql_scales_rows_to_stream_bytes(tmp_path):
    p = _probe(tmp_path, stream_bytes=4096 * 10)
    sql = p._copy_sql()
    assert "generate_series(1, 10)" in sql
    assert "TO STDOUT" in sql


def test_measure_throughput_single_stream(tmp_path):
    p = _probe(tmp_path, streams=1)
    p._copy_stream = lambda: (1_000_000, 0.5)  # 2 MB/s
    per_stream, bandwidth = p.measure_throughput()
    assert per_stream == pytest.approx(2_000_000)
    assert bandwidth == pytest.approx(per_stream)  # 1 stream -> no aggregate


def test_measure_throughput_parallel_bandwidth_at_least_per_stream(tmp_path):
    p = _probe(tmp_path, streams=3)
    calls = []
    p._copy_stream = lambda: (calls.append(1) or (500_000, 0.25))
    per_stream, bandwidth = p.measure_throughput()
    assert per_stream == pytest.approx(2_000_000)
    assert bandwidth >= per_stream
    assert len(calls) == 1 + 3  # one single-stream probe + three parallel


def test_measure_throughput_none_when_stream_fails(tmp_path):
    p = _probe(tmp_path)
    def boom():
        raise PgError("no COPY")
    p._copy_stream = boom
    assert p.measure_throughput() is None
    assert p.errors


def test_probe_success_writes_valid_network_json(tmp_path):
    p = _probe(tmp_path)
    p.measure_rtt = lambda: 12.3456
    p.measure_throughput = lambda: (50_000_000.0, 180_000_000.0)
    summary = p.probe()

    assert summary["status"] == "success"
    net = json.loads((tmp_path / "network.json").read_text())
    assert cc.validate("network.json", net) == []          # our own contract holds
    assert net["per_stream_bytes_per_sec"] == 50_000_000.0
    assert net["bandwidth_bytes_per_sec"] == 180_000_000.0
    assert net["rtt_ms"] == 12.346
    assert net["_contract_version"] == cc.CONTRACT_VERSION


def test_probe_without_rtt_still_succeeds(tmp_path):
    p = _probe(tmp_path)
    p.measure_rtt = lambda: None
    p.measure_throughput = lambda: (50_000_000.0, 50_000_000.0)
    assert p.probe()["status"] == "success"
    net = json.loads((tmp_path / "network.json").read_text())
    assert "rtt_ms" not in net
    assert cc.validate("network.json", net) == []


def test_probe_success_overrides_declared(tmp_path):
    (tmp_path / "network.json").write_text(json.dumps(
        {"bandwidth_bytes_per_sec": 1, "per_stream_bytes_per_sec": 1}))
    p = _probe(tmp_path)
    p.measure_rtt = lambda: 5.0
    p.measure_throughput = lambda: (9.0e7, 9.0e7)
    p.probe()
    net = json.loads((tmp_path / "network.json").read_text())
    assert net["per_stream_bytes_per_sec"] == 9.0e7  # measured won


def test_probe_degraded_preserves_declared_and_writes_errors(tmp_path):
    declared = {"bandwidth_bytes_per_sec": 123, "per_stream_bytes_per_sec": 45}
    (tmp_path / "network.json").write_text(json.dumps(declared))
    p = _probe(tmp_path)
    p.measure_rtt = lambda: 5.0
    p.measure_throughput = lambda: None  # probe failed
    summary = p.probe()

    assert summary["status"] == "degraded"
    # The declared artifact is untouched -- it stays the planner's fallback.
    assert json.loads((tmp_path / "network.json").read_text()) == declared
    assert (tmp_path / "network-errors.json").exists()


def test_probe_degrades_on_contract_invalid_measurement(tmp_path):
    p = _probe(tmp_path)
    p.measure_rtt = lambda: 5.0
    p.measure_throughput = lambda: (0.0, 0.0)  # non-positive -> fails the contract
    summary = p.probe()
    assert summary["status"] == "degraded"
    assert not (tmp_path / "network.json").exists()
    assert (tmp_path / "network-errors.json").exists()


def test_from_env_returns_zero_even_when_degraded(tmp_path, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr(PgCyanNetworkProbe, "measure_rtt", lambda self: None)
    monkeypatch.setattr(PgCyanNetworkProbe, "measure_throughput", lambda self: None)
    # Optional collector: a degraded probe must not fail the walk.
    assert probe_network_from_env() == 0
