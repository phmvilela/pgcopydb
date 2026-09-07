"""
DB-free tests for the network collector (pg_cyan/collect/netprobe.py).

The socket/COPY I/O is stubbed; these pin the behaviour that matters: a valid,
contract-checked network.json on success, the throughput aggregation math
(now steady-state sampled, #153), the scaling-efficiency attribution, the
near-zero-CPU payload shape, and the graceful-degradation contract -- a probe
failure must preserve a declared network.json (the fallback) and never break
the walk.
"""
import json

import pytest
from psycopg2 import Error as PgError

from pg_cyan.collect import capacity_contract as cc
from pg_cyan.collect.netprobe import (
    PgCyanNetworkProbe,
    _CountingSink,
    _scaling_attribution,
    _steady_state_rate,
    probe_network_from_env,
)


def _probe(tmp_path, **kw):
    kw.setdefault("streams", 1)
    return PgCyanNetworkProbe(
        host="src", port=5432, user="u", password="p", database="db",
        output_dir=str(tmp_path), **kw)


def _uniform_samples(total_bytes, total_secs, n=1000):
    """Evenly-spaced chunk samples for a steady, non-bursty synthetic transfer --
    so a warm-up cutoff doesn't skew the rate away from the plain average."""
    chunk = total_bytes / n
    return [(total_secs * (i + 1) / n, chunk) for i in range(n)]


def test_counting_sink_counts_bytes_and_timestamps_chunks():
    s = _CountingSink(start_time=0.0)
    s.write("xxxxx")
    s.write("yy")
    assert s.n == 7
    assert [b for _, b in s.samples] == [5, 2]


def test_copy_sql_scales_rows_to_stream_bytes_and_avoids_per_row_repeat(tmp_path):
    p = _probe(tmp_path, stream_bytes=4096 * 10)
    sql = p._copy_sql()
    assert "generate_series(1, 10)" in sql
    assert "TO STDOUT" in sql
    assert "repeat(" not in sql  # #153: no per-row function call


def test_steady_state_rate_discards_warmup_window():
    # 1 byte/sec for the first 10s (warm-up), then 100 bytes/sec for 10s.
    samples = [(t, 1) for t in range(10)] + [(10 + t, 100) for t in range(10)]
    rate = _steady_state_rate(samples, total_elapsed=20.0, warmup_fraction=0.5)
    assert rate == pytest.approx(100.0, rel=0.05)


def test_steady_state_rate_falls_back_to_average_when_all_in_warmup():
    samples = [(0.0, 500)]
    rate = _steady_state_rate(samples, total_elapsed=1.0, warmup_fraction=0.9)
    assert rate == pytest.approx(500.0)


def test_steady_state_rate_handles_no_samples():
    assert _steady_state_rate([], total_elapsed=5.0, warmup_fraction=0.2) == 0.0
    assert _steady_state_rate([(0.0, 10)], total_elapsed=0.0, warmup_fraction=0.2) == 0.0


def test_scaling_attribution_flags_source_when_scaling_is_poor():
    # 4 streams, but aggregate barely beats one stream -- classic contention.
    attribution = _scaling_attribution(per_stream=50_000_000.0, bandwidth=55_000_000.0,
                                       streams=4)
    assert attribution["likely_limited_by"] == "source"
    assert attribution["streams"] == 4
    assert attribution["expected_bytes_per_sec"] == pytest.approx(200_000_000.0)
    assert attribution["efficiency"] == pytest.approx(0.275, rel=0.01)


def test_scaling_attribution_flags_network_when_scaling_is_good():
    attribution = _scaling_attribution(per_stream=50_000_000.0, bandwidth=180_000_000.0,
                                       streams=4)
    assert attribution["likely_limited_by"] == "network"


def test_scaling_attribution_none_for_a_single_stream():
    assert _scaling_attribution(per_stream=50_000_000.0, bandwidth=50_000_000.0,
                                streams=1) is None


def test_measure_throughput_single_stream(tmp_path):
    p = _probe(tmp_path, streams=1)
    samples = _uniform_samples(1_000_000, 0.5)
    p._copy_stream = lambda start_time=None: (1_000_000, 0.5, samples)
    per_stream, bandwidth, scaling = p.measure_throughput()
    assert per_stream == pytest.approx(2_000_000, rel=0.05)
    assert bandwidth == pytest.approx(per_stream)  # 1 stream -> no aggregate
    assert scaling is None


def test_measure_throughput_parallel_bandwidth_at_least_per_stream(tmp_path):
    p = _probe(tmp_path, streams=3)
    calls = []

    def fake_copy_stream(start_time=None):
        calls.append(1)
        samples = _uniform_samples(500_000, 0.25)
        return 500_000, 0.25, samples

    p._copy_stream = fake_copy_stream
    per_stream, bandwidth, scaling = p.measure_throughput()
    assert per_stream == pytest.approx(2_000_000, rel=0.05)
    assert bandwidth >= per_stream
    assert len(calls) == 1 + 3  # one single-stream probe + three parallel
    assert scaling is not None and scaling["streams"] == 3


def test_measure_throughput_none_when_stream_fails(tmp_path):
    p = _probe(tmp_path)

    def boom(start_time=None):
        raise PgError("no COPY")

    p._copy_stream = boom
    assert p.measure_throughput() is None
    assert p.errors


def test_probe_success_writes_valid_network_json(tmp_path):
    p = _probe(tmp_path)
    p.measure_rtt = lambda: 12.3456
    p.measure_throughput = lambda: (50_000_000.0, 180_000_000.0, None)
    summary = p.probe()

    assert summary["status"] == "success"
    net = json.loads((tmp_path / "network.json").read_text())
    assert cc.validate("network.json", net) == []          # our own contract holds
    assert net["per_stream_bytes_per_sec"] == 50_000_000.0
    assert net["bandwidth_bytes_per_sec"] == 180_000_000.0
    assert net["rtt_ms"] == 12.346
    assert net["_contract_version"] == cc.CONTRACT_VERSION
    assert "aggregate_scaling" not in net


def test_probe_success_includes_scaling_attribution(tmp_path):
    p = _probe(tmp_path, streams=4)
    p.measure_rtt = lambda: 5.0
    scaling = {"streams": 4, "expected_bytes_per_sec": 200_000_000.0,
               "efficiency": 0.275, "likely_limited_by": "source"}
    p.measure_throughput = lambda: (50_000_000.0, 55_000_000.0, scaling)
    summary = p.probe()

    assert summary["status"] == "success"
    net = json.loads((tmp_path / "network.json").read_text())
    assert cc.validate("network.json", net) == []
    assert net["aggregate_scaling"] == scaling


def test_probe_without_rtt_still_succeeds(tmp_path):
    p = _probe(tmp_path)
    p.measure_rtt = lambda: None
    p.measure_throughput = lambda: (50_000_000.0, 50_000_000.0, None)
    assert p.probe()["status"] == "success"
    net = json.loads((tmp_path / "network.json").read_text())
    assert "rtt_ms" not in net
    assert cc.validate("network.json", net) == []


def test_probe_success_overrides_declared(tmp_path):
    (tmp_path / "network.json").write_text(json.dumps(
        {"bandwidth_bytes_per_sec": 1, "per_stream_bytes_per_sec": 1}))
    p = _probe(tmp_path)
    p.measure_rtt = lambda: 5.0
    p.measure_throughput = lambda: (9.0e7, 9.0e7, None)
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
    p.measure_throughput = lambda: (0.0, 0.0, None)  # non-positive -> fails the contract
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
