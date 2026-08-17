"""
Unit tests for the planner's capacity model (concurrency budget + shard shape).

See also: pg_cyan/collect/capacity.py

DB-free by nature: the capacity model is a pure transform of the collected
capacity artifacts into ``C*`` and a shard shape. Tests cover each ceiling term
binding in turn, the min selection + binding-term attribution, the BDP/RTT
network fill, the constraints clamp layer (tighten only), shard shaping bounds,
the fallback ladder when inputs are missing, and determinism.
"""
import pytest

from pg_cyan.collect.capacity import (
    DEFAULT_PER_STREAM_BYTES_PER_SEC,
    compute_budget,
)

# A deliberately permissive baseline: every term is far from binding, and a
# 1 MiB/s measured per-stream keeps the arithmetic easy (a ceiling in MiB/s maps
# 1:1 to a stream count). Individual tests then tighten one input to make it the
# bottleneck.
_PER_STREAM = 1024 * 1024
_NETWORK = {"bandwidth_bytes_per_sec": 10**15, "per_stream_bytes_per_sec": _PER_STREAM}
_SOURCE = {"read_throughput_bytes_per_sec": 10**12,
           "max_connections": 100_000, "current_connections": 0}
_TARGET = {"write_throughput_bytes_per_sec": 10**12,
           "max_connections": 100_000, "current_connections": 0}
_CLUSTER = {"allocatable_cpu": 1_000_000.0, "node_count": 1000, "max_pods_per_node": 1000}


def _budget(*, constraints=None, source=_SOURCE, target=_TARGET,
            network=_NETWORK, cluster=_CLUSTER, table_count=100,
            fallback_shards=4, fallback_table_jobs=8):
    return compute_budget(
        constraints=constraints,
        source_capacity=source, target_capacity=target,
        network=network, cluster_capacity=cluster,
        table_count=table_count,
        fallback_shards=fallback_shards, fallback_table_jobs=fallback_table_jobs,
    )


# --------------------------------------------------------------- per-stream rate

def test_measured_per_stream_wins():
    b = _budget(network={"bandwidth_bytes_per_sec": 1e12,
                         "per_stream_bytes_per_sec": 7 * 1024 * 1024})
    assert b.per_stream_bytes_per_sec == 7 * 1024 * 1024


def test_per_stream_capped_at_realistic_ceiling_on_a_fat_link():
    # A fat low-latency link would imply an implausibly fast single stream; the
    # per-connection ceiling caps it so parallelism stays meaningful.
    b = _budget(network={"bandwidth_bytes_per_sec": 10 * 1024**3, "rtt_ms": 1})
    assert b.per_stream_bytes_per_sec == DEFAULT_PER_STREAM_BYTES_PER_SEC


def test_high_rtt_shrinks_the_single_stream():
    # window (4 MiB) / rtt (0.5 s) = 8 MiB/s, below the ceiling -> it binds.
    b = _budget(network={"bandwidth_bytes_per_sec": 10 * 1024**3, "rtt_ms": 500})
    assert b.per_stream_bytes_per_sec == pytest.approx(8 * 1024 * 1024)


# ------------------------------------------------------------- each term binding

def test_source_connections_bind():
    b = _budget(source={**_SOURCE, "max_connections": 12, "current_connections": 2})
    assert b.binding_term == "source_connections"
    assert b.c_star == 10  # 12 - 2


def test_target_write_binds():
    # Target can absorb only 3 streams' worth of writes.
    b = _budget(target={**_TARGET, "write_throughput_bytes_per_sec": 3 * _PER_STREAM})
    assert b.binding_term == "target_write"
    assert b.c_star == 3


def test_network_fill_binds():
    # A 10 MiB/s link at 1 MiB/s per stream = 10 streams fill the pipe.
    b = _budget(network={"bandwidth_bytes_per_sec": 10 * _PER_STREAM,
                         "per_stream_bytes_per_sec": _PER_STREAM})
    assert b.binding_term == "network_fill"
    assert b.c_star == 10


def test_k8s_cpu_binds():
    b = _budget(cluster={"allocatable_cpu": 1.0, "cpu_per_stream": 0.25,
                         "node_count": 100, "max_pods_per_node": 100})
    assert b.binding_term == "k8s_cpu"
    assert b.c_star == 4  # 1.0 / 0.25


def test_baseline_utilization_discounts_the_ceiling():
    # 80% of source read already in use -> only 20% left for the copy.
    full = _budget(source={"read_throughput_bytes_per_sec": 50 * 10 * 1024 * 1024,
                           "max_connections": 10_000})
    busy = _budget(source={"read_throughput_bytes_per_sec": 50 * 10 * 1024 * 1024,
                           "baseline_read_utilization": 0.8, "max_connections": 10_000})
    assert busy.terms["source_read"] == pytest.approx(full.terms["source_read"] * 0.2)


# ------------------------------------------------------------------ constraints

def test_constraints_max_concurrency_clamps_c_star():
    b = _budget(constraints={"max_concurrency": 5})
    assert b.c_star == 5
    assert b.binding_term == "constraints_max_concurrency"
    assert any("max_concurrency" in c for c in b.clamps)


def test_constraints_only_tighten_never_loosen():
    # A max_concurrency well above the physical budget must not raise C*.
    physical = _budget().c_star
    b = _budget(constraints={"max_concurrency": physical + 1000})
    assert b.c_star == physical
    assert b.binding_term != "constraints_max_concurrency"


def test_constraints_max_source_connections_tightens_headroom():
    b = _budget(source={**_SOURCE, "max_connections": 10_000},
                target={**_TARGET, "max_connections": 10_000},
                network={"bandwidth_bytes_per_sec": 1e12, "per_stream_bytes_per_sec": 1e6},
                cluster={"allocatable_cpu": 1e9},
                constraints={"max_source_connections": 7})
    assert b.terms["source_connections"] == 7
    assert b.binding_term == "source_connections"
    assert b.c_star == 7


def test_max_source_connections_applies_without_source_capacity():
    b = _budget(source=None, constraints={"max_source_connections": 9})
    assert b.terms["source_connections"] == 9


# --------------------------------------------------------------- shard shaping

def test_shard_count_derived_from_c_star_and_job_ceiling():
    # C* = 20, default 8 jobs/shard -> ceil(20/8) = 3 shards.
    b = _budget(source={**_SOURCE, "max_connections": 20})
    assert b.c_star == 20
    assert b.shard_count == 3
    assert b.shard_count * b.table_jobs >= b.c_star


def test_min_shards_raises_the_floor():
    b = _budget(source={**_SOURCE, "max_connections": 4}, constraints={"min_shards": 6})
    assert b.shard_count == 6
    assert any("min_shards" in c for c in b.clamps)


def test_max_shards_caps_the_count():
    b = _budget(source={**_SOURCE, "max_connections": 100}, constraints={"max_shards": 3})
    assert b.shard_count == 3
    assert any("max_shards" in c for c in b.clamps)


def test_shard_count_never_exceeds_table_count():
    b = _budget(source={**_SOURCE, "max_connections": 100}, table_count=2)
    assert b.shard_count == 2
    assert any("table_count" in c for c in b.clamps)


def test_cluster_pod_capacity_caps_shard_count():
    b = _budget(source={**_SOURCE, "max_connections": 100},
                cluster={"allocatable_cpu": 1e9, "node_count": 1, "max_pods_per_node": 2})
    assert b.shard_count <= 2
    assert any("cluster_pod_capacity" in c for c in b.clamps)


def test_max_table_jobs_per_shard_caps_jobs():
    b = _budget(source={**_SOURCE, "max_connections": 40},
                constraints={"max_table_jobs_per_shard": 4})
    assert b.table_jobs <= 4


# ------------------------------------------------------------- fallback ladder

def test_no_capacity_inputs_degrade_to_static_defaults():
    b = compute_budget(
        constraints=None, source_capacity=None, target_capacity=None,
        network=None, cluster_capacity=None,
        table_count=100, fallback_shards=4, fallback_table_jobs=8,
    )
    assert b.degraded is True
    assert b.c_star is None
    assert b.binding_term == "none"
    assert b.shard_count == 4
    assert b.table_jobs == 8
    assert b.per_stream_bytes_per_sec == DEFAULT_PER_STREAM_BYTES_PER_SEC


def test_partial_inputs_still_produce_a_budget():
    # Only a target-write ceiling is known; the budget rests on that alone.
    b = compute_budget(
        constraints=None, source_capacity=None, target_capacity=None,
        network=None,
        cluster_capacity={"allocatable_cpu": 2.0, "cpu_per_stream": 0.5},
        table_count=100, fallback_shards=4, fallback_table_jobs=8,
    )
    assert b.degraded is False
    assert b.binding_term == "k8s_cpu"
    assert b.c_star == 4


def test_degraded_still_clamps_shards_to_table_count():
    b = compute_budget(
        constraints=None, source_capacity=None, target_capacity=None,
        network=None, cluster_capacity=None,
        table_count=2, fallback_shards=4, fallback_table_jobs=8,
    )
    assert b.shard_count == 2


def test_malformed_fields_never_raise_and_drop_their_term():
    b = _budget(source={"read_throughput_bytes_per_sec": "lots",
                        "max_connections": None},
                network={"bandwidth_bytes_per_sec": "fast"})
    # Junk source/network terms drop out; the target/cluster terms still stand.
    assert "source_read" not in b.terms
    assert "network_fill" not in b.terms
    assert b.c_star is not None


# ---------------------------------------------------------------- determinism

def test_budget_is_deterministic():
    a = _budget(constraints={"max_concurrency": 17})
    b = _budget(constraints={"max_concurrency": 17})
    assert a.as_plan_block() == b.as_plan_block()
