"""
DB-free tests for the cluster-capacity collector (pg_cyan/collect/clustercap.py).

The Kubernetes node list is stubbed; these pin the aggregation (CPU quantity
parsing, Ready+schedulable filtering, pod-cap max), the merge that preserves a
declared cpu_per_stream, and the optional-collector degradation contract.
"""
import json

from pg_cyan.collect import capacity_contract as cc
from pg_cyan.collect.clustercap import PgCyanClusterCapacity, _parse_cpu, collect_cluster_capacity_from_env


def _node(cpu, pods="110", ready=True, schedulable=True):
    return {"allocatable": {"cpu": cpu, "pods": pods},
            "ready": ready, "schedulable": schedulable}


def test_parse_cpu_handles_cores_and_millicores():
    assert _parse_cpu("4") == 4.0
    assert _parse_cpu("3920m") == 3.92
    assert _parse_cpu(None) is None
    assert _parse_cpu("weird") is None


def test_measure_capacity_sums_ready_schedulable_only(tmp_path):
    c = PgCyanClusterCapacity(output_dir=str(tmp_path))
    nodes = [
        _node("4", pods="110"),
        _node("3920m", pods="250"),
        _node("8", ready=False),            # NotReady -> excluded
        _node("8", schedulable=False),      # cordoned -> excluded
    ]
    cap = c.measure_capacity(nodes)
    assert cap["allocatable_cpu"] == 7.92     # 4 + 3.92
    assert cap["node_count"] == 2
    assert cap["max_pods_per_node"] == 250


def test_measure_capacity_none_when_no_usable_nodes(tmp_path):
    c = PgCyanClusterCapacity(output_dir=str(tmp_path))
    assert c.measure_capacity([_node("4", ready=False)]) is None
    assert c.measure_capacity([]) is None


def test_collect_writes_valid_artifact_and_merges_declared(tmp_path):
    (tmp_path / "cluster-capacity.json").write_text(json.dumps({"cpu_per_stream": 0.5}))
    c = PgCyanClusterCapacity(output_dir=str(tmp_path))
    c._load_nodes = lambda: [_node("4"), _node("4")]
    summary = c.collect()

    assert summary["status"] == "success"
    art = json.loads((tmp_path / "cluster-capacity.json").read_text())
    assert cc.validate("cluster-capacity.json", art) == []
    assert art["allocatable_cpu"] == 8.0 and art["node_count"] == 2
    assert art["cpu_per_stream"] == 0.5       # declared override preserved
    assert art["_contract_version"] == cc.CONTRACT_VERSION


def test_collect_degrades_when_nodes_unreachable_preserving_existing(tmp_path):
    declared = {"allocatable_cpu": 16, "cpu_per_stream": 0.25}
    (tmp_path / "cluster-capacity.json").write_text(json.dumps(declared))
    c = PgCyanClusterCapacity(output_dir=str(tmp_path))
    c._load_nodes = lambda: None              # no in-cluster config / RBAC
    summary = c.collect()

    assert summary["status"] == "degraded"
    assert json.loads((tmp_path / "cluster-capacity.json").read_text()) == declared
    assert (tmp_path / "cluster-capacity-errors.json").exists()


def test_collect_degrades_when_no_usable_node(tmp_path):
    c = PgCyanClusterCapacity(output_dir=str(tmp_path))
    c._load_nodes = lambda: [_node("4", ready=False)]
    assert c.collect()["status"] == "degraded"
    assert not (tmp_path / "cluster-capacity.json").exists()


def test_from_env_returns_zero_even_when_degraded(tmp_path, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr(PgCyanClusterCapacity, "_load_nodes", lambda self: None)
    assert collect_cluster_capacity_from_env() == 0
