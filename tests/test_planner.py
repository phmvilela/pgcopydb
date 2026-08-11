"""
Unit tests for the migration planner (`pg_cyan.cli plan`).

See also: pg_cyan/planner.py

DB-free by nature: the planner is a pure offline transform of the collected
source artifacts. Tests exercise the bin-packer, the provable-disjointness
guarantee, the LOB abort gate, the input contract, and the file-writing
behaviour (plan.json + shard-<n>.filters on success; plan-errors.json and no
plan on failure).
"""
import json

import pytest

from pg_cyan.cost_model import CostModel
from pg_cyan.planner import PgCyanPlanner


def _table(schema, name, heap_bytes, index_bytes=0, **cost_fields):
    return {
        "schema": schema, "name": name, "relkind": "r",
        "heap_bytes": heap_bytes, "heap_pretty": f"{heap_bytes} bytes",
        "index_bytes": index_bytes, "index_pretty": f"{index_bytes} bytes",
        "row_estimate": 0.0,
        **cost_fields,
    }


def _heap_lpt(tables, n):
    """Baseline: plain heap-bytes LPT packing, for comparison."""
    shards = [[] for _ in range(n)]
    loads = [0] * n
    for t in sorted(tables, key=lambda x: x["heap_bytes"], reverse=True):
        i = loads.index(min(loads))
        shards[i].append(t)
        loads[i] += t["heap_bytes"]
    return shards


def _keys(shards):
    """Normalize a packing to a comparable set of per-shard key sets."""
    return sorted(
        tuple(sorted(f'{t["schema"]}.{t["name"]}' for t in shard))
        for shard in shards
    )


def _cost_imbalance(shards, model):
    """Max-min of per-shard total copy time under ``model``."""
    loads = [sum(model.estimate_seconds(t) for t in shard) for shard in shards]
    return max(loads) - min(loads)


SAMPLE_TABLES = [
    _table("public", "orders", 1000, 400),
    _table("public", "order_lines", 800, 300),
    _table("public", "customers", 600, 200),
    _table("public", "addresses", 400, 100),
    _table("public", "widgets", 200, 50),
    _table("public", "gadgets", 100, 25),
]


def _write_inputs(inputs_dir, tables=SAMPLE_TABLES, lob_count=0,
                  include_metrics=True, include_lobs=True):
    """Write the metrics + largeobjects artifacts a planner run needs."""
    if include_metrics:
        (inputs_dir / PgCyanPlanner.METRICS_FILENAME).write_text(json.dumps({
            "component": "source-pg-metrics", "status": "success",
            "tables": tables,
        }))
    if include_lobs:
        (inputs_dir / PgCyanPlanner.LOBS_FILENAME).write_text(json.dumps({
            "component": "source-pg-largeobjects", "status": "success",
            "lob_count": lob_count,
        }))


def _write_capacity(inputs_dir, **artifacts):
    """Write the optional capacity artifacts (name without .json -> dict)."""
    names = {
        "constraints": PgCyanPlanner.CONSTRAINTS_FILENAME,
        "source_capacity": PgCyanPlanner.SOURCE_CAPACITY_FILENAME,
        "target_capacity": PgCyanPlanner.TARGET_CAPACITY_FILENAME,
        "network": PgCyanPlanner.NETWORK_FILENAME,
        "cluster_capacity": PgCyanPlanner.CLUSTER_CAPACITY_FILENAME,
    }
    for key, payload in artifacts.items():
        (inputs_dir / names[key]).write_text(json.dumps(payload))


def make_planner(inputs_dir, output_dir, **kwargs):
    return PgCyanPlanner(
        inputs_dir=str(inputs_dir), output_dir=str(output_dir),
        run_id="test-run", **kwargs,
    )


# ------------------------------------------------------------------ bin-packing

def test_bin_pack_partitions_every_table_disjointly():
    planner = PgCyanPlanner(shards=4)
    shards = planner.bin_pack(SAMPLE_TABLES, 4)

    assigned = [planner._table_key(t) for shard in shards for t in shard]
    # Disjoint: no table key repeats.
    assert len(assigned) == len(set(assigned))
    # Complete: exactly the input set, nothing added or dropped.
    assert set(assigned) == {planner._table_key(t) for t in SAMPLE_TABLES}
    assert planner._validate_disjoint(shards, SAMPLE_TABLES)


def test_bin_pack_balances_heap_bytes():
    planner = PgCyanPlanner(shards=2)
    shards = planner.bin_pack(SAMPLE_TABLES, 2)
    loads = [sum(planner._heap_bytes(t) for t in s) for s in shards]
    # LPT keeps the two shards within the largest single table of each other.
    assert abs(loads[0] - loads[1]) <= max(planner._heap_bytes(t) for t in SAMPLE_TABLES)
    assert sum(loads) == sum(planner._heap_bytes(t) for t in SAMPLE_TABLES)


# ------------------------------------------------------ time-based cost weight

def test_cost_packing_beats_heap_packing_on_a_skewed_schema():
    # A bloated table (huge heap, mostly dead tuples) plus three tight tables.
    # Heap-bytes LPT isolates the bloated table on its own shard and crams the
    # rest together, leaving the shards badly imbalanced by *copy time*; the
    # time-based weight sees through the bloat and balances the real work.
    model = CostModel(per_stream_bytes_per_sec=1000.0, per_stream_rows_per_sec=1000.0)
    tables = [
        _table("public", "bloated", 10_000, dead_tuple_ratio=0.9),  # ~1000 live
        _table("public", "a", 1_000),
        _table("public", "b", 1_000),
        _table("public", "c", 1_000),
    ]
    planner = PgCyanPlanner(cost_model_=model)

    cost_shards = planner.bin_pack(tables, 2)
    heap_shards = _heap_lpt(tables, 2)

    assert _cost_imbalance(cost_shards, model) < _cost_imbalance(heap_shards, model)
    # The bloated table no longer gets a shard to itself.
    assert _keys(cost_shards) != _keys(heap_shards)


def test_narrow_hot_table_is_not_undercosted():
    # A tiny-heap, billion-row table would look free to a byte packer; the
    # rows term must make it the costliest object so it isn't buried.
    model = CostModel(per_stream_bytes_per_sec=1000.0, per_stream_rows_per_sec=1000.0)
    planner = PgCyanPlanner(cost_model_=model)
    hot = _table("public", "events", 100, row_estimate=1_000_000)
    fat = _table("public", "archive", 5_000)
    assert planner._cost(hot) > planner._cost(fat)


def test_cost_packing_matches_heap_packing_without_extended_fields(tmp_path):
    # With only heap_bytes present (no dead ratio / rows), the cost is a constant
    # rescaling of heap bytes, so the assignment is identical to plain heap-bytes
    # packing -- the graceful-degradation guarantee.
    planner = PgCyanPlanner()
    assert _keys(planner.bin_pack(SAMPLE_TABLES, 3)) == _keys(_heap_lpt(SAMPLE_TABLES, 3))


def test_validate_disjoint_rejects_overlap():
    planner = PgCyanPlanner()
    dup = _table("public", "orders", 1000)
    # Same table in two shards must be rejected.
    assert planner._validate_disjoint([[dup], [dup]], [dup]) is False
    assert planner.errors


def test_validate_disjoint_rejects_incomplete_coverage():
    planner = PgCyanPlanner()
    a = _table("public", "a", 10)
    b = _table("public", "b", 20)
    # 'b' never assigned -> coverage incomplete.
    assert planner._validate_disjoint([[a]], [a, b]) is False


# --------------------------------------------------------------------- LOB gate

def test_lob_gate_passes_on_zero():
    planner = PgCyanPlanner()
    assert planner.check_lob_gate({"lob_count": 0}) is True


def test_lob_gate_aborts_when_large_objects_present():
    planner = PgCyanPlanner()
    assert planner.check_lob_gate({"lob_count": 5}) is False
    assert any("LOB abort gate" in e for e in planner.errors)


def test_lob_gate_aborts_when_count_missing():
    planner = PgCyanPlanner()
    assert planner.check_lob_gate({}) is False
    assert planner.errors


# ----------------------------------------------------------------- plan() paths

def test_plan_writes_plan_and_disjoint_filters(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=0)

    planner = make_planner(inputs, output, shards=4)
    summary = planner.plan()

    assert summary["status"] == "success", planner.errors
    assert summary["shard_count"] == 4
    assert summary["table_count"] == len(SAMPLE_TABLES)

    plan_file = output / PgCyanPlanner.OUTPUT_FILENAME
    assert plan_file.exists()
    assert not (output / PgCyanPlanner.ERROR_FILENAME).exists()

    plan = json.loads(plan_file.read_text())
    assert plan  # non-empty
    assert plan["run_id"] == "test-run"
    assert plan["snapshot"]["mode"] == PgCyanPlanner.DEFAULT_SNAPSHOT_MODE
    assert plan["snapshot"]["not_consistent"] is False
    assert plan["publication"]["name"] == PgCyanPlanner.DEFAULT_PUBLICATION
    assert plan["slot"]["name"] == PgCyanPlanner.DEFAULT_SLOT
    assert plan["fk_graph"]["status"] == "deferred"
    assert len(plan["data_shards"]) == 4
    assert plan["totals"]["table_count"] == len(SAMPLE_TABLES)

    # One filter file per shard, and the include-only-table lists are a
    # provably-disjoint partition of the source tables.
    all_listed = []
    for i in range(4):
        body = (output / f"shard-{i}.filters").read_text()
        assert body.startswith("[include-only-table]\n")
        listed = [ln for ln in body.splitlines()[1:] if ln]
        all_listed.extend(listed)
    assert len(all_listed) == len(set(all_listed))  # disjoint
    assert set(all_listed) == {f'{t["schema"]}.{t["name"]}' for t in SAMPLE_TABLES}


def test_plan_records_cost_model_and_per_shard_seconds(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=0)

    planner = make_planner(inputs, output, shards=2)
    assert planner.plan()["status"] == "success"

    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    assert plan["cost_model"]["weight"] == "estimated_copy_seconds"
    assert plan["cost_model"]["constants"]["per_stream_bytes_per_sec"] > 0
    # Every shard carries its estimated copy time, and they sum to the total.
    per_shard = [s["est_copy_seconds"] for s in plan["data_shards"]]
    assert all(s >= 0 for s in per_shard)
    assert plan["totals"]["est_copy_seconds"] == pytest.approx(sum(per_shard))


# ------------------------------------------------------- capacity budget (plan)

def test_plan_without_capacity_degrades_to_static_shards(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=0)

    planner = make_planner(inputs, output, shards=4)
    assert planner.plan()["status"] == "success"

    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    conc = plan["concurrency"]
    assert conc["degraded"] is True
    assert conc["c_star"] is None
    assert conc["binding_term"] == "none"
    # Static fallback: the requested shard count (clamped to the table set).
    assert plan["totals"]["shard_count"] == 4
    # No budget -> no cost-driven splitting, and no meaningful bottleneck.
    assert plan["splitting"]["enabled"] is False
    assert plan["bottleneck"] == "none"


def _skewed_tables(giant_key="id"):
    gib = 1024 ** 3
    return [
        _table("public", "big", 100 * gib, splittable_key=giant_key),
        _table("public", "a", 1 * gib, splittable_key="id"),
        _table("public", "b", 1 * gib, splittable_key="id"),
        _table("public", "c", 1 * gib, splittable_key="id"),
    ]


def _split_capacity(inputs):
    _write_capacity(
        inputs,
        source_capacity={"max_connections": 16, "current_connections": 0},
        network={"bandwidth_bytes_per_sec": 10 ** 12,
                 "per_stream_bytes_per_sec": 100 * 1024 * 1024},
        constraints={"max_table_jobs_per_shard": 8},
    )


def test_plan_splits_a_dominant_table_and_lowers_makespan(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, tables=_skewed_tables(), lob_count=0)
    _split_capacity(inputs)

    planner = make_planner(inputs, output)
    assert planner.plan()["status"] == "success"

    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    sp = plan["splitting"]
    assert sp["enabled"] is True
    big = next(s for s in sp["splits"] if s["table"] == "public.big")
    assert big["subtasks"] >= 2 and big["reason"] == "split"
    # Splitting the dominant table shortens the predicted makespan.
    assert sp["predicted_makespan_seconds"] < sp["makespan_without_splitting_seconds"]
    # Not a single-table floor -> the bottleneck is the stream budget's binding
    # term, canonicalized, with the raw term preserved in the attribution block.
    assert plan["bottleneck"] == "source-connections"
    assert plan["attribution"]["binding_term"] == "source_connections"
    assert plan["attribution"]["remediation"]
    # The shard holding the split table carries a byte (int) split threshold.
    assert any(isinstance(s["split_tables_larger_than"], int) for s in plan["data_shards"])

    # Disjointness invariant still holds over the split plan.
    listed = []
    for i in range(plan["totals"]["shard_count"]):
        body = (output / f"shard-{i}.filters").read_text()
        listed.extend(ln for ln in body.splitlines()[1:] if ln)
    assert len(listed) == len(set(listed))
    assert set(listed) == {f'{t["schema"]}.{t["name"]}' for t in _skewed_tables()}


# ---------------------------------------------------- feasibility gate (plan)

def test_plan_passes_feasibility_and_emits_resume(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, tables=_skewed_tables(), lob_count=0)
    _write_capacity(
        inputs,
        source_capacity={"max_connections": 16, "current_connections": 0,
                         # Roomy WAL disk relative to the write rate -> passes.
                         "wal_disk_free_bytes": 10 ** 15,
                         "wal_generation_bytes_per_sec": 1_000_000},
        network={"bandwidth_bytes_per_sec": 10 ** 12,
                 "per_stream_bytes_per_sec": 100 * 1024 * 1024},
        constraints={"max_table_jobs_per_shard": 8},
    )

    planner = make_planner(inputs, output)
    assert planner.plan()["status"] == "success"

    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    assert plan["feasibility"]["status"] == "pass"
    assert plan["feasibility"]["binding_deadline_source"] == "wal_retention"
    resume = plan["resume"]
    assert resume["enabled"] is True
    assert resume["checkpoints"] == [f"shard-{i}" for i in range(plan["totals"]["shard_count"])]


def test_plan_aborts_and_writes_no_plan_when_wal_deadline_breached(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, tables=_skewed_tables(), lob_count=0)
    _write_capacity(
        inputs,
        source_capacity={"max_connections": 16, "current_connections": 0,
                         # Tiny WAL disk + high write rate -> fills before copy ends.
                         "wal_disk_free_bytes": 1024,
                         "wal_generation_bytes_per_sec": 1_000_000_000},
        network={"bandwidth_bytes_per_sec": 10 ** 12,
                 "per_stream_bytes_per_sec": 100 * 1024 * 1024},
    )

    planner = make_planner(inputs, output)
    summary = planner.plan()

    assert summary["status"] == "failure"
    # Refused like the LOB gate: no plan, reasons (with numbers) in the error.
    assert not (output / PgCyanPlanner.OUTPUT_FILENAME).exists()
    payload = json.loads((output / PgCyanPlanner.ERROR_FILENAME).read_text())
    assert any("Feasibility abort" in e and "wal_retention" in e for e in payload["errors"])


def test_plan_feasibility_is_noop_without_deadline_inputs(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, tables=_skewed_tables(), lob_count=0)
    # Capacity present (so a budget is computed) but no WAL/window inputs.
    _write_capacity(
        inputs,
        source_capacity={"max_connections": 16, "current_connections": 0},
        network={"bandwidth_bytes_per_sec": 10 ** 12,
                 "per_stream_bytes_per_sec": 100 * 1024 * 1024},
    )

    planner = make_planner(inputs, output)
    assert planner.plan()["status"] == "success"

    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    assert plan["feasibility"]["status"] == "not_evaluated"


def test_plan_flags_unsplittable_giant_as_single_table_bottleneck(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, tables=_skewed_tables(giant_key="none"), lob_count=0)
    _split_capacity(inputs)

    planner = make_planner(inputs, output)
    assert planner.plan()["status"] == "success"

    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    assert plan["bottleneck"] == "single-table"
    assert plan["splitting"]["single_table_floor"] is True
    giant = next(s for s in plan["splitting"]["splits"] if s["table"] == "public.big")
    assert giant["reason"] == "unsplittable_giant"
    assert giant["subtasks"] == 1


def test_plan_uses_capacity_budget_for_shard_shape(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=0)  # six tables
    # Source connections cap the budget at C* = 6; two jobs/shard -> 3 shards.
    _write_capacity(
        inputs,
        source_capacity={"max_connections": 6, "current_connections": 0},
        network={"bandwidth_bytes_per_sec": 10**12, "per_stream_bytes_per_sec": 1024 * 1024},
        constraints={"max_table_jobs_per_shard": 2},
    )

    planner = make_planner(inputs, output, shards=4)
    assert planner.plan()["status"] == "success"

    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    conc = plan["concurrency"]
    assert conc["degraded"] is False
    assert conc["c_star"] == 6
    assert conc["binding_term"] == "source_connections"
    assert plan["totals"]["shard_count"] == 3
    assert all(s["table_jobs"] == 2 for s in plan["data_shards"])


def test_plan_network_probe_calibrates_the_cost_model(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=0)
    measured = 12 * 1024 * 1024
    _write_capacity(inputs, network={"bandwidth_bytes_per_sec": 10**12,
                                     "per_stream_bytes_per_sec": measured})

    planner = make_planner(inputs, output)
    assert planner.plan()["status"] == "success"

    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    # The measured single-stream throughput flows into the copy-time weight.
    assert plan["cost_model"]["constants"]["per_stream_bytes_per_sec"] == measured
    assert plan["concurrency"]["per_stream_bytes_per_sec"] == measured


# ------------------------------------------- explainability + calibration (plan)

def _write_history(inputs_dir, observed_mib_per_sec=25.0, n=6, mib_each=500):
    """A copylog-history.json of n wide tables copied at a fixed real rate."""
    mib = 1024 * 1024
    dur = mib_each / observed_mib_per_sec
    (inputs_dir / PgCyanPlanner.HISTORY_FILENAME).write_text(json.dumps({
        "component": "copylog-history",
        "observations": [
            {"schema": "public", "name": f"h{i}", "heap_bytes": mib_each * mib,
             "row_estimate": 0, "duration_seconds": dur}
            for i in range(n)
        ],
    }))


def _run_plan(inputs, output):
    planner = make_planner(inputs, output)
    assert planner.plan()["status"] == "success"
    return json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())


def test_plan_calibrates_cost_model_from_history_and_lengthens_prediction(tmp_path):
    inputs = tmp_path / "in"
    inputs.mkdir()
    # Tables with real heap, so the bytes term drives the copy-time estimate.
    tables = [_table("public", f"t{i}", heap_bytes=200 * 1024 * 1024) for i in range(4)]
    _write_inputs(inputs, tables=tables, lob_count=0)

    base = _run_plan(inputs, tmp_path / "out-base")

    # History shows the real link is half as fast (25 MiB/s vs the 50 default).
    _write_history(inputs, observed_mib_per_sec=25.0)
    cal = _run_plan(inputs, tmp_path / "out-cal")

    assert base["cost_model"]["calibration"]["source"] == "default"
    assert cal["cost_model"]["calibration"]["source"] == "history"
    # A slower calibrated throughput -> a longer predicted copy time.
    assert cal["totals"]["est_copy_seconds"] > base["totals"]["est_copy_seconds"]
    # Provenance records where the byte constant came from in each plan.
    assert cal["cost_model"]["provenance"]["per_stream_bytes_per_sec"] == "history-calibrated"
    assert base["cost_model"]["provenance"]["per_stream_bytes_per_sec"] == "default"
    # And the replay error does not grow once calibrated.
    err = cal["cost_model"]["calibration"]["predicted_error_ratio"]
    assert err["before"] is not None and err["after"] <= err["before"]


def test_plan_cost_model_block_is_auditable(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=0)  # no history, no capacity

    plan = _run_plan(inputs, output)
    cm = plan["cost_model"]
    assert cm["formula"] and cm["assumptions"]
    assert set(cm["provenance"]) == {
        "per_stream_bytes_per_sec", "per_stream_rows_per_sec", "toast_overhead_factor"}
    # Absent history -> default constants (the documented fallback).
    assert cm["calibration"]["source"] == "default"
    assert cm["provenance"]["per_stream_bytes_per_sec"] == "default"
    # The attribution block agrees with the top-level bottleneck.
    assert plan["attribution"]["bottleneck"] == plan["bottleneck"]
    assert plan["attribution"]["remediation"]


def test_plan_bottleneck_is_canonical_with_capacity(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=0)
    # Target write throughput is the tightest ceiling -> canonical target-write.
    _write_capacity(
        inputs,
        target_capacity={"max_connections": 500,
                         "write_throughput_bytes_per_sec": 20 * 1024 * 1024},
        network={"bandwidth_bytes_per_sec": 10**12},
    )

    plan = _run_plan(inputs, output)
    assert plan["bottleneck"] == "target-write"
    assert plan["attribution"]["binding_term"] == "target_write"


def test_plan_frozen_source_sets_not_consistent(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=0)

    planner = make_planner(inputs, output, snapshot_mode="frozen-source")
    summary = planner.plan()

    assert summary["status"] == "success"
    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    assert plan["snapshot"]["mode"] == "frozen-source"
    assert plan["snapshot"]["not_consistent"] is True


def test_plan_clamps_shards_to_table_count(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    two = SAMPLE_TABLES[:2]
    _write_inputs(inputs, tables=two, lob_count=0)

    planner = make_planner(inputs, output, shards=4)
    summary = planner.plan()

    # Only two tables -> at most two non-empty shards, no empty filter files.
    assert summary["status"] == "success"
    assert summary["shard_count"] == 2
    assert not (output / "shard-2.filters").exists()


def test_plan_aborts_and_writes_no_plan_when_lobs_present(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, lob_count=3)

    planner = make_planner(inputs, output)
    summary = planner.plan()

    assert summary["status"] == "failure"
    # No plan is written on the LOB abort.
    assert not (output / PgCyanPlanner.OUTPUT_FILENAME).exists()
    assert not (output / "shard-0.filters").exists()

    error_file = output / PgCyanPlanner.ERROR_FILENAME
    assert error_file.exists()
    payload = json.loads(error_file.read_text())
    assert any("large object" in e.lower() for e in payload["errors"])


def test_plan_fails_when_metrics_missing(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    # Only the lobs artifact present; metrics is required.
    _write_inputs(inputs, lob_count=0, include_metrics=False)

    planner = make_planner(inputs, output)
    summary = planner.plan()

    assert summary["status"] == "failure"
    assert not (output / PgCyanPlanner.OUTPUT_FILENAME).exists()
    payload = json.loads((output / PgCyanPlanner.ERROR_FILENAME).read_text())
    assert any(PgCyanPlanner.METRICS_FILENAME in e for e in payload["errors"])


def test_plan_fails_when_lobs_missing(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, include_lobs=False)

    planner = make_planner(inputs, output)
    summary = planner.plan()

    assert summary["status"] == "failure"
    assert not (output / PgCyanPlanner.OUTPUT_FILENAME).exists()


def test_plan_degrades_on_malformed_metrics(tmp_path):
    # A table row missing schema/name would raise mid-planning; plan() must
    # degrade to the error file rather than propagate.
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    (inputs / PgCyanPlanner.METRICS_FILENAME).write_text(json.dumps({
        "tables": [{"heap_bytes": 100}],  # no schema/name keys
    }))
    (inputs / PgCyanPlanner.LOBS_FILENAME).write_text(json.dumps({"lob_count": 0}))

    planner = make_planner(inputs, output)
    summary = planner.plan()  # must not raise

    assert summary["status"] == "failure"
    assert not (output / PgCyanPlanner.OUTPUT_FILENAME).exists()
    assert (output / PgCyanPlanner.ERROR_FILENAME).exists()


def test_plan_fails_on_empty_table_set(tmp_path):
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    _write_inputs(inputs, tables=[], lob_count=0)

    planner = make_planner(inputs, output)
    summary = planner.plan()

    assert summary["status"] == "failure"
    payload = json.loads((output / PgCyanPlanner.ERROR_FILENAME).read_text())
    assert any("no tables" in e.lower() for e in payload["errors"])
