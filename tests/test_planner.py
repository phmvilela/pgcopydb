"""
Unit tests for the migration planner (`pg_cyan.cli plan`, issue #38).

DB-free by nature: the planner is a pure offline transform of the collected
source artifacts. Tests exercise the bin-packer, the provable-disjointness
guarantee, the LOB abort gate, the input contract, and the file-writing
behaviour (plan.json + shard-<n>.filters on success; plan-errors.json and no
plan on failure).
"""
import json

import pytest

from pg_cyan.planner import PgCyanPlanner


def _table(schema, name, heap_bytes, index_bytes=0):
    return {
        "schema": schema, "name": name, "relkind": "r",
        "heap_bytes": heap_bytes, "heap_pretty": f"{heap_bytes} bytes",
        "index_bytes": index_bytes, "index_pretty": f"{index_bytes} bytes",
        "row_estimate": 0.0,
    }


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
    assert plan  # non-empty (issue #38 acceptance criterion)
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
