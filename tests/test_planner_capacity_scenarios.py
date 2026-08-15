"""
End-to-end: the committed capacity scenarios drive the planner off the fallback.

Each fixture under ``tests/fixtures/capacity/`` is an operator-declared bundle.
This runs the real pipeline the way a cluster would -- the static provider
(``capacity-inputs``) explodes the bundle into artifacts, then the planner
(``plan``) consumes them -- and pins the non-degraded outcome each scenario is
built to produce: a specific ``C*`` binding term / canonical bottleneck, a
feasibility pass, or a WAL-deadline abort. If the contract, the provider, or the
planner drifts, exactly one scenario fails and names the break. DB-free.

See also: pg_cyan/capacity_contract.py, pg_cyan/capinputs.py, pg_cyan/planner.py.
"""
import json
import os

import pytest

from pg_cyan.capinputs import PgCyanCapacityInputs
from pg_cyan.planner import PgCyanPlanner

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "capacity")


def _run(tmp_path, scenario):
    """Provider(scenario bundle) -> planner; return the plan summary + inputs dir."""
    inputs, output = tmp_path / "in", tmp_path / "out"
    inputs.mkdir()
    output.mkdir()
    # Required planner inputs: representative metrics + a clean LOB gate.
    with open(os.path.join(FIXTURES, "metrics.json")) as fh:
        (inputs / PgCyanPlanner.METRICS_FILENAME).write_text(fh.read())
    (inputs / PgCyanPlanner.LOBS_FILENAME).write_text(
        json.dumps({"component": "source-pg-largeobjects", "lob_count": 0}))
    # The operator-declared bundle for this scenario.
    with open(os.path.join(FIXTURES, f"{scenario}.json")) as fh:
        (inputs / PgCyanCapacityInputs.DECLARED_FILENAME).write_text(fh.read())

    provided = PgCyanCapacityInputs(
        inputs_dir=str(inputs), output_dir=str(inputs)).provide()
    assert provided["status"] in ("success", "warning"), provided

    summary = PgCyanPlanner(
        inputs_dir=str(inputs), output_dir=str(output), run_id="scenario").plan()
    return summary, output


def _plan(summary, output):
    assert summary["status"] == "success", summary.get("errors")
    plan = json.loads((output / PgCyanPlanner.OUTPUT_FILENAME).read_text())
    assert plan["concurrency"]["degraded"] is False  # NOT the static fallback
    return plan


@pytest.mark.parametrize("scenario,binding,bottleneck", [
    ("source-read-bound", "source_read", "source-read"),
    ("network-bound", "network_fill", "network"),
    ("target-write-bound", "target_write", "target-write"),
])
def test_scenario_binds_expected_term_and_bottleneck(tmp_path, scenario, binding, bottleneck):
    summary, output = _run(tmp_path, scenario)
    plan = _plan(summary, output)
    assert plan["concurrency"]["binding_term"] == binding
    assert plan["bottleneck"] == bottleneck
    assert plan["attribution"]["remediation"]


def test_feasible_scenario_passes_and_calibrates_from_history(tmp_path):
    summary, output = _run(tmp_path, "feasible")
    plan = _plan(summary, output)
    assert plan["feasibility"]["status"] == "pass"
    assert plan["feasibility"]["binding_deadline_source"] == "wal_retention"
    # The declared copylog history calibrated the cost model off its default.
    assert plan["cost_model"]["calibration"]["source"] == "history"


def test_wal_deadline_scenario_aborts_and_writes_no_plan(tmp_path):
    summary, output = _run(tmp_path, "wal-deadline-abort")
    assert summary["status"] == "failure"
    assert not (output / PgCyanPlanner.OUTPUT_FILENAME).exists()
    assert any("wal_retention" in e or "deadline" in e for e in summary["errors"])


def test_scenarios_preserve_shard_disjointness(tmp_path):
    """Whatever shape capacity imposes, the shards still partition the table set."""
    summary, output = _run(tmp_path, "source-read-bound")
    plan = _plan(summary, output)
    listed = []
    for i in range(plan["totals"]["shard_count"]):
        body = (output / f"shard-{i}.filters").read_text()
        listed.extend(ln for ln in body.splitlines()[1:] if ln)
    assert len(listed) == len(set(listed))
    with open(os.path.join(FIXTURES, "metrics.json")) as fh:
        tables = json.load(fh)["tables"]
    assert set(listed) == {f'{t["schema"]}.{t["name"]}' for t in tables}
