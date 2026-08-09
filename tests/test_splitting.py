"""
Unit tests for the planner's adaptive table splitting.

See also: pg_cyan/splitting.py

DB-free by nature: the split planner is a pure transform of the per-table cost
model + the concurrency budget into split decisions and a predicted makespan.
Tests cover the per-stream-share rule, the subtask cap, un-splittable giants (the
single-table floor), the makespan improvement, and the no-budget fallback.
"""
import pytest

from pg_cyan.cost_model import CostModel
from pg_cyan.splitting import plan_splits

# 1 byte/s, 1 row/s -> a table's cost in seconds equals its heap bytes (or rows),
# whichever is larger. Keeps the arithmetic legible.
_MODEL = CostModel(per_stream_bytes_per_sec=1.0, per_stream_rows_per_sec=1.0)


def _t(name, heap_bytes, splittable_key="id", rows=0):
    return {"schema": "public", "name": name, "heap_bytes": heap_bytes,
            "row_estimate": rows, "splittable_key": splittable_key}


def _plan(tables, c_star, max_table_jobs=100):
    return plan_splits(tables, c_star=c_star, max_table_jobs=max_table_jobs,
                       cost_model=_MODEL)


# ------------------------------------------------------------------- the rule

def test_table_within_ideal_share_is_not_split():
    # Four equal tables, C* = 4 -> ideal share = each table exactly; none split.
    tables = [_t(f"t{i}", 100) for i in range(4)]
    plan = _plan(tables, c_star=4)
    assert plan.subtasks_by_table() == {}
    assert not plan.single_table_floor


def test_dominant_splittable_table_is_split_into_subtasks():
    # One 1000 s table among small ones; ideal share = 1300/4 = 325 -> split.
    tables = [_t("big", 1000)] + [_t(f"s{i}", 100) for i in range(3)]
    plan = _plan(tables, c_star=4)
    subtasks = plan.subtasks_by_table()
    assert "public.big" in subtasks
    assert subtasks["public.big"] >= 2  # ceil(1000 / 325) = 4


def test_split_lowers_predicted_makespan():
    tables = [_t("big", 1000)] + [_t(f"s{i}", 100) for i in range(3)]
    plan = _plan(tables, c_star=4)
    # Without splitting the 1000 s table alone floors the makespan; splitting
    # brings it down toward the ideal share.
    assert plan.makespan_without_splitting_seconds == 1000
    assert plan.predicted_makespan_seconds < plan.makespan_without_splitting_seconds


def test_subtasks_capped_by_max_table_jobs():
    # Needs ceil(1000/ (1100/... )) many ranges, but only 3 workers per shard.
    tables = [_t("big", 1000), _t("s", 100)]
    plan = _plan(tables, c_star=10, max_table_jobs=3)
    assert plan.subtasks_by_table()["public.big"] == 3
    # Capped parallelism leaves a residual floor above the ideal share.
    assert plan.predicted_makespan_seconds == pytest.approx(1000 / 3)


def test_single_worker_shard_cannot_split():
    tables = [_t("big", 1000), _t("s", 100)]
    plan = _plan(tables, c_star=10, max_table_jobs=1)
    assert plan.subtasks_by_table() == {}
    assert plan.predicted_makespan_seconds == 1000  # costliest table floors it


# ------------------------------------------------------------ un-splittable

def test_unsplittable_giant_is_flagged_as_single_table_floor():
    tables = [_t("giant", 1000, splittable_key="none")] + [_t(f"s{i}", 100) for i in range(3)]
    plan = _plan(tables, c_star=4)
    assert plan.single_table_floor is True
    assert plan.subtasks_by_table() == {}  # not split
    # It floors the makespan at its full cost.
    assert plan.predicted_makespan_seconds == 1000
    giant = next(d for d in plan.decisions if d.table_key == "public.giant")
    assert giant.reason == "unsplittable_giant"


@pytest.mark.parametrize("key", [None, "", "none", "NONE"])
def test_missing_or_none_key_means_unsplittable(key):
    tables = [_t("giant", 1000, splittable_key=key)] + [_t(f"s{i}", 100) for i in range(3)]
    plan = _plan(tables, c_star=4)
    assert plan.single_table_floor is True


def test_ctid_key_is_splittable():
    tables = [_t("big", 1000, splittable_key="ctid")] + [_t(f"s{i}", 100) for i in range(3)]
    plan = _plan(tables, c_star=4)
    assert "public.big" in plan.subtasks_by_table()
    assert plan.single_table_floor is False


# --------------------------------------------------------------- no budget

def test_no_budget_disables_splitting():
    tables = [_t("big", 1000)] + [_t(f"s{i}", 100) for i in range(3)]
    plan = _plan(tables, c_star=None)
    assert plan.enabled is False
    assert plan.subtasks_by_table() == {}
    # Falls back to the costliest-table floor with no ideal share.
    assert plan.predicted_makespan_seconds == 1000


def test_empty_table_set_is_safe():
    plan = _plan([], c_star=4)
    assert plan.predicted_makespan_seconds == 0
    assert plan.subtasks_by_table() == {}


def test_plan_block_is_serializable_and_deterministic():
    tables = [_t("big", 1000)] + [_t(f"s{i}", 100) for i in range(3)]
    a = _plan(tables, c_star=4).as_plan_block()
    b = _plan(tables, c_star=4).as_plan_block()
    assert a == b
    assert a["enabled"] is True
    assert any(s["table"] == "public.big" for s in a["splits"])
