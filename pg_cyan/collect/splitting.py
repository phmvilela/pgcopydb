"""
Adaptive table splitting for the migration planner.

A shard's copy is only as fast as its slowest single table: once a table's
estimated copy time exceeds the ideal per-stream share of the work, that one
table floors the whole makespan no matter how the others are balanced. This
module decides which tables to break into parallel range sub-tasks so no single
table dominates.

The rule, given the concurrency budget ``C*`` and the per-table cost model:

    ideal_share = total_cost / C*                # the best any stream can do
    split a table  iff  t(table) > ideal_share  and it is splittable

A split table is carved into ``subtasks = ceil(t / ideal_share)`` parallel
ranges, capped at the per-shard job ceiling (within-shard parallelism can't
exceed ``table_jobs``). The planner expresses this to pgcopydb as a decision plus
a ``split_tables_larger_than`` threshold -- pgcopydb carves the actual ctid ranges
at run time; the planner emits no explicit boundaries (that would need key
statistics from the collector and per-range filters -- deferred).

An oversized table that is **not** splittable (``splittable_key == "none"``) stays
whole and is flagged as a makespan floor -- the ``single-table`` bottleneck the
operator must address by other means (partitioning, exclusion, a bigger window).

It is a pure, DB-free transform. With no budget (``C* is None``) there is no
ideal share to compare against, so nothing is split -- the static
``split_tables_larger_than`` behaviour is unchanged.

See also: pg_cyan/collect/cost_model.py (per-table cost), pg_cyan/collect/capacity.py (C* and the
          per-shard job ceiling), pg_cyan/plan/planner.py (applies the decisions).
"""
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pg_cyan.collect import cost_model as cost_model_mod

#: Values of ``splittable_key`` that mean a table cannot be range-split.
_UNSPLITTABLE = {None, "", "none"}


def _splittable(table: Dict) -> bool:
    """A table is splittable unless its ``splittable_key`` says otherwise.

    Absent metadata is treated as **not** splittable, so an older metrics
    artifact degrades to leaving tables whole rather than assuming a key exists.
    """
    key = table.get("splittable_key")
    if isinstance(key, str):
        key = key.strip().lower()
    return key not in _UNSPLITTABLE


@dataclass
class TableSplit:
    """One table's split decision."""

    table_key: str
    cost_seconds: float
    splittable: bool
    subtasks: int  # 1 = not split
    reason: str  # "split" | "unsplittable_giant"


@dataclass
class SplitPlan:
    """The split decisions plus the makespan they imply."""

    enabled: bool
    ideal_share_seconds: float
    predicted_makespan_seconds: float
    makespan_without_splitting_seconds: float
    single_table_floor: bool
    total_cost_seconds: float
    decisions: List[TableSplit] = field(default_factory=list)

    def subtasks_by_table(self) -> Dict[str, int]:
        """Map of table key -> subtask count for the tables that are split."""
        return {d.table_key: d.subtasks for d in self.decisions if d.subtasks > 1}

    def as_plan_block(self) -> Dict:
        """The ``splitting`` block embedded in ``plan.json``."""
        return {
            "enabled": self.enabled,
            "ideal_share_seconds": round(self.ideal_share_seconds, 2),
            "predicted_makespan_seconds": round(self.predicted_makespan_seconds, 2),
            "makespan_without_splitting_seconds": round(
                self.makespan_without_splitting_seconds, 2),
            "single_table_floor": self.single_table_floor,
            "splits": [
                {"table": d.table_key, "subtasks": d.subtasks,
                 "cost_seconds": round(d.cost_seconds, 2), "reason": d.reason}
                for d in self.decisions
            ],
            "note": (
                "Split a table when its estimated copy time exceeds the ideal "
                "per-stream share (total_cost / C*) and it is splittable; "
                "pgcopydb carves ctid ranges at run time via "
                "split_tables_larger_than. Un-splittable giants stay whole and "
                "floor the makespan (bottleneck: single-table). No capacity "
                "budget disables cost-driven splitting."
            ),
        }


def _key(table: Dict) -> str:
    return f'{table["schema"]}.{table["name"]}'


def plan_splits(
    tables: List[Dict],
    *,
    c_star: Optional[int],
    max_table_jobs: int,
    cost_model: Optional[cost_model_mod.CostModel] = None,
) -> SplitPlan:
    """
    Decide which tables to split and predict the resulting makespan.

    Deterministic and never raises. ``c_star`` is the concurrency budget from
    :mod:`pg_cyan.collect.capacity`; ``None`` (no capacity data) disables cost-driven
    splitting. ``max_table_jobs`` caps a single table's within-shard parallelism.
    """
    model = cost_model or cost_model_mod.DEFAULT
    costs = {_key(t): model.estimate_seconds(t) for t in tables}
    total = sum(costs.values())
    max_cost = max(costs.values(), default=0.0)

    # Without a budget there is no ideal share to split against; leave tables
    # whole. The makespan floor is then the single costliest table.
    if not c_star or c_star < 1:
        return SplitPlan(
            enabled=False,
            ideal_share_seconds=0.0,
            predicted_makespan_seconds=max_cost,
            makespan_without_splitting_seconds=max_cost,
            single_table_floor=False,
            total_cost_seconds=total,
            decisions=[],
        )

    ideal_share = total / c_star
    cap = max(1, int(max_table_jobs))
    baseline = max(max_cost, ideal_share)

    # A shard limited to one worker can't parallelize any single table, so no
    # cost-driven split is possible; the costliest table floors the makespan.
    if cap < 2:
        return SplitPlan(
            enabled=True,
            ideal_share_seconds=ideal_share,
            predicted_makespan_seconds=baseline,
            makespan_without_splitting_seconds=baseline,
            single_table_floor=False,
            total_cost_seconds=total,
            decisions=[],
        )

    decisions: List[TableSplit] = []
    makespan = ideal_share
    single_table_floor = False

    for table in tables:
        key = _key(table)
        cost = costs[key]
        if cost <= ideal_share:
            continue  # already within the ideal share; nothing to do
        if _splittable(table):
            # Enough ranges to bring it down to the ideal share, bounded by the
            # parallelism one shard can actually run.
            needed = math.ceil(cost / ideal_share) if ideal_share > 0 else cap
            subtasks = max(2, min(cap, needed))
            makespan = max(makespan, cost / subtasks)
            decisions.append(TableSplit(key, cost, True, subtasks, "split"))
        else:
            # Indivisible: this table alone floors the makespan.
            makespan = max(makespan, cost)
            single_table_floor = True
            decisions.append(TableSplit(key, cost, False, 1, "unsplittable_giant"))

    return SplitPlan(
        enabled=True,
        ideal_share_seconds=ideal_share,
        predicted_makespan_seconds=makespan,
        makespan_without_splitting_seconds=baseline,
        single_table_floor=single_table_floor,
        total_cost_seconds=total,
        decisions=decisions,
    )
