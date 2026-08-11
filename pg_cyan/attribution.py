"""
Bottleneck attribution for the migration planner: name what limits the copy.

The concurrency budget ``C* = min(terms)`` is only as large as its smallest
("binding") term, and a single indivisible giant table can floor the makespan
regardless of ``C*``. This module turns that raw binding term into the plan's
``bottleneck`` -- a stable, canonical label plus the operator remediation that
actually moves it -- so the plan answers "what do I change to go faster?".

The canonical vocabulary (what the epic surfaces):

    source-read | network | target-write | k8s-cpu | single-table

Two clamp-side limits are reported distinctly because their remediation differs
from the physical ceilings: ``source-connections`` / ``target-connections`` (a
connection-count limit, not a throughput one) and ``operator-constraint`` (a
``constraints.json`` cap the operator set deliberately). When there is no
capacity data at all the budget is degraded and the bottleneck is ``none``.

A ``single-table`` floor takes precedence over the stream-budget term: if one
un-splittable table alone dictates the makespan, widening ``C*`` won't help, so
that is the finding the operator must act on first (see pg_cyan/splitting.py).

It is a pure, DB-free transform and never raises. See also: pg_cyan/capacity.py
(produces the binding term), pg_cyan/planner.py (embeds the result in plan.json).
"""
from dataclasses import dataclass
from typing import Dict, Optional

from pg_cyan import capacity as capacity_mod
from pg_cyan import splitting as splitting_mod

#: Raw ``Budget.binding_term`` -> canonical bottleneck label.
_CANONICAL = {
    "source_read": "source-read",
    "source_connections": "source-connections",
    "network_fill": "network",
    "target_write": "target-write",
    "target_connections": "target-connections",
    "k8s_cpu": "k8s-cpu",
    "constraints_max_concurrency": "operator-constraint",
}

#: Canonical bottleneck -> the operator action that relaxes it.
_REMEDIATION = {
    "source-read": (
        "Source read I/O is the ceiling: provision more source IOPS/throughput "
        "or a larger source SKU, or raise the source read budget in constraints.json."
    ),
    "source-connections": (
        "Source connection headroom is the ceiling: raise the source "
        "max_connections, reduce competing sessions, or lift "
        "max_source_connections in constraints.json."
    ),
    "network": (
        "The source->cluster link is the ceiling: widen bandwidth or reduce RTT "
        "(a closer region / more parallel streams to fill a high-latency pipe)."
    ),
    "target-write": (
        "Target write I/O is the ceiling: provision more target IOPS/throughput "
        "or a larger target SKU, or raise maintenance_work_mem for the load."
    ),
    "target-connections": (
        "Target connection headroom is the ceiling: raise the target "
        "max_connections or reduce competing sessions on the target."
    ),
    "k8s-cpu": (
        "Allocatable cluster CPU is the ceiling: add nodes / a larger node SKU, "
        "or lower cpu_per_stream if the copy pods are over-requested."
    ),
    "single-table": (
        "One un-splittable table alone floors the makespan: widening C* won't "
        "help. Partition it, add a splittable key, exclude it, or grant a longer "
        "window."
    ),
    "operator-constraint": (
        "A constraints.json cap (e.g. max_concurrency) is binding by choice: "
        "relax it if the source/target/cluster can sustain more."
    ),
    "none": (
        "No capacity data was supplied, so no bottleneck could be attributed; "
        "the plan used the static shard defaults. Provide the capacity artifacts "
        "to size and explain the copy."
    ),
}


@dataclass(frozen=True)
class Attribution:
    """The canonical bottleneck plus the raw term and its remediation."""

    bottleneck: str
    binding_term: str
    term_value: Optional[float]
    remediation: str

    def as_dict(self) -> Dict:
        """The ``attribution`` block embedded in ``plan.json``."""
        return {
            "bottleneck": self.bottleneck,
            "binding_term": self.binding_term,
            "term_value": (round(self.term_value, 4)
                           if self.term_value is not None else None),
            "remediation": self.remediation,
            "note": (
                "The plan's binding constraint. bottleneck is the canonical label "
                "(source-read | network | target-write | k8s-cpu | single-table, "
                "plus source-connections / target-connections / operator-constraint "
                "for the clamp-side limits); binding_term is the raw C* term it "
                "came from. A single-table floor takes precedence over the stream "
                "budget -- widening C* cannot relieve it."
            ),
        }


def attribute(
    budget: capacity_mod.Budget, split_plan: splitting_mod.SplitPlan
) -> Attribution:
    """
    Attribute the plan's bottleneck from the budget and the split decisions.

    An un-splittable giant that floors the makespan wins over the stream-budget
    term; otherwise the canonical mapping of ``budget.binding_term`` applies, and
    a degraded (no-capacity) budget yields ``none``. Never raises.
    """
    if split_plan.single_table_floor:
        return Attribution(
            bottleneck="single-table",
            binding_term="single_table_floor",
            term_value=round(split_plan.predicted_makespan_seconds, 2),
            remediation=_REMEDIATION["single-table"],
        )
    if budget.degraded:
        return Attribution(
            bottleneck="none",
            binding_term="none",
            term_value=None,
            remediation=_REMEDIATION["none"],
        )
    raw = budget.binding_term
    canonical = _CANONICAL.get(raw, raw)
    return Attribution(
        bottleneck=canonical,
        binding_term=raw,
        term_value=budget.terms.get(raw),
        remediation=_REMEDIATION.get(canonical, "See the concurrency terms for detail."),
    )
