"""
Feasibility gate for the migration planner: will the copy finish in time?

While the replication slot is held, the source pins WAL; a long copy on a busy
source can fill the source's WAL disk and break replication. Operators also have
maintenance windows. This module compares the planner's predicted makespan to two
deadlines and refuses to emit a plan that is bound to fail -- the same spirit as
the existing large-object abort gate.

    wal_retention deadline = wal_disk_free / wal_generation_rate   (hard)
    maintenance window     = constraints.max_copy_seconds          (soft, unless
                                                                     hard_deadline)

    status = abort  if the makespan exceeds a *hard* deadline
             warn   if it exceeds a soft deadline, or is within warn_ratio of any
             pass   otherwise
             not_evaluated  when no deadline inputs are present (a no-op gate)

It also emits the ``resume`` block the **controller** acts on -- circuit-breaker
thresholds (pause when the source WAL disk fills) and checkpoint boundaries (each
completed shard is a durable point to resume from). The runtime
pause/flush/re-snapshot/resume machinery is a sibling controller epic; the planner
only emits the parameters it consumes.

It is a pure, DB-free transform and never raises. See also:
pg_cyan/splitting.py (supplies the predicted makespan), pg_cyan/planner.py
(applies the verdict), pg_cyan/capacity.py (the source-capacity artifact).
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

#: Warn when the makespan reaches this fraction of any deadline.
DEFAULT_WARN_RATIO = 0.8
#: Pause the copy/CDC when the source WAL disk crosses this percentage.
DEFAULT_WAL_DISK_PCT_PAUSE = 85.0

_SEVERITY = {"pass": 0, "warn": 1, "abort": 2}


def _num(value, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _pos(value) -> Optional[float]:
    n = _num(value)
    return n if (n is not None and n > 0) else None


@dataclass
class Assessment:
    """The feasibility verdict plus the controller's resume parameters."""

    status: str  # pass | warn | abort | not_evaluated
    predicted_makespan_seconds: float
    deadlines: Dict[str, float]
    binding_deadline_seconds: Optional[float]
    binding_deadline_source: Optional[str]
    margin_ratio: Optional[float]
    reasons: List[str] = field(default_factory=list)
    resume: Dict = field(default_factory=dict)

    @property
    def aborted(self) -> bool:
        return self.status == "abort"

    def feasibility_block(self) -> Dict:
        """The ``feasibility`` block embedded in ``plan.json``."""
        return {
            "status": self.status,
            "predicted_makespan_seconds": round(self.predicted_makespan_seconds, 2),
            "deadlines_seconds": {k: round(v, 2) for k, v in self.deadlines.items()},
            "binding_deadline_seconds": (
                round(self.binding_deadline_seconds, 2)
                if self.binding_deadline_seconds is not None else None),
            "binding_deadline_source": self.binding_deadline_source,
            "margin_ratio": round(self.margin_ratio, 4) if self.margin_ratio is not None else None,
            "reasons": self.reasons,
            "note": (
                "Predicted makespan vs. the WAL-retention deadline (hard) and the "
                "maintenance window (soft unless hard_deadline). abort => no plan "
                "is written, like the large-object gate; warn => plan written with "
                "the warning; not_evaluated => no deadline inputs were supplied."
            ),
        }

    def resume_block(self) -> Dict:
        return self.resume


def _build_resume(constraints: Dict, shard_names: List[str]) -> Dict:
    """
    Circuit-breaker thresholds + checkpoint boundaries for the controller.

    Each completed shard is a natural durable checkpoint to resume from. See
    also: pg_cyan/planner.py (sibling controller epic consumes this).
    """
    enabled = constraints.get("resume_enabled", True) is not False
    pct = _pos(constraints.get("wal_disk_pct_pause")) or DEFAULT_WAL_DISK_PCT_PAUSE
    resume = {
        "enabled": enabled,
        "wal_disk_pct_pause": pct,
        "checkpoints": list(shard_names),
        "note": (
            "Controller circuit-breaker: pause/flush/re-snapshot/resume when the "
            "source WAL disk exceeds wal_disk_pct_pause; each completed shard is a "
            "durable checkpoint. Runtime machinery lives in the controller."
        ),
    }
    lag = _pos(constraints.get("max_lag_seconds"))
    if lag is not None:
        resume["max_lag_seconds"] = lag
    return resume


def assess(
    predicted_makespan_seconds,
    *,
    source_capacity: Optional[Dict],
    constraints: Optional[Dict],
    shard_names: List[str],
    warn_ratio: float = DEFAULT_WARN_RATIO,
) -> Assessment:
    """
    Compare the predicted makespan to the deadlines and build the resume block.

    Deterministic and never raises. With no deadline inputs the status is
    ``not_evaluated`` (the gate is a no-op) but the resume block is still emitted.
    """
    source_capacity = source_capacity or {}
    constraints = constraints or {}
    makespan = max(0.0, _num(predicted_makespan_seconds, 0.0) or 0.0)
    resume = _build_resume(constraints, shard_names)

    deadlines: Dict[str, float] = {}
    wal_free = _pos(source_capacity.get("wal_disk_free_bytes"))
    wal_rate = _pos(source_capacity.get("wal_generation_bytes_per_sec"))
    if wal_free is not None and wal_rate is not None:
        deadlines["wal_retention"] = wal_free / wal_rate
    window = _pos(constraints.get("max_copy_seconds"))
    if window is not None:
        deadlines["maintenance_window"] = window

    if not deadlines:
        return Assessment(
            status="not_evaluated",
            predicted_makespan_seconds=makespan,
            deadlines={},
            binding_deadline_seconds=None,
            binding_deadline_source=None,
            margin_ratio=None,
            reasons=["No WAL-retention or maintenance-window inputs; "
                     "feasibility not evaluated"],
            resume=resume,
        )

    hard_window = bool(constraints.get("hard_deadline"))
    status = "pass"
    reasons: List[str] = []
    for source, deadline in deadlines.items():
        hard = source == "wal_retention" or (source == "maintenance_window" and hard_window)
        ratio = makespan / deadline if deadline > 0 else float("inf")
        if ratio > 1.0:
            level = "abort" if hard else "warn"
            reasons.append(
                f"predicted makespan {makespan:.0f}s exceeds the {source} deadline "
                f"{deadline:.0f}s ({ratio:.2f}x)")
        elif ratio >= warn_ratio:
            level = "warn"
            reasons.append(
                f"predicted makespan {makespan:.0f}s is within {int(warn_ratio * 100)}% "
                f"of the {source} deadline {deadline:.0f}s ({ratio:.2f}x)")
        else:
            level = "pass"
        if _SEVERITY[level] > _SEVERITY[status]:
            status = level

    binding_source = min(deadlines, key=deadlines.get)  # tightest deadline
    binding = deadlines[binding_source]
    margin = makespan / binding if binding > 0 else None
    if status == "pass":
        reasons.append(
            f"predicted makespan {makespan:.0f}s is within all deadlines "
            f"(tightest: {binding_source} {binding:.0f}s)")

    return Assessment(
        status=status,
        predicted_makespan_seconds=makespan,
        deadlines=deadlines,
        binding_deadline_seconds=binding,
        binding_deadline_source=binding_source,
        margin_ratio=margin,
        reasons=reasons,
        resume=resume,
    )
