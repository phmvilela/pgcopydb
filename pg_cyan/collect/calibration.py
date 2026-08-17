"""
Throughput calibration for the migration planner: learn from real copy runs.

The cost model's throughput constants (``per_stream_bytes_per_sec`` /
``per_stream_rows_per_sec``) start as conservative guesses. This module refines
them from a ``copylog-history.json`` artifact of **prior per-table copy
durations**, so each migration's plan is fitted to what the previous runs
actually achieved end to end (source read + decode + network + target write),
not just a default or a bare network probe. The feedback loop the epic asks for:
real durations flow back into the cost model, shrinking predicted-makespan error.

The calibration is **global** (one constant per resource, applied to every
table). The cost model already differentiates table classes structurally -- a
narrow, row-heavy table is bound by the rows term of ``max(bytes, rows)`` and a
TOAST-heavy table by ``toast_overhead_factor`` -- so a single fitted
bytes/sec + rows/sec pair captures the class behaviour without a separate
per-class constant. Per-table-class constants (narrow / wide / TOAST) are a
documented follow-up; the observed-vs-predicted residuals recorded here are the
data that would seed them.

Method (per resource, over the observations where that resource binds):

    observed_rate = sum(volume) / sum(duration)          # aggregate throughput
    calibrated    = w * observed_rate + (1 - w) * prior  # shrink toward the prior
    w             = n / (n + PRIOR_STRENGTH)              # confidence in n samples

The prior is the base cost model handed in (default, or already adjusted by a
network probe -- see ``pg_cyan/planner._resolve_budget``). Shrinking toward it
keeps a single noisy run from swinging the constant wildly; with no history the
weight is zero and the base model is returned unchanged (the documented
fallback). An observation is attributed to the bytes or rows resource by which
term the base model would treat as binding, so each constant is fitted only from
the runs where it actually mattered.

It is a pure, DB-free transform and never raises: malformed or partial history
degrades to fewer usable observations, and an empty set degrades to the base
model. See also: pg_cyan/collect/cost_model.py (the constants this fits),
pg_cyan/plan/planner.py (reads the history artifact and applies the result).
"""
import dataclasses
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pg_cyan.collect import cost_model as cost_model_mod

#: Prior strength, in observations: the number of samples at which the fitted
#: constant is weighted equally with the base model. Small so a first real run
#: already moves the constant meaningfully, but non-zero so one outlier can't own
#: it. Confidence grows as n / (n + PRIOR_STRENGTH).
PRIOR_STRENGTH = 3.0


def _num(value, default: Optional[float] = None) -> Optional[float]:
    """Coerce to float; return ``default`` for None/missing/non-numeric."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _pos(value) -> Optional[float]:
    """A strictly-positive float, or None."""
    n = _num(value)
    return n if (n is not None and n > 0) else None


@dataclass(frozen=True)
class _Observation:
    """One prior table copy: the volume shipped and the time it took."""

    live_bytes: float
    rows: float
    duration_seconds: float


@dataclass(frozen=True)
class Calibration:
    """
    The calibrated cost model plus an audit trail of how it was fitted.

    Attributes:
        model: The cost model to plan with -- calibrated when history was usable,
            otherwise the base model unchanged.
        source: ``"history"`` when constants were fitted, ``"default"`` when the
            base model was kept (no/unusable history).
        observations: Number of usable prior table copies parsed from history.
        observed_bytes_per_sec / observed_rows_per_sec: The raw aggregate rates
            from history (before shrinkage), or ``None`` if that resource never
            bound in the sample.
        blend_weight_bytes / blend_weight_rows: The confidence weight applied to
            each observed rate (``n / (n + PRIOR_STRENGTH)``).
        default_error_ratio / calibrated_error_ratio: Mean relative
            predicted-vs-actual duration error over the sample, under the base and
            the calibrated model. The pair the acceptance checks: calibration
            should not increase it.
        reasons: Human-readable notes on what was and wasn't fitted.
    """

    model: cost_model_mod.CostModel
    source: str
    observations: int
    observed_bytes_per_sec: Optional[float]
    observed_rows_per_sec: Optional[float]
    blend_weight_bytes: float
    blend_weight_rows: float
    default_error_ratio: Optional[float]
    calibrated_error_ratio: Optional[float]
    reasons: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict:
        """The calibration audit sub-block embedded in ``plan.json``'s cost_model."""
        return {
            "source": self.source,
            "observations": self.observations,
            "observed_bytes_per_sec": (
                round(self.observed_bytes_per_sec, 2)
                if self.observed_bytes_per_sec is not None else None),
            "observed_rows_per_sec": (
                round(self.observed_rows_per_sec, 2)
                if self.observed_rows_per_sec is not None else None),
            "blend_weight_bytes": round(self.blend_weight_bytes, 4),
            "blend_weight_rows": round(self.blend_weight_rows, 4),
            "prior_strength_observations": PRIOR_STRENGTH,
            "predicted_error_ratio": {
                "before": (round(self.default_error_ratio, 4)
                           if self.default_error_ratio is not None else None),
                "after": (round(self.calibrated_error_ratio, 4)
                          if self.calibrated_error_ratio is not None else None),
            },
            "reasons": self.reasons,
            "granularity": "global",
            "note": (
                "Throughput constants fitted from copylog-history.json (prior "
                "per-table durations): observed_rate = sum(volume)/sum(duration) "
                "over the tables where that resource bound, shrunk toward the base "
                "model by n/(n+prior_strength). Global (one constant per resource); "
                "per-table-class calibration is a documented follow-up. No history "
                "keeps the base constants (default, or the network-probe rate)."
            ),
        }


def _extract_observations(
    history: Dict, base: cost_model_mod.CostModel
) -> List[_Observation]:
    """
    Flatten a ``copylog-history.json`` artifact into usable observations.

    Lenient about shape: accepts a top-level ``observations``/``tables`` list, or
    a ``runs`` list each carrying its own ``observations``/``tables``. Every row
    needs a positive ``duration_seconds`` and some volume (``live_bytes`` directly,
    or the ``heap_bytes``/``dead_tuple_ratio``/``toast_bytes`` the base model turns
    into live bytes, and/or a positive ``row_estimate``/``rows``). Rows that carry
    neither volume nor a duration are dropped.
    """
    rows = _history_rows(history)
    observations: List[_Observation] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        duration = _pos(row.get("duration_seconds") if "duration_seconds" in row
                        else row.get("seconds"))
        if duration is None:
            continue
        live = _num(row.get("live_bytes"))
        if live is None:
            live = base.live_bytes(row)
        rowcount = _num(row.get("row_estimate")) or _num(row.get("rows")) or 0.0
        live = max(0.0, live or 0.0)
        rowcount = max(0.0, rowcount)
        if live <= 0.0 and rowcount <= 0.0:
            continue
        observations.append(_Observation(live, rowcount, duration))
    return observations


def _history_rows(history: Dict) -> List:
    """Pull the flat list of table rows out of the (lenient) history shape."""
    if not isinstance(history, dict):
        return list(history) if isinstance(history, list) else []
    runs = history.get("runs")
    if isinstance(runs, list):
        rows: List = []
        for run in runs:
            if isinstance(run, dict):
                rows.extend(run.get("observations") or run.get("tables") or [])
        return rows
    return history.get("observations") or history.get("tables") or []


def _blend(observed: Optional[float], prior: float, n: int) -> tuple:
    """Shrink ``observed`` toward ``prior`` by confidence ``n/(n+PRIOR_STRENGTH)``."""
    if observed is None or n <= 0:
        return prior, 0.0
    weight = n / (n + PRIOR_STRENGTH)
    return weight * observed + (1.0 - weight) * prior, weight


def _mean_error_ratio(
    observations: List[_Observation], bytes_per_sec: float, rows_per_sec: float
) -> Optional[float]:
    """Mean relative error of ``max(bytes, rows)`` predictions vs. real durations."""
    if not observations:
        return None
    errors = []
    for obs in observations:
        predicted = max(
            obs.live_bytes / bytes_per_sec if bytes_per_sec > 0 else 0.0,
            obs.rows / rows_per_sec if rows_per_sec > 0 else 0.0,
        )
        errors.append(abs(predicted - obs.duration_seconds) / obs.duration_seconds)
    return sum(errors) / len(errors)


def calibrate(
    history: Optional[Dict], base: Optional[cost_model_mod.CostModel] = None
) -> Calibration:
    """
    Fit the cost model's throughput constants from prior copy durations.

    ``base`` is the model to refine (default, or already network-probe adjusted);
    it is also the shrinkage prior. Deterministic and never raises: absent or
    unusable history returns ``base`` unchanged with ``source == "default"``.
    """
    model = base or cost_model_mod.DEFAULT
    if not history:
        return Calibration(
            model=model, source="default", observations=0,
            observed_bytes_per_sec=None, observed_rows_per_sec=None,
            blend_weight_bytes=0.0, blend_weight_rows=0.0,
            default_error_ratio=None, calibrated_error_ratio=None,
            reasons=["No copylog-history.json; kept the base cost-model constants"],
        )

    observations = _extract_observations(history, model)
    if not observations:
        return Calibration(
            model=model, source="default", observations=0,
            observed_bytes_per_sec=None, observed_rows_per_sec=None,
            blend_weight_bytes=0.0, blend_weight_rows=0.0,
            default_error_ratio=None, calibrated_error_ratio=None,
            reasons=["copylog-history.json had no usable per-table durations; "
                     "kept the base cost-model constants"],
        )

    # Attribute each observation to the resource the base model treats as binding,
    # so each constant is fitted only from the runs where it actually mattered.
    byte_bound: List[_Observation] = []
    row_bound: List[_Observation] = []
    for obs in observations:
        bytes_term = obs.live_bytes / model.per_stream_bytes_per_sec
        rows_term = obs.rows / model.per_stream_rows_per_sec
        (byte_bound if bytes_term >= rows_term else row_bound).append(obs)

    observed_bps = _aggregate_rate([o.live_bytes for o in byte_bound],
                                   [o.duration_seconds for o in byte_bound])
    observed_rps = _aggregate_rate([o.rows for o in row_bound],
                                   [o.duration_seconds for o in row_bound])

    bps, w_bytes = _blend(observed_bps, model.per_stream_bytes_per_sec, len(byte_bound))
    rps, w_rows = _blend(observed_rps, model.per_stream_rows_per_sec, len(row_bound))

    calibrated = dataclasses.replace(
        model, per_stream_bytes_per_sec=bps, per_stream_rows_per_sec=rps)

    reasons = []
    if observed_bps is not None:
        reasons.append(
            f"bytes/sec fitted from {len(byte_bound)} byte-bound table(s): "
            f"{observed_bps / (1024 * 1024):.1f} MiB/s observed, "
            f"blended to {bps / (1024 * 1024):.1f} MiB/s (w={w_bytes:.2f})")
    if observed_rps is not None:
        reasons.append(
            f"rows/sec fitted from {len(row_bound)} row-bound table(s): "
            f"{observed_rps:.0f} observed, blended to {rps:.0f} (w={w_rows:.2f})")

    return Calibration(
        model=calibrated,
        source="history",
        observations=len(observations),
        observed_bytes_per_sec=observed_bps,
        observed_rows_per_sec=observed_rps,
        blend_weight_bytes=w_bytes,
        blend_weight_rows=w_rows,
        default_error_ratio=_mean_error_ratio(
            observations, model.per_stream_bytes_per_sec, model.per_stream_rows_per_sec),
        calibrated_error_ratio=_mean_error_ratio(observations, bps, rps),
        reasons=reasons,
    )


def _aggregate_rate(volumes: List[float], durations: List[float]) -> Optional[float]:
    """Aggregate throughput ``sum(volume) / sum(duration)``, or None if empty."""
    total_time = sum(durations)
    total_volume = sum(volumes)
    if total_time <= 0.0 or total_volume <= 0.0:
        return None
    return total_volume / total_time
