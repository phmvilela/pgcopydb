"""
Unit tests for the throughput calibration (`pg_cyan.calibration`).

See also: pg_cyan/calibration.py

DB-free by nature: calibration is a pure transform of a copylog-history.json
artifact into refined cost-model constants. Tests exercise the fallback (no/
unusable history keeps the base model), the aggregate fit and shrinkage toward
the prior, the byte/row resource attribution, the lenient history shapes, and
the acceptance property -- calibrated predictions are no worse than the default's.
"""
import pytest

from pg_cyan import calibration
from pg_cyan.cost_model import CostModel

MiB = 1024 * 1024


def _obs(heap_bytes=0, rows=0, duration=1.0, **extra):
    """A history row shaped like a metrics table row plus its copy duration."""
    return {
        "schema": "public", "name": f"t{int(heap_bytes)}_{int(rows)}",
        "heap_bytes": heap_bytes, "row_estimate": rows,
        "duration_seconds": duration, **extra,
    }


def _byte_bound_history(observed_mib_per_sec, n=5, mib_each=500):
    """n wide tables all copied at ``observed_mib_per_sec`` (bytes bind)."""
    dur = mib_each / observed_mib_per_sec
    return {"observations": [
        _obs(heap_bytes=mib_each * MiB, rows=0, duration=dur) for _ in range(n)
    ]}


# ------------------------------------------------------------------- fallbacks

def test_no_history_keeps_the_base_model():
    base = CostModel()
    result = calibration.calibrate(None, base=base)
    assert result.source == "default"
    assert result.model is base
    assert result.observations == 0
    assert result.blend_weight_bytes == 0.0 and result.blend_weight_rows == 0.0


def test_empty_history_keeps_the_base_model():
    result = calibration.calibrate({"observations": []}, base=CostModel())
    assert result.source == "default"
    assert result.observations == 0


def test_rows_without_duration_or_volume_are_dropped():
    history = {"observations": [
        _obs(heap_bytes=0, rows=0, duration=10.0),        # no volume
        {"schema": "public", "name": "x", "heap_bytes": 100 * MiB},  # no duration
        {"schema": "public", "name": "y", "heap_bytes": 100 * MiB,
         "duration_seconds": "not-a-number"},             # unparseable duration
    ]}
    result = calibration.calibrate(history, base=CostModel())
    assert result.source == "default"


# ------------------------------------------------------------------- the fit

def test_byte_constant_moves_toward_the_observed_rate():
    base = CostModel()  # 50 MiB/s default
    result = calibration.calibrate(_byte_bound_history(25.0, n=5), base=base)

    assert result.source == "history"
    assert result.observations == 5
    # Aggregate observed throughput is the real 25 MiB/s.
    assert result.observed_bytes_per_sec == pytest.approx(25 * MiB, rel=1e-6)
    # Shrunk toward the 50 MiB/s prior by n/(n+PRIOR_STRENGTH) = 5/8.
    w = 5 / (5 + calibration.PRIOR_STRENGTH)
    assert result.blend_weight_bytes == pytest.approx(w)
    expected = w * 25 * MiB + (1 - w) * 50 * MiB
    assert result.model.per_stream_bytes_per_sec == pytest.approx(expected)
    # Strictly between observed and prior -- it moved, but not all the way.
    assert 25 * MiB < result.model.per_stream_bytes_per_sec < 50 * MiB


def test_more_history_moves_the_constant_further():
    near = calibration.calibrate(_byte_bound_history(25.0, n=2), base=CostModel())
    far = calibration.calibrate(_byte_bound_history(25.0, n=50), base=CostModel())
    # More observations -> higher confidence -> closer to the observed 25 MiB/s.
    assert far.model.per_stream_bytes_per_sec < near.model.per_stream_bytes_per_sec
    assert far.blend_weight_bytes > near.blend_weight_bytes


def test_calibration_does_not_increase_prediction_error():
    # History says the link is half as fast as the default assumes.
    result = calibration.calibrate(_byte_bound_history(25.0, n=8), base=CostModel())
    assert result.default_error_ratio is not None
    assert result.calibrated_error_ratio <= result.default_error_ratio


def test_row_constant_fitted_from_row_bound_tables():
    # Narrow tables: tiny bytes, many rows -> the rows term binds under default.
    base = CostModel()
    dur = 2.0
    history = {"observations": [
        _obs(heap_bytes=1024, rows=100_000, duration=dur) for _ in range(4)
    ]}
    result = calibration.calibrate(history, base=base)
    assert result.source == "history"
    # 100k rows in 2s -> 50k rows/s observed, below the 200k default.
    assert result.observed_rows_per_sec == pytest.approx(50_000, rel=1e-6)
    assert result.observed_bytes_per_sec is None  # nothing byte-bound
    assert result.model.per_stream_rows_per_sec < base.per_stream_rows_per_sec
    assert result.model.per_stream_bytes_per_sec == base.per_stream_bytes_per_sec


# --------------------------------------------------------------- input shapes

def test_accepts_runs_wrapper_and_live_bytes_field():
    # `runs` list, each with `tables`, and live_bytes supplied directly.
    dur = 10.0
    history = {"runs": [
        {"run_id": "r1", "tables": [
            {"schema": "public", "name": "a", "live_bytes": 250 * MiB,
             "duration_seconds": dur},
        ]},
        {"run_id": "r2", "tables": [
            {"schema": "public", "name": "b", "live_bytes": 250 * MiB,
             "duration_seconds": dur},
        ]},
    ]}
    result = calibration.calibrate(history, base=CostModel())
    assert result.source == "history"
    assert result.observations == 2
    assert result.observed_bytes_per_sec == pytest.approx(25 * MiB, rel=1e-6)


def test_malformed_history_degrades_without_raising():
    # Wrong types everywhere -> no usable rows -> default, no exception.
    for bad in ["nonsense", 42, {"observations": "not-a-list"},
                {"runs": [None, 7, {"tables": None}]}]:
        result = calibration.calibrate(bad, base=CostModel())
        assert result.source == "default"


def test_as_dict_reports_the_audit_trail():
    result = calibration.calibrate(_byte_bound_history(25.0, n=5), base=CostModel())
    block = result.as_dict()
    assert block["source"] == "history"
    assert block["granularity"] == "global"
    assert block["observations"] == 5
    assert block["predicted_error_ratio"]["before"] is not None
    assert block["predicted_error_ratio"]["after"] is not None
    assert block["prior_strength_observations"] == calibration.PRIOR_STRENGTH
