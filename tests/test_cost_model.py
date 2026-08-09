"""
Unit tests for the planner's time-based cost model.

See also: pg_cyan/cost_model.py

DB-free by nature: the cost model is a pure transform of a table's collected
size/shape into an estimated single-stream copy time. Tests exercise the four
table shapes it must handle -- wide/TOAST-heavy, narrow-hot, bloated, and
empty -- plus the graceful fallback to ``heap_bytes`` when the extended metrics
fields are absent.
"""
import pytest

from pg_cyan.cost_model import DEFAULT, CostModel


def _table(**overrides):
    base = {"schema": "public", "name": "t", "heap_bytes": 0}
    base.update(overrides)
    return base


# ------------------------------------------------------------------- live_bytes

def test_live_bytes_falls_back_to_heap_when_no_extended_fields():
    # No dead ratio / toast -> COPY volume is just the heap bytes.
    assert DEFAULT.live_bytes(_table(heap_bytes=1000)) == 1000.0


def test_live_bytes_discounts_dead_tuples_on_a_bloated_table():
    # 90% bloat -> only 10% is live tuples COPY actually ships.
    t = _table(heap_bytes=1000, dead_tuple_ratio=0.9)
    assert DEFAULT.live_bytes(t) == pytest.approx(100.0)


def test_live_bytes_clamps_a_noisy_dead_ratio():
    assert DEFAULT.live_bytes(_table(heap_bytes=1000, dead_tuple_ratio=1.5)) == 0.0
    assert DEFAULT.live_bytes(_table(heap_bytes=1000, dead_tuple_ratio=-0.5)) == 1000.0


def test_toast_surcharge_only_applies_with_a_factor_above_one():
    t = _table(heap_bytes=1000, toast_bytes=400)
    # Neutral default: TOAST already counted in heap, no extra.
    assert DEFAULT.live_bytes(t) == 1000.0
    # With a 2x detoast factor the 400 TOAST bytes cost an extra 400.
    surcharged = CostModel(toast_overhead_factor=2.0)
    assert surcharged.live_bytes(t) == 1400.0


def test_toast_bytes_cannot_exceed_heap_in_the_surcharge():
    # A noisy toast_bytes > heap_bytes must not blow up the surcharge.
    t = _table(heap_bytes=1000, toast_bytes=5000)
    surcharged = CostModel(toast_overhead_factor=2.0)
    assert surcharged.live_bytes(t) == 2000.0  # capped at heap, +1x


# --------------------------------------------------------------- estimate_seconds

def test_wide_table_is_bytes_bound():
    # A wide/TOAST-heavy table with few rows: the bytes term dominates.
    model = CostModel(per_stream_bytes_per_sec=100.0, per_stream_rows_per_sec=100.0)
    t = _table(heap_bytes=10_000, row_estimate=10)
    assert model.estimate_seconds(t) == 100.0  # 10_000 / 100 bytes-bound


def test_narrow_hot_table_is_rows_bound():
    # Many rows, tiny bytes: per-row overhead dominates, so bytes alone
    # (today's weight) would badly under-cost this table.
    model = CostModel(per_stream_bytes_per_sec=100.0, per_stream_rows_per_sec=100.0)
    t = _table(heap_bytes=100, row_estimate=10_000)
    assert model.estimate_seconds(t) == 100.0  # 10_000 / 100 rows-bound
    # And it out-costs a same-byte table that has almost no rows.
    quiet = _table(heap_bytes=100, row_estimate=1)
    assert model.estimate_seconds(t) > model.estimate_seconds(quiet)


def test_bloated_table_costs_less_than_its_heap_suggests():
    model = CostModel(per_stream_bytes_per_sec=100.0, per_stream_rows_per_sec=1e12)
    bloated = _table(heap_bytes=10_000, dead_tuple_ratio=0.9, row_estimate=10)
    tight = _table(heap_bytes=10_000, dead_tuple_ratio=0.0, row_estimate=10)
    assert model.estimate_seconds(bloated) < model.estimate_seconds(tight)
    assert model.estimate_seconds(bloated) == pytest.approx(10.0)  # 1000 live / 100


def test_empty_table_costs_zero():
    assert DEFAULT.estimate_seconds(_table(heap_bytes=0)) == 0.0
    assert DEFAULT.estimate_seconds(_table()) == 0.0


def test_unanalysed_reltuples_minus_one_is_treated_as_unknown():
    # reltuples is -1 for a never-analysed table; it must not go negative.
    model = CostModel(per_stream_bytes_per_sec=100.0, per_stream_rows_per_sec=100.0)
    t = _table(heap_bytes=1000, row_estimate=-1)
    assert model.estimate_seconds(t) == 10.0  # bytes term only


def test_missing_and_junk_fields_never_raise():
    model = DEFAULT
    assert model.estimate_seconds(_table(heap_bytes="not a number")) == 0.0
    assert model.estimate_seconds(_table(heap_bytes=1000, dead_tuple_ratio=None)) > 0.0
    assert model.estimate_seconds({}) == 0.0


def test_as_dict_exposes_the_constants():
    d = CostModel(per_stream_bytes_per_sec=1.0, per_stream_rows_per_sec=2.0,
                  toast_overhead_factor=3.0).as_dict()
    assert d == {
        "per_stream_bytes_per_sec": 1.0,
        "per_stream_rows_per_sec": 2.0,
        "toast_overhead_factor": 3.0,
    }
