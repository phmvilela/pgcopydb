"""
Unit tests for the planner's feasibility gate.

See also: pg_cyan/collect/feasibility.py

DB-free by nature: the gate is a pure transform of the predicted makespan +
deadline inputs into a pass/warn/abort verdict and a controller resume block.
Tests cover the WAL-retention deadline (hard), the maintenance window (soft, and
escalated by hard_deadline), the warn band, the no-input no-op, and the resume
block contents.
"""
from pg_cyan.collect.feasibility import (
    DEFAULT_WAL_DISK_PCT_PAUSE,
    assess,
)

_SHARDS = ["shard-0", "shard-1", "shard-2"]


def _assess(makespan, *, source=None, constraints=None):
    return assess(makespan, source_capacity=source, constraints=constraints,
                  shard_names=_SHARDS)


def _wal(deadline_seconds):
    """A source-capacity whose WAL disk fills in ``deadline_seconds``."""
    return {"wal_disk_free_bytes": deadline_seconds, "wal_generation_bytes_per_sec": 1}


# ------------------------------------------------------------- WAL deadline

def test_pass_when_makespan_well_under_the_wal_deadline():
    a = _assess(100, source=_wal(1000))
    assert a.status == "pass"
    assert a.binding_deadline_source == "wal_retention"
    assert a.margin_ratio == 0.1


def test_warn_when_makespan_is_within_the_warn_band():
    # 900 / 1000 = 0.9 >= default warn_ratio 0.8 -> warn, but still under.
    a = _assess(900, source=_wal(1000))
    assert a.status == "warn"
    assert not a.aborted


def test_abort_when_makespan_exceeds_the_wal_deadline():
    a = _assess(1500, source=_wal(1000))
    assert a.status == "abort"
    assert a.aborted
    assert any("wal_retention" in r and "exceeds" in r for r in a.reasons)


def test_wal_deadline_is_disk_free_over_generation_rate():
    a = _assess(1, source={"wal_disk_free_bytes": 1000,
                           "wal_generation_bytes_per_sec": 10})
    assert a.deadlines["wal_retention"] == 100.0


# ------------------------------------------------------- maintenance window

def test_window_breach_is_soft_warn_by_default():
    a = _assess(1500, constraints={"max_copy_seconds": 1000})
    assert a.status == "warn"  # soft: operator window, not a physical limit
    assert not a.aborted


def test_hard_deadline_escalates_window_breach_to_abort():
    a = _assess(1500, constraints={"max_copy_seconds": 1000, "hard_deadline": True})
    assert a.status == "abort"


def test_tightest_deadline_is_the_binding_one():
    a = _assess(10, source=_wal(1000), constraints={"max_copy_seconds": 200})
    assert a.binding_deadline_source == "maintenance_window"
    assert a.binding_deadline_seconds == 200


def test_wal_abort_wins_over_window_pass():
    # Comfortably inside the window, but the WAL disk fills first -> abort.
    a = _assess(1500, source=_wal(1000), constraints={"max_copy_seconds": 100000})
    assert a.status == "abort"


# --------------------------------------------------------------- no-op gate

def test_no_deadline_inputs_is_not_evaluated():
    a = _assess(1500)
    assert a.status == "not_evaluated"
    assert not a.aborted
    assert a.binding_deadline_seconds is None
    assert a.deadlines == {}


# ------------------------------------------------------------- resume block

def test_resume_block_defaults_and_checkpoints():
    a = _assess(100, source=_wal(1000))
    r = a.resume_block()
    assert r["enabled"] is True
    assert r["wal_disk_pct_pause"] == DEFAULT_WAL_DISK_PCT_PAUSE
    assert r["checkpoints"] == _SHARDS
    assert "max_lag_seconds" not in r  # only when supplied


def test_resume_block_honors_constraint_overrides():
    a = _assess(100, source=_wal(1000),
                constraints={"wal_disk_pct_pause": 70, "max_lag_seconds": 300,
                             "resume_enabled": False})
    r = a.resume_block()
    assert r["wal_disk_pct_pause"] == 70
    assert r["max_lag_seconds"] == 300
    assert r["enabled"] is False


def test_resume_block_present_even_when_not_evaluated():
    a = _assess(100)
    assert a.resume_block()["checkpoints"] == _SHARDS


# ---------------------------------------------------------------- robustness

def test_never_raises_on_junk_and_is_deterministic():
    a = assess("not a number", source_capacity={"wal_disk_free_bytes": "x"},
               constraints={"max_copy_seconds": None}, shard_names=_SHARDS)
    assert a.status == "not_evaluated"
    b = _assess(1500, source=_wal(1000))
    c = _assess(1500, source=_wal(1000))
    assert b.feasibility_block() == c.feasibility_block()
