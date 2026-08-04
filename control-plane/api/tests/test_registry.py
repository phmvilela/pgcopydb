"""SQLite registry tests."""
import sqlite3

import pytest

from app.registry import Registry


def _rec(reg, mid, ns, state="succeeded", now="2026-01-01T00:00:00+00:00"):
    """Seed a row in an explicit (terminal by default) state."""
    reg.record(mid, ns, "src:5432/db", "tgt:5432/db", state, now)


def test_record_list_and_get(registry):
    _rec(registry, "aaa", "pgcyan-mig-aaa")
    _rec(registry, "bbb", "pgcyan-mig-bbb", now="2026-01-02T00:00:00+00:00")

    ids = [r["id"] for r in registry.list()]
    assert ids == ["bbb", "aaa"]  # newest first
    assert registry.get("aaa")["namespace"] == "pgcyan-mig-aaa"
    assert registry.get("missing") is None
    # credentials are never stored
    assert registry.get("aaa")["source"] == "src:5432/db"


def test_active_excludes_terminal_states(registry):
    _rec(registry, "run", "ns-run", state="running")   # the one active
    _rec(registry, "done", "ns-done", state="succeeded")
    _rec(registry, "fail", "ns-fail", state="failed")
    _rec(registry, "halt", "ns-halt", state="halted")

    active = registry.active()
    assert [r["id"] for r in active] == ["run"]


def test_reserve_claims_the_singleton_slot(registry):
    assert registry.reserve("first", "ns-1", "s", "t", "2026-01-01T00:00:00+00:00")
    # a second reserve is rejected by the partial unique index -> False
    assert not registry.reserve("second", "ns-2", "s", "t", "2026-01-01T00:00:01+00:00")
    assert [r["id"] for r in registry.active()] == ["first"]
    assert registry.get("second") is None  # loser left no row


def test_reserve_slot_freed_when_holder_goes_terminal(registry):
    assert registry.reserve("a", "ns-a", "s", "t", "2026-01-01T00:00:00+00:00")
    assert not registry.reserve("b", "ns-b", "s", "t", "2026-01-01T00:00:01+00:00")
    registry.set_state("a", "succeeded", "2026-01-02T00:00:00+00:00")
    # the terminal row dropped out of the index; the slot is free again
    assert registry.reserve("b", "ns-b", "s", "t", "2026-01-02T00:00:01+00:00")
    assert [r["id"] for r in registry.active()] == ["b"]


def test_remove_releases_a_reservation(registry):
    assert registry.reserve("a", "ns-a", "s", "t", "2026-01-01T00:00:00+00:00")
    registry.remove("a")
    assert registry.get("a") is None
    assert registry.reserve("b", "ns-b", "s", "t", "2026-01-01T00:00:01+00:00")


def test_record_raises_on_second_active(registry):
    _rec(registry, "run", "ns-run", state="running")
    with pytest.raises(sqlite3.IntegrityError):
        _rec(registry, "run2", "ns-run2", state="waiting-to-start")


def test_set_state_transitions_out_of_active(registry):
    _rec(registry, "x", "ns-x", state="running")
    assert [r["id"] for r in registry.active()] == ["x"]
    registry.set_state("x", "succeeded", "2026-01-03T00:00:00+00:00")
    assert registry.active() == []
    row = registry.get("x")
    assert row["state"] == "succeeded"
    assert row["updated_at"] == "2026-01-03T00:00:00+00:00"


def test_registry_persists_across_reopen(tmp_path):
    path = tmp_path / "registry.db"
    _rec(Registry(path), "keep", "ns-keep")
    # A fresh instance on the same file (mirrors an API pod restart) still sees it.
    reopened = Registry(path)
    assert reopened.get("keep")["namespace"] == "ns-keep"
