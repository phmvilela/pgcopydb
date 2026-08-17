"""
Unit tests for the preflight orchestrator.

These are fast, DB-free tests: they exercise the guard paths (no connection),
event serialization, and summary aggregation. Tests that require a live
PostgreSQL server belong in a separate integration suite.
"""
import json

from pg_cyan.plan.preflight import PgCyanPreflight


def make_checker() -> PgCyanPreflight:
    return PgCyanPreflight(
        host="localhost", port=5432, user="postgres",
        password="", database="postgres", timeout=1,
    )


def test_log_event_emits_valid_json(caplog):
    checker = make_checker()
    with caplog.at_level("INFO"):
        checker._log_event("unit_test", "success", "hello", {"k": "v"})
    payload = json.loads(caplog.records[-1].message)
    assert payload["event_type"] == "unit_test"
    assert payload["status"] == "success"
    assert payload["details"] == {"k": "v"}
    assert payload["timestamp"].endswith("Z")


def test_checks_fail_gracefully_without_connection():
    checker = make_checker()
    # No connect() call -> conn is None -> checks must return False, not raise.
    assert checker.check_long_running_transactions() is False
    assert checker.check_primary_keys() is False


def test_run_all_checks_reports_not_ready_when_connection_fails():
    checker = make_checker()  # nothing listening -> connect() fails fast (timeout=1)
    results = checker.run_all_checks()
    assert results["summary"]["ready_for_migration"] is False
    assert results["summary"]["total_failed"] >= 1
