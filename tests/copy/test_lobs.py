"""
Unit tests for the source-pg-largeobjects collector.

DB-free: they exercise guard paths (no connection), the JSON event shape, the
payload builder, and the output/error file-writing contract:
  1. Happy path        -> a non-empty source-pg-largeobjects.json with the count
  2. Zero large objects -> still a success (an empty database is not a failure)
  3. Connection issues  -> source-pg-largeobjects-errors.json with a conn error
  4. Query issues       -> source-pg-largeobjects-errors.json with the source's error
Tests needing a live PostgreSQL server belong in the integration suite.
"""
import json

from pg_cyan.copy.lobs import PgCyanLobs


def make_collector(output_dir: str = ".") -> PgCyanLobs:
    return PgCyanLobs(
        host="localhost", port=5432, user="postgres",
        password="", database="postgres", timeout=1, output_dir=output_dir,
    )


def test_log_event_emits_valid_json(caplog):
    collector = make_collector()
    with caplog.at_level("INFO"):
        collector._log_event("unit_test", "success", "hello", {"k": "v"})
    payload = json.loads(caplog.records[-1].message)
    assert payload["component"] == "source-pg-largeobjects"
    assert payload["event_type"] == "unit_test"
    assert payload["status"] == "success"
    assert payload["details"] == {"k": "v"}
    assert payload["timestamp"].endswith("Z")


def test_fetch_guard_without_connection():
    collector = make_collector()
    # No connect() call -> conn is None -> must return None, not raise.
    assert collector.fetch_lob_count() is None
    assert collector.errors  # an error message was recorded


def test_build_output_is_non_empty_and_well_formed():
    collector = make_collector()
    payload = collector._build_output(42)
    assert payload["status"] == "success"
    assert payload["component"] == "source-pg-largeobjects"
    assert payload["source"]["database"] == "postgres"
    assert payload["lob_count"] == 42
    # Round-trips as JSON and is clearly non-empty.
    assert json.loads(json.dumps(payload, default=str))["lob_count"] == 42


def test_build_output_accepts_zero_count():
    collector = make_collector()
    payload = collector._build_output(0)
    assert payload["status"] == "success"
    assert payload["lob_count"] == 0


def test_collect_writes_error_file_when_connection_fails(tmp_path):
    collector = make_collector(output_dir=str(tmp_path))
    # Nothing listening -> connect() fails fast (timeout=1).
    summary = collector.collect()

    assert summary["status"] == "failure"
    assert summary["output_file"] is None
    assert summary["lob_count"] is None

    error_file = tmp_path / PgCyanLobs.ERROR_FILENAME
    assert error_file.exists()
    assert not (tmp_path / PgCyanLobs.OUTPUT_FILENAME).exists()

    payload = json.loads(error_file.read_text())
    assert payload["status"] == "failure"
    assert payload["errors"]  # a non-empty error message is present
    assert "Connection failed" in payload["errors"][0]


def test_collect_writes_error_file_on_query_failure(tmp_path, monkeypatch):
    collector = make_collector(output_dir=str(tmp_path))
    monkeypatch.setattr(collector, "connect", lambda: True)

    def fake_fetch():
        collector.errors.append(
            "Large-object query failed: permission denied for table pg_largeobject_metadata"
        )
        return None

    monkeypatch.setattr(collector, "fetch_lob_count", fake_fetch)

    summary = collector.collect()

    assert summary["status"] == "failure"
    error_file = tmp_path / PgCyanLobs.ERROR_FILENAME
    assert error_file.exists()
    payload = json.loads(error_file.read_text())
    assert "permission denied" in payload["errors"][0]


def test_collect_writes_output_file_on_success(tmp_path, monkeypatch):
    collector = make_collector(output_dir=str(tmp_path))
    # Stub the DB interaction: pretend we connected and counted 7 large objects.
    monkeypatch.setattr(collector, "connect", lambda: True)
    monkeypatch.setattr(collector, "fetch_lob_count", lambda: 7)

    summary = collector.collect()

    assert summary["status"] == "success"
    assert summary["lob_count"] == 7

    output_file = tmp_path / PgCyanLobs.OUTPUT_FILENAME
    assert output_file.exists()
    assert not (tmp_path / PgCyanLobs.ERROR_FILENAME).exists()

    payload = json.loads(output_file.read_text())
    assert payload  # non-empty
    assert payload["lob_count"] == 7


def test_collect_treats_zero_count_as_success(tmp_path, monkeypatch):
    # A database with no large objects is a valid happy path, not a failure:
    # the JSON file is still written and non-empty.
    collector = make_collector(output_dir=str(tmp_path))
    monkeypatch.setattr(collector, "connect", lambda: True)
    monkeypatch.setattr(collector, "fetch_lob_count", lambda: 0)

    summary = collector.collect()

    assert summary["status"] == "success"
    assert summary["lob_count"] == 0

    output_file = tmp_path / PgCyanLobs.OUTPUT_FILENAME
    assert output_file.exists()
    assert not (tmp_path / PgCyanLobs.ERROR_FILENAME).exists()

    payload = json.loads(output_file.read_text())
    assert payload["lob_count"] == 0
