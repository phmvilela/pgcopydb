"""
Unit tests for the source-pg-metrics collector.

DB-free: they exercise guard paths (no connection), the JSON event shape, the
payload builders, and the output/error file-writing contract from issue #3:
  1. Happy path       -> a non-empty source-pg-metrics.json
  2. Connection issues -> source-pg-metrics-errors.json with a connection error
  3. Other issues      -> source-pg-metrics-errors.json with the source's error
Tests needing a live PostgreSQL server belong in a separate integration suite.
"""
import json

from pg_cyan.metrics import PgCyanMetrics


def make_collector(output_dir: str = ".") -> PgCyanMetrics:
    return PgCyanMetrics(
        host="localhost", port=5432, user="postgres",
        password="", database="postgres", timeout=1, output_dir=output_dir,
    )


SAMPLE_METRICS = {
    "version": {
        "version_string": "PostgreSQL 16.2",
        "server_version": "16.2",
        "server_version_num": 160002,
    },
    "database_size": {"size_bytes": 1234, "size_pretty": "1234 bytes"},
    "object_counts": {
        "tables": 3,
        "partitioned_tables": 0,
        "partitions": 0,
        "views": 1,
        "materialized_views": 0,
        "indexes": 4,
        "sequences": 2,
        "functions": 1,
        "procedures": 0,
        "triggers": 0,
        "extensions": 1,
    },
    "tables": [
        {"schema": "public", "name": "widgets", "relkind": "r",
         "size_bytes": 1000, "size_pretty": "1000 bytes", "row_estimate": 10.0},
    ],
}


def test_log_event_emits_valid_json(caplog):
    collector = make_collector()
    with caplog.at_level("INFO"):
        collector._log_event("unit_test", "success", "hello", {"k": "v"})
    payload = json.loads(caplog.records[-1].message)
    assert payload["component"] == "source-pg-metrics"
    assert payload["event_type"] == "unit_test"
    assert payload["status"] == "success"
    assert payload["details"] == {"k": "v"}
    assert payload["timestamp"].endswith("Z")


def test_fetch_metrics_guard_without_connection():
    collector = make_collector()
    # No connect() call -> conn is None -> must return None, not raise.
    assert collector.fetch_metrics() is None
    assert collector.errors  # an error message was recorded


def test_build_output_is_non_empty_and_well_formed():
    collector = make_collector()
    payload = collector._build_output(SAMPLE_METRICS)
    assert payload["status"] == "success"
    assert payload["source"]["database"] == "postgres"
    assert payload["version"]["server_version"] == "16.2"
    assert payload["object_counts"]["tables"] == 3
    assert payload["tables"] == SAMPLE_METRICS["tables"]
    # Round-trips as JSON and is clearly non-empty.
    assert json.loads(json.dumps(payload, default=str))["object_counts"]["tables"] == 3


def test_collect_writes_error_file_when_connection_fails(tmp_path):
    collector = make_collector(output_dir=str(tmp_path))
    # Nothing listening -> connect() fails fast (timeout=1).
    summary = collector.collect()

    assert summary["status"] == "failure"
    assert summary["output_file"] is None

    error_file = tmp_path / PgCyanMetrics.ERROR_FILENAME
    assert error_file.exists()
    assert not (tmp_path / PgCyanMetrics.OUTPUT_FILENAME).exists()

    payload = json.loads(error_file.read_text())
    assert payload["status"] == "failure"
    assert payload["errors"]  # a non-empty error message is present
    assert "Connection failed" in payload["errors"][0]


def test_collect_writes_error_file_on_query_failure(tmp_path, monkeypatch):
    collector = make_collector(output_dir=str(tmp_path))
    monkeypatch.setattr(collector, "connect", lambda: True)

    def fake_fetch():
        collector.errors.append("Metrics query failed: permission denied for table pg_class")
        return None

    monkeypatch.setattr(collector, "fetch_metrics", fake_fetch)

    summary = collector.collect()

    assert summary["status"] == "failure"
    error_file = tmp_path / PgCyanMetrics.ERROR_FILENAME
    assert error_file.exists()
    payload = json.loads(error_file.read_text())
    assert "permission denied" in payload["errors"][0]


def test_collect_writes_output_file_on_success(tmp_path, monkeypatch):
    collector = make_collector(output_dir=str(tmp_path))
    # Stub the DB interaction: pretend we connected and collected metrics.
    monkeypatch.setattr(collector, "connect", lambda: True)
    monkeypatch.setattr(collector, "fetch_metrics", lambda: SAMPLE_METRICS)

    summary = collector.collect()

    assert summary["status"] == "success"
    assert summary["table_count"] == 1

    output_file = tmp_path / PgCyanMetrics.OUTPUT_FILENAME
    assert output_file.exists()
    assert not (tmp_path / PgCyanMetrics.ERROR_FILENAME).exists()

    payload = json.loads(output_file.read_text())
    assert payload["tables"] == SAMPLE_METRICS["tables"]
    assert payload["object_counts"]["tables"] == 3


def test_collect_treats_empty_metrics_as_failure(tmp_path, monkeypatch):
    collector = make_collector(output_dir=str(tmp_path))
    monkeypatch.setattr(collector, "connect", lambda: True)
    monkeypatch.setattr(collector, "fetch_metrics", lambda: {})

    summary = collector.collect()

    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanMetrics.ERROR_FILENAME).exists()
    assert not (tmp_path / PgCyanMetrics.OUTPUT_FILENAME).exists()
