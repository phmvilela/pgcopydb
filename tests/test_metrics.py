"""
Unit tests for the source-pg-metrics collector.

DB-free: they exercise guard paths (no connection), the JSON event shape, the
payload builders, and the output/error file-writing contract:
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
         "heap_bytes": 600, "heap_pretty": "600 bytes",
         "index_bytes": 400, "index_pretty": "400 bytes", "row_estimate": 10.0,
         "dead_tuple_ratio": 0.0, "toast_bytes": 0, "avg_row_width": 60},
        {"schema": "public", "name": "gadgets", "relkind": "r",
         "heap_bytes": 120, "heap_pretty": "120 bytes",
         "index_bytes": 80, "index_pretty": "80 bytes", "row_estimate": 5.0,
         "dead_tuple_ratio": 0.5, "toast_bytes": 40, "avg_row_width": 24},
    ],
    "table_totals": {"heap_bytes": 720, "index_bytes": 480, "total_bytes": 1200},
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


def test_table_totals_sum_heap_and_index_separately():
    # Heap and index are summed as distinct figures; total is their sum
    # (heap_bytes + index_bytes).
    totals = PgCyanMetrics._table_totals(SAMPLE_METRICS["tables"])
    assert totals["heap_bytes"] == 600 + 120
    assert totals["index_bytes"] == 400 + 80
    assert totals["total_bytes"] == totals["heap_bytes"] + totals["index_bytes"]


def test_table_totals_handle_empty_and_missing_values():
    assert PgCyanMetrics._table_totals([]) == {
        "heap_bytes": 0, "index_bytes": 0, "total_bytes": 0,
    }
    # A row missing/None figures must not raise; it contributes zero.
    totals = PgCyanMetrics._table_totals(
        [{"heap_bytes": None, "index_bytes": 50}, {"heap_bytes": 10}]
    )
    assert totals == {"heap_bytes": 10, "index_bytes": 50, "total_bytes": 60}


def test_build_output_carries_heap_index_split_and_totals():
    collector = make_collector()
    payload = collector._build_output(SAMPLE_METRICS)
    # Per-table split is heap_bytes + index_bytes, with no conflated size_bytes.
    widgets = payload["tables"][0]
    assert widgets["heap_bytes"] == 600
    assert widgets["index_bytes"] == 400
    assert "size_bytes" not in widgets
    # Totals sum to the total-relation-size figure.
    assert payload["table_totals"]["heap_bytes"] == 720
    assert payload["table_totals"]["index_bytes"] == 480
    assert payload["table_totals"]["total_bytes"] == 1200


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
    assert summary["table_count"] == 2

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
