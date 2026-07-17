"""
Unit tests for the source-parameters exporter.

DB-free: they exercise guard paths (no connection), the JSON event shape, the
payload builders, and the output/error file-writing contract from issue #4:
  1. Happy path  -> a non-empty pg-source-parameters.json
  2. Any failure -> pg-source-parameters-errors.json with an error message
Tests needing a live PostgreSQL server belong in a separate integration suite.
"""
import json

from pg_cyan.parameters import PgCyanParameters


def make_exporter(output_dir: str = ".") -> PgCyanParameters:
    return PgCyanParameters(
        host="localhost", port=5432, user="postgres",
        password="", database="postgres", timeout=1, output_dir=output_dir,
    )


def test_log_event_emits_valid_json(caplog):
    exporter = make_exporter()
    with caplog.at_level("INFO"):
        exporter._log_event("unit_test", "success", "hello", {"k": "v"})
    payload = json.loads(caplog.records[-1].message)
    assert payload["component"] == "source-parameters"
    assert payload["event_type"] == "unit_test"
    assert payload["status"] == "success"
    assert payload["details"] == {"k": "v"}
    assert payload["timestamp"].endswith("Z")


def test_fetch_parameters_guard_without_connection():
    exporter = make_exporter()
    # No connect() call -> conn is None -> must return None, not raise.
    assert exporter.fetch_parameters() is None
    assert exporter.errors  # an error message was recorded


def test_build_output_is_non_empty_and_well_formed():
    exporter = make_exporter()
    params = [
        {"name": "wal_level", "setting": "logical", "context": "postmaster"},
        {"name": "max_wal_senders", "setting": "10", "context": "postmaster"},
    ]
    payload = exporter._build_output(params)
    assert payload["status"] == "success"
    assert payload["parameter_count"] == 2
    assert payload["parameters"] == params
    assert payload["source"]["database"] == "postgres"
    # Round-trips as JSON and is clearly non-empty.
    assert json.loads(json.dumps(payload, default=str))["parameter_count"] == 2


def test_export_writes_error_file_when_connection_fails(tmp_path):
    exporter = make_exporter(output_dir=str(tmp_path))
    # Nothing listening -> connect() fails fast (timeout=1).
    summary = exporter.export()

    assert summary["status"] == "failure"
    assert summary["output_file"] is None

    error_file = tmp_path / PgCyanParameters.ERROR_FILENAME
    assert error_file.exists()
    assert not (tmp_path / PgCyanParameters.OUTPUT_FILENAME).exists()

    payload = json.loads(error_file.read_text())
    assert payload["status"] == "failure"
    assert payload["errors"]  # a non-empty error message is present


def test_export_writes_output_file_on_success(tmp_path, monkeypatch):
    exporter = make_exporter(output_dir=str(tmp_path))
    params = [{"name": "wal_level", "setting": "logical"}]
    # Stub the DB interaction: pretend we connected and read parameters.
    monkeypatch.setattr(exporter, "connect", lambda: True)
    monkeypatch.setattr(exporter, "fetch_parameters", lambda: params)

    summary = exporter.export()

    assert summary["status"] == "success"
    assert summary["parameter_count"] == 1

    output_file = tmp_path / PgCyanParameters.OUTPUT_FILENAME
    assert output_file.exists()
    assert not (tmp_path / PgCyanParameters.ERROR_FILENAME).exists()

    payload = json.loads(output_file.read_text())
    assert payload["parameters"] == params
    assert payload["parameter_count"] == 1


def test_export_treats_empty_parameter_set_as_failure(tmp_path, monkeypatch):
    exporter = make_exporter(output_dir=str(tmp_path))
    monkeypatch.setattr(exporter, "connect", lambda: True)
    monkeypatch.setattr(exporter, "fetch_parameters", lambda: [])

    summary = exporter.export()

    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanParameters.ERROR_FILENAME).exists()
    assert not (tmp_path / PgCyanParameters.OUTPUT_FILENAME).exists()
