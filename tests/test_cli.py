"""
Unit tests for the click CLI wiring.

We stub PgCyanPreflight so no database is touched — the goal is to verify that
CLI options/env vars flow into the orchestrator and that exit codes map
correctly to the readiness result.
"""
import pytest
from click.testing import CliRunner

from pg_cyan import cli as cli_module


class _StubChecker:
    """Records constructor kwargs and returns a canned readiness result."""

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubChecker.last_kwargs = kwargs
        self._ready = kwargs["database"] == "ready_db"

    def run_all_checks(self):
        return {"summary": {"ready_for_migration": self._ready}}

    def close(self):
        pass


class _StubExporter:
    """Records constructor kwargs and returns a canned export summary."""

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubExporter.last_kwargs = kwargs
        self._ok = kwargs["database"] == "ok_db"

    def export(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "output_file": None, "error_file": None}

    def close(self):
        pass


class _StubMetricsCollector:
    """Records constructor kwargs and returns a canned collect summary."""

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubMetricsCollector.last_kwargs = kwargs
        self._ok = kwargs["database"] == "ok_db"

    def collect(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "output_file": None, "error_file": None}

    def close(self):
        pass


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.setattr(cli_module, "PgCyanPreflight", _StubChecker)
    monkeypatch.setattr(cli_module, "PgCyanParameters", _StubExporter)
    monkeypatch.setattr(cli_module, "PgCyanMetrics", _StubMetricsCollector)
    return CliRunner()


def test_version(runner):
    result = runner.invoke(cli_module.cli, ["--version"])
    assert result.exit_code == 0
    assert "pg_cyan" in result.output


def test_preflight_exit_zero_when_ready(runner):
    result = runner.invoke(cli_module.cli, ["preflight", "--database", "ready_db"])
    assert result.exit_code == 0
    assert _StubChecker.last_kwargs["database"] == "ready_db"


def test_preflight_exit_one_when_not_ready(runner):
    result = runner.invoke(cli_module.cli, ["preflight", "--database", "other"])
    assert result.exit_code == 1


def test_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PG_SOURCE_HOST", "db.internal")
    monkeypatch.setenv("PG_SOURCE_PORT", "6432")
    runner.invoke(cli_module.cli, ["preflight"])
    assert _StubChecker.last_kwargs["host"] == "db.internal"
    assert _StubChecker.last_kwargs["port"] == 6432


def test_source_parameters_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, ["source-parameters", "--database", "ok_db"])
    assert result.exit_code == 0
    assert _StubExporter.last_kwargs["database"] == "ok_db"


def test_source_parameters_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, ["source-parameters", "--database", "other"])
    assert result.exit_code == 1


def test_source_parameters_output_dir_falls_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", "/tmp/pgcyan-out")
    runner.invoke(cli_module.cli, ["source-parameters", "--database", "ok_db"])
    assert _StubExporter.last_kwargs["output_dir"] == "/tmp/pgcyan-out"


def test_source_pg_metrics_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, ["source-pg-metrics", "--database", "ok_db"])
    assert result.exit_code == 0
    assert _StubMetricsCollector.last_kwargs["database"] == "ok_db"


def test_source_pg_metrics_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, ["source-pg-metrics", "--database", "other"])
    assert result.exit_code == 1


def test_source_pg_metrics_output_dir_falls_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", "/tmp/pgcyan-out")
    runner.invoke(cli_module.cli, ["source-pg-metrics", "--database", "ok_db"])
    assert _StubMetricsCollector.last_kwargs["output_dir"] == "/tmp/pgcyan-out"
