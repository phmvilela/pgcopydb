"""
Unit tests for the click CLI wiring.

We stub PgCyanPreflight so no database is touched — the goal is to verify that
CLI options/env vars flow into the orchestrator and that exit codes map
correctly to the readiness result.
"""
import pytest
from click.testing import CliRunner

from pg_cyan import cli as cli_module
from pg_cyan.orchestrator import PgCyanOrchestrator as RealOrchestrator
from pg_cyan.planner import PgCyanPlanner as RealPlanner
from pg_cyan.predata import PgCyanPreData as RealPreData
from pg_cyan.snapshot import PgCyanSnapshot as RealSnapshot


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


class _StubLobsCollector:
    """Records constructor kwargs and returns a canned collect summary."""

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubLobsCollector.last_kwargs = kwargs
        self._ok = kwargs["database"] == "ok_db"

    def collect(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "output_file": None, "error_file": None}

    def close(self):
        pass


class _StubPlanner(RealPlanner):
    """Records constructor kwargs and returns a canned plan summary.

    Subclasses the real planner so the CLI's option-default lambdas (which read
    ``PgCyanPlanner.DEFAULT_*``) still resolve after monkeypatching.
    """

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubPlanner.last_kwargs = kwargs
        self._ok = kwargs["output_dir"] == "ok_dir"

    def plan(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "output_file": None, "error_file": None}


class _StubOrchestrator(RealOrchestrator):
    """Records constructor kwargs and returns a canned run summary.

    Subclasses the real orchestrator (like ``_StubPlanner``) so the CLI's
    option-default lambdas, which read ``PgCyanOrchestrator.DEFAULT_*``, still
    resolve after monkeypatching. Deliberately does NOT call ``super().__init__``
    -- no real Kubernetes client should be built for a CLI-wiring test.
    """

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubOrchestrator.last_kwargs = kwargs
        self.namespace = kwargs["namespace"]
        self._ok = self.namespace == "ok_ns"

    def run(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "namespace": self.namespace, "phases": [], "errors": []}


class _StubSnapshot(RealSnapshot):
    """Records constructor kwargs and canned establish/hold results.

    Subclasses the real holder (like ``_StubPlanner``) so the CLI's option
    defaults, which read ``PgCyanSnapshot.DEFAULT_*``, still resolve after
    monkeypatching. Never launches pgcopydb or opens a connection.
    """

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubSnapshot.last_kwargs = kwargs
        self._ok = kwargs["slot_name"] == "ok_slot"

    def establish(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "slot_file": None, "publication_file": None,
                "snapshot_name": "0000000A-1" if self._ok else None, "errors": []}

    def hold(self):
        return 0

    def close(self):
        pass


class _StubPreData(RealPreData):
    """Records constructor kwargs and a canned run() result.

    Subclasses the real wrapper (like ``_StubSnapshot``) so the CLI's option
    defaults, which read ``PgCyanPreData.DEFAULT_*``, still resolve after
    monkeypatching. Never launches pgcopydb.
    """

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubPreData.last_kwargs = kwargs
        self.snapshot_id = kwargs.get("snapshot_id")
        # Success unless the caller passes the sentinel "bad" target.
        self._ok = kwargs.get("target_pguri") != "postgres://bad/db"

    def run(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "log_file": None, "log_error_file": None,
                "snapshot": self.snapshot_id, "errors": []}


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.setattr(cli_module, "PgCyanPreflight", _StubChecker)
    monkeypatch.setattr(cli_module, "PgCyanParameters", _StubExporter)
    monkeypatch.setattr(cli_module, "PgCyanMetrics", _StubMetricsCollector)
    monkeypatch.setattr(cli_module, "PgCyanLobs", _StubLobsCollector)
    monkeypatch.setattr(cli_module, "PgCyanPlanner", _StubPlanner)
    monkeypatch.setattr(cli_module, "PgCyanOrchestrator", _StubOrchestrator)
    monkeypatch.setattr(cli_module, "PgCyanSnapshot", _StubSnapshot)
    monkeypatch.setattr(cli_module, "PgCyanPreData", _StubPreData)
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


def test_source_largeobjects_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, ["source-largeobjects", "--database", "ok_db"])
    assert result.exit_code == 0
    assert _StubLobsCollector.last_kwargs["database"] == "ok_db"


def test_source_largeobjects_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, ["source-largeobjects", "--database", "other"])
    assert result.exit_code == 1


def test_source_largeobjects_output_dir_falls_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PG_OUTPUT_DIR", "/tmp/pgcyan-out")
    runner.invoke(cli_module.cli, ["source-largeobjects", "--database", "ok_db"])
    assert _StubLobsCollector.last_kwargs["output_dir"] == "/tmp/pgcyan-out"


def test_plan_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, ["plan", "--output-dir", "ok_dir"])
    assert result.exit_code == 0
    assert _StubPlanner.last_kwargs["output_dir"] == "ok_dir"


def test_plan_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, ["plan", "--output-dir", "other"])
    assert result.exit_code == 1


def test_plan_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PG_INPUTS_DIR", "/inputs")
    monkeypatch.setenv("PGCYAN_SHARDS", "7")
    runner.invoke(cli_module.cli, ["plan", "--output-dir", "ok_dir"])
    assert _StubPlanner.last_kwargs["inputs_dir"] == "/inputs"
    assert _StubPlanner.last_kwargs["shards"] == 7


def test_snapshot_hold_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, ["snapshot-hold", "--slot-name", "ok_slot"])
    assert result.exit_code == 0
    assert _StubSnapshot.last_kwargs["slot_name"] == "ok_slot"


def test_snapshot_hold_exit_one_when_not_established(runner):
    result = runner.invoke(cli_module.cli, ["snapshot-hold", "--slot-name", "other"])
    assert result.exit_code == 1


def test_snapshot_hold_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PGCYAN_PUBLICATION", "env_pub")
    monkeypatch.setenv("PGCYAN_SLOT", "ok_slot")
    monkeypatch.setenv("PGCYAN_SNAPSHOT_MODE", "frozen-source")
    monkeypatch.setenv("PGCOPYDB_SOURCE_PGURI", "postgres://h/db")
    runner.invoke(cli_module.cli, ["snapshot-hold"])
    assert _StubSnapshot.last_kwargs["publication_name"] == "env_pub"
    assert _StubSnapshot.last_kwargs["slot_name"] == "ok_slot"
    assert _StubSnapshot.last_kwargs["snapshot_mode"] == "frozen-source"
    assert _StubSnapshot.last_kwargs["source_pguri"] == "postgres://h/db"


def test_restore_predata_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, [
        "restore-predata", "--target-pguri", "postgres://good/db",
        "--snapshot", "0000000A-1",
    ])
    assert result.exit_code == 0
    assert _StubPreData.last_kwargs["target_pguri"] == "postgres://good/db"
    assert _StubPreData.last_kwargs["snapshot_id"] == "0000000A-1"
    # target roles assumed pre-created -> --no-owner default is on
    assert _StubPreData.last_kwargs["no_owner"] is True


def test_restore_predata_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, [
        "restore-predata", "--target-pguri", "postgres://bad/db",
        "--snapshot", "0000000A-1",
    ])
    assert result.exit_code == 1


def test_restore_predata_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PGCOPYDB_SOURCE_PGURI", "postgres://src/db")
    monkeypatch.setenv("PGCOPYDB_TARGET_PGURI", "postgres://tgt/db")
    monkeypatch.setenv("SNAPSHOT_ID", "0000000B-2")
    monkeypatch.setenv("PGCYAN_WORK_DIR", "/mywork")
    runner.invoke(cli_module.cli, ["restore-predata"])
    assert _StubPreData.last_kwargs["source_pguri"] == "postgres://src/db"
    assert _StubPreData.last_kwargs["target_pguri"] == "postgres://tgt/db"
    assert _StubPreData.last_kwargs["snapshot_id"] == "0000000B-2"
    assert _StubPreData.last_kwargs["work_dir"] == "/mywork"


def test_restore_predata_owner_flag_overrides_default(runner):
    runner.invoke(cli_module.cli, [
        "restore-predata", "--target-pguri", "postgres://good/db",
        "--snapshot", "x", "--owner", "--not-consistent",
    ])
    assert _StubPreData.last_kwargs["no_owner"] is False
    assert _StubPreData.last_kwargs["not_consistent"] is True


def test_orchestrate_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, ["orchestrate", "--namespace", "ok_ns"])
    assert result.exit_code == 0
    assert _StubOrchestrator.last_kwargs["namespace"] == "ok_ns"


def test_orchestrate_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, ["orchestrate", "--namespace", "other_ns"])
    assert result.exit_code == 1


def test_orchestrate_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PGCYAN_NAMESPACE", "ok_ns")
    monkeypatch.setenv("PGCYAN_JOB_TIMEOUT", "42")
    runner.invoke(cli_module.cli, ["orchestrate"])
    assert _StubOrchestrator.last_kwargs["namespace"] == "ok_ns"
    assert _StubOrchestrator.last_kwargs["job_timeout"] == 42


def test_orchestrate_kubeconfig_flag(runner):
    runner.invoke(cli_module.cli, ["orchestrate", "--namespace", "ok_ns", "--kubeconfig"])
    assert _StubOrchestrator.last_kwargs["in_cluster"] is False
