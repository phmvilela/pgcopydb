"""
Unit tests for the click CLI wiring.

We stub PgCyanPreflight so no database is touched — the goal is to verify that
CLI options/env vars flow into the orchestrator and that exit codes map
correctly to the readiness result.
"""
import pytest
from click.testing import CliRunner

from pg_cyan import cli as cli_module
from pg_cyan.copy.datashard import PgCyanDataShard as RealDataShard
from pg_cyan.copy.postfk import PgCyanForeignKeys as RealForeignKeys
from pg_cyan.copy.postindex import PgCyanPostIndex as RealPostIndex
from pg_cyan.copy.sequences import PgCyanSequences as RealSequences
from pg_cyan.orchestrator import PgCyanOrchestrator as RealOrchestrator
from pg_cyan.plan.planner import PgCyanPlanner as RealPlanner
from pg_cyan.copy.predata import PgCyanPreData as RealPreData
from pg_cyan.collect.capinputs import PgCyanCapacityInputs as RealCapacityInputs
from pg_cyan.collect.netprobe import PgCyanNetworkProbe as RealNetworkProbe
from pg_cyan.collect.sourcecap import PgCyanSourceCapacity as RealSourceCapacity
from pg_cyan.collect.targetcap import PgCyanTargetCapacity as RealTargetCapacity
from pg_cyan.collect.clustercap import PgCyanClusterCapacity as RealClusterCapacity


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
        return {"status": status, "namespace": self.namespace, "steps": [], "errors": []}


class _StubPreData(RealPreData):
    """Records constructor kwargs and a canned run() result.

    Subclasses the real wrapper (like ``_StubPlanner``) so the CLI's option
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


class _StubDataShard(RealDataShard):
    """Records constructor kwargs and a canned run() result.

    Subclasses the real wrapper (like ``_StubPreData``) so the CLI's option
    defaults, which read ``PgCyanDataShard.DEFAULT_*``, still resolve after
    monkeypatching. Never launches pgcopydb or opens a connection.
    """

    last_kwargs = None

    def __init__(self, **kwargs):
        _StubDataShard.last_kwargs = kwargs
        self.snapshot_id = kwargs.get("snapshot_id")
        # Success unless the caller passes the sentinel "bad" target.
        self._ok = kwargs.get("target_pguri") != "postgres://bad/db"

    def run(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "log_file": None, "log_error_file": None,
                "shard": self.snapshot_id, "snapshot": self.snapshot_id,
                "row_count_total": 0, "table_count": 0, "errors": []}


class _StubController:
    """Records constructor kwargs and a canned run() result.

    Does NOT subclass the real controller -- building it would try to load kube
    credentials -- so it stands in wholesale. Success unless namespace is 'bad_ns'.
    """

    last_kwargs = None
    #: Mirror the real class attribute the CLI reads for the default API port.
    DEFAULT_API_PORT = 9999

    def __init__(self, **kwargs):
        _StubController.last_kwargs = kwargs
        self._ok = kwargs.get("namespace") != "bad_ns"

    def run(self):
        status = "success" if self._ok else "failure"
        return {"status": status, "ready": self._ok, "snapshot": "0000000A-1",
                "owned": True, "steps": [], "errors": []}

    def park(self):
        return None  # no-op: real controller blocks until SIGTERM

    def serve_and_run(self):
        # Wait-to-start path: stands in for run()+park() gated by POST /start.
        return self.run()


class _StubPostIndex(RealPostIndex):
    """Records kwargs; success unless target is the 'bad' sentinel.

    Subclasses the real class so the CLI's option defaults (which read
    ``PgCyanPostIndex.DEFAULT_*``) still resolve after monkeypatching.
    """
    last_kwargs = None

    def __init__(self, **kwargs):
        _StubPostIndex.last_kwargs = kwargs
        self._ok = kwargs.get("target_pguri") != "postgres://bad/db"

    def run(self):
        return {"status": "success" if self._ok else "failure",
                "shard": self.last_kwargs.get("shard_name"), "errors": []}


class _StubForeignKeys(RealForeignKeys):
    """Records kwargs; success unless target is the 'bad' sentinel."""
    last_kwargs = None

    def __init__(self, **kwargs):
        _StubForeignKeys.last_kwargs = kwargs
        self._ok = kwargs.get("target_pguri") != "postgres://bad/db"

    def run(self):
        return {"status": "success" if self._ok else "failure",
                "fk_total": 0, "fk_validated": 0, "errors": []}


class _StubSequences(RealSequences):
    """Records kwargs; success unless target is the 'bad' sentinel."""
    last_kwargs = None

    def __init__(self, **kwargs):
        _StubSequences.last_kwargs = kwargs
        self._ok = kwargs.get("target_pguri") != "postgres://bad/db"

    def run(self):
        return {"status": "success" if self._ok else "failure",
                "sequences_total": 0, "sequences_synced": 0, "errors": []}


class _StubCapacityInputs(RealCapacityInputs):
    """Records kwargs; failure when the declared filename is the 'bad' sentinel."""
    last_kwargs = None

    def __init__(self, **kwargs):
        _StubCapacityInputs.last_kwargs = kwargs
        self._ok = kwargs.get("declared_filename") != "bad.json"

    def provide(self):
        return {"status": "success" if self._ok else "failure",
                "component": "capacity-inputs", "written": [], "skipped": {},
                "errors": []}


class _StubNetworkProbe(RealNetworkProbe):
    """Records kwargs; the probe is optional and always exits 0."""
    last_kwargs = None

    def __init__(self, **kwargs):
        _StubNetworkProbe.last_kwargs = kwargs

    def probe(self):
        return {"status": "success", "component": "network-probe",
                "output_file": "network.json", "error_file": None, "errors": []}


class _StubSourceCapacity(RealSourceCapacity):
    """Records kwargs; the collector is optional and always exits 0."""
    last_kwargs = None

    def __init__(self, **kwargs):
        _StubSourceCapacity.last_kwargs = kwargs

    def collect(self):
        return {"status": "success", "component": "source-capacity",
                "output_file": "source-capacity.json", "error_file": None, "errors": []}


class _StubTargetCapacity(RealTargetCapacity):
    """Records kwargs; the collector is optional and always exits 0."""
    last_kwargs = None

    def __init__(self, **kwargs):
        _StubTargetCapacity.last_kwargs = kwargs

    def collect(self):
        return {"status": "success", "component": "target-capacity",
                "output_file": "target-capacity.json", "error_file": None, "errors": []}


class _StubClusterCapacity(RealClusterCapacity):
    """Records kwargs; the collector is optional and always exits 0."""
    last_kwargs = None

    def __init__(self, **kwargs):
        _StubClusterCapacity.last_kwargs = kwargs

    def collect(self):
        return {"status": "success", "component": "cluster-capacity",
                "output_file": "cluster-capacity.json", "error_file": None, "errors": []}


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.setattr(cli_module, "PgCyanCapacityInputs", _StubCapacityInputs)
    monkeypatch.setattr(cli_module, "PgCyanNetworkProbe", _StubNetworkProbe)
    monkeypatch.setattr(cli_module, "PgCyanSourceCapacity", _StubSourceCapacity)
    monkeypatch.setattr(cli_module, "PgCyanTargetCapacity", _StubTargetCapacity)
    monkeypatch.setattr(cli_module, "PgCyanClusterCapacity", _StubClusterCapacity)
    monkeypatch.setattr(cli_module, "PgCyanPreflight", _StubChecker)
    monkeypatch.setattr(cli_module, "PgCyanParameters", _StubExporter)
    monkeypatch.setattr(cli_module, "PgCyanMetrics", _StubMetricsCollector)
    monkeypatch.setattr(cli_module, "PgCyanLobs", _StubLobsCollector)
    monkeypatch.setattr(cli_module, "PgCyanPlanner", _StubPlanner)
    monkeypatch.setattr(cli_module, "PgCyanOrchestrator", _StubOrchestrator)
    monkeypatch.setattr(cli_module, "PgCyanPreData", _StubPreData)
    monkeypatch.setattr(cli_module, "PgCyanDataShard", _StubDataShard)
    monkeypatch.setattr(cli_module, "PgCyanController", _StubController)
    monkeypatch.setattr(cli_module, "PgCyanPostIndex", _StubPostIndex)
    monkeypatch.setattr(cli_module, "PgCyanForeignKeys", _StubForeignKeys)
    monkeypatch.setattr(cli_module, "PgCyanSequences", _StubSequences)
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


# The in-cluster collector Job carries only the pgcopydb-style PGCOPYDB_SOURCE_PGURI
# (the shared `pgcyan-db-uris` Secret), not the discrete PG_SOURCE_* vars -- so the
# metrics/lobs commands must derive their psycopg2 connection from that URI,
# percent-decoding userinfo and honouring the URI's sslmode.
def test_source_pg_metrics_connection_from_pguri(runner, monkeypatch):
    monkeypatch.setenv(
        "PGCOPYDB_SOURCE_PGURI",
        "postgres://us%40er:p%3Aw@db.example.com:6432/ok_db?sslmode=require",
    )
    result = runner.invoke(cli_module.cli, ["source-pg-metrics"])
    assert result.exit_code == 0
    k = _StubMetricsCollector.last_kwargs
    assert k["host"] == "db.example.com"
    assert k["port"] == 6432
    assert k["user"] == "us@er"          # %40 decoded
    assert k["password"] == "p:w"        # %3A decoded
    assert k["database"] == "ok_db"
    assert k["sslmode"] == "require"


def test_source_largeobjects_connection_from_pguri(runner, monkeypatch):
    monkeypatch.setenv(
        "PGCOPYDB_SOURCE_PGURI",
        "postgres://u:s@lob.example.com:5555/ok_db?sslmode=verify-full",
    )
    result = runner.invoke(cli_module.cli, ["source-largeobjects"])
    assert result.exit_code == 0
    k = _StubLobsCollector.last_kwargs
    assert k["host"] == "lob.example.com"
    assert k["port"] == 5555
    assert k["sslmode"] == "verify-full"


def test_source_pg_metrics_explicit_flag_overrides_pguri(runner, monkeypatch):
    monkeypatch.setenv(
        "PGCOPYDB_SOURCE_PGURI",
        "postgres://u:s@uri-host:5432/uri_db?sslmode=require",
    )
    runner.invoke(cli_module.cli, ["source-pg-metrics", "--database", "ok_db",
                                   "--host", "flag-host"])
    k = _StubMetricsCollector.last_kwargs
    assert k["host"] == "flag-host"      # explicit flag beats the URI
    assert k["database"] == "ok_db"


def test_source_pg_metrics_falls_back_to_discrete_env(runner, monkeypatch):
    # No PGCOPYDB_SOURCE_PGURI -> the discrete PG_SOURCE_* path is unchanged.
    monkeypatch.delenv("PGCOPYDB_SOURCE_PGURI", raising=False)
    monkeypatch.setenv("PG_SOURCE_HOST", "legacy-host")
    monkeypatch.setenv("PG_SOURCE_DATABASE", "ok_db")
    result = runner.invoke(cli_module.cli, ["source-pg-metrics"])
    assert result.exit_code == 0
    k = _StubMetricsCollector.last_kwargs
    assert k["host"] == "legacy-host"
    assert k["sslmode"] is None          # unset -> connect() omits it


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


def test_capacity_inputs_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, ["capacity-inputs", "--output-dir", "ok_dir"])
    assert result.exit_code == 0
    assert _StubCapacityInputs.last_kwargs["output_dir"] == "ok_dir"


def test_capacity_inputs_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, ["capacity-inputs", "--declared-filename", "bad.json"])
    assert result.exit_code == 1


def test_capacity_inputs_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PG_INPUTS_DIR", "/inputs")
    monkeypatch.setenv("PGCYAN_CAPACITY_DECLARED", "caps.json")
    runner.invoke(cli_module.cli, ["capacity-inputs", "--output-dir", "ok_dir"])
    assert _StubCapacityInputs.last_kwargs["inputs_dir"] == "/inputs"
    assert _StubCapacityInputs.last_kwargs["declared_filename"] == "caps.json"


def test_network_probe_exit_zero_and_wires_options(runner):
    result = runner.invoke(cli_module.cli, [
        "network-probe", "--host", "srchost", "--output-dir", "ok_dir", "--streams", "6"])
    assert result.exit_code == 0
    assert _StubNetworkProbe.last_kwargs["host"] == "srchost"
    assert _StubNetworkProbe.last_kwargs["output_dir"] == "ok_dir"
    assert _StubNetworkProbe.last_kwargs["streams"] == 6


def test_network_probe_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PGCYAN_NET_STREAMS", "3")
    monkeypatch.setenv("PGCYAN_NET_RTT_SAMPLES", "9")
    runner.invoke(cli_module.cli, ["network-probe", "--output-dir", "ok_dir"])
    assert _StubNetworkProbe.last_kwargs["streams"] == 3
    assert _StubNetworkProbe.last_kwargs["rtt_samples"] == 9


def test_source_capacity_exit_zero_and_wires_options(runner):
    result = runner.invoke(cli_module.cli, [
        "source-capacity", "--host", "srchost", "--output-dir", "ok_dir",
        "--wal-sample-seconds", "1.5"])
    assert result.exit_code == 0
    assert _StubSourceCapacity.last_kwargs["host"] == "srchost"
    assert _StubSourceCapacity.last_kwargs["output_dir"] == "ok_dir"
    assert _StubSourceCapacity.last_kwargs["wal_sample_seconds"] == 1.5


def test_source_capacity_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PGCYAN_WAL_SAMPLE_SECONDS", "4")
    runner.invoke(cli_module.cli, ["source-capacity", "--output-dir", "ok_dir"])
    assert _StubSourceCapacity.last_kwargs["wal_sample_seconds"] == 4.0


def test_target_capacity_exit_zero_and_wires_options(runner):
    result = runner.invoke(cli_module.cli, [
        "target-capacity", "--host", "tgthost", "--output-dir", "ok_dir"])
    assert result.exit_code == 0
    assert _StubTargetCapacity.last_kwargs["host"] == "tgthost"
    assert _StubTargetCapacity.last_kwargs["output_dir"] == "ok_dir"


def test_target_capacity_host_falls_back_to_target_env(runner, monkeypatch):
    monkeypatch.setenv("PG_TARGET_HOST", "envtarget")
    monkeypatch.delenv("PGCOPYDB_TARGET_PGURI", raising=False)
    runner.invoke(cli_module.cli, ["target-capacity", "--output-dir", "ok_dir"])
    assert _StubTargetCapacity.last_kwargs["host"] == "envtarget"


def test_cluster_capacity_exit_zero_and_wires_output_dir(runner):
    result = runner.invoke(cli_module.cli, ["cluster-capacity", "--output-dir", "ok_dir"])
    assert result.exit_code == 0
    assert _StubClusterCapacity.last_kwargs["output_dir"] == "ok_dir"


def test_copylog_history_produces_and_exits_zero(runner, tmp_path):
    import json
    log = {"component": "data-shard", "shard": "shard-0", "duration_seconds": 5,
           "table_jobs": 2, "row_count_total": 100, "tables": {"public.a": 100}}
    (tmp_path / "migration-copy-log.json").write_text(json.dumps(log))
    result = runner.invoke(cli_module.cli, [
        "copylog-history", "--copylog", str(tmp_path / "migration-copy-log.json"),
        "--output-dir", str(tmp_path), "--run-id", "cli-run"])
    assert result.exit_code == 0
    hist = json.loads((tmp_path / "copylog-history.json").read_text())
    assert hist["runs"][0]["run_id"] == "cli-run"
    assert hist["runs"][0]["observations"][0]["duration_seconds"] == 10.0  # 5 x 2


def test_restore_predata_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, [
        "schema", "--target-pguri", "postgres://good/db",
        "--snapshot", "0000000A-1",
    ])
    assert result.exit_code == 0
    assert _StubPreData.last_kwargs["target_pguri"] == "postgres://good/db"
    assert _StubPreData.last_kwargs["snapshot_id"] == "0000000A-1"
    # target roles assumed pre-created -> --no-owner default is on
    assert _StubPreData.last_kwargs["no_owner"] is True


def test_restore_predata_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, [
        "schema", "--target-pguri", "postgres://bad/db",
        "--snapshot", "0000000A-1",
    ])
    assert result.exit_code == 1


def test_restore_predata_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PGCOPYDB_SOURCE_PGURI", "postgres://src/db")
    monkeypatch.setenv("PGCOPYDB_TARGET_PGURI", "postgres://tgt/db")
    monkeypatch.setenv("SNAPSHOT_ID", "0000000B-2")
    monkeypatch.setenv("PGCYAN_WORK_DIR", "/mywork")
    runner.invoke(cli_module.cli, ["schema"])
    assert _StubPreData.last_kwargs["source_pguri"] == "postgres://src/db"
    assert _StubPreData.last_kwargs["target_pguri"] == "postgres://tgt/db"
    assert _StubPreData.last_kwargs["snapshot_id"] == "0000000B-2"
    assert _StubPreData.last_kwargs["work_dir"] == "/mywork"


def test_restore_predata_owner_flag_overrides_default(runner):
    runner.invoke(cli_module.cli, [
        "schema", "--target-pguri", "postgres://good/db",
        "--snapshot", "x", "--owner", "--not-consistent",
    ])
    assert _StubPreData.last_kwargs["no_owner"] is False
    assert _StubPreData.last_kwargs["not_consistent"] is True


def test_copy_shard_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, [
        "copy", "--target-pguri", "postgres://good/db",
        "--snapshot", "0000000A-1", "--filter-file", "shard-0.filters",
        "--shard-name", "shard-0",
    ])
    assert result.exit_code == 0
    assert _StubDataShard.last_kwargs["target_pguri"] == "postgres://good/db"
    assert _StubDataShard.last_kwargs["snapshot_id"] == "0000000A-1"
    assert _StubDataShard.last_kwargs["filter_file"] == "shard-0.filters"
    assert _StubDataShard.last_kwargs["shard_name"] == "shard-0"
    # counting the copied rows is the verification default
    assert _StubDataShard.last_kwargs["count_rows"] is True


def test_copy_shard_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, [
        "copy", "--target-pguri", "postgres://bad/db",
        "--snapshot", "0000000A-1", "--filter-file", "shard-0.filters",
    ])
    assert result.exit_code == 1


def test_copy_shard_options_fall_back_to_env(runner, monkeypatch):
    monkeypatch.setenv("PGCOPYDB_SOURCE_PGURI", "postgres://src/db")
    monkeypatch.setenv("PGCOPYDB_TARGET_PGURI", "postgres://tgt/db")
    monkeypatch.setenv("SNAPSHOT_ID", "0000000B-2")
    monkeypatch.setenv("FILTER_FILE", "shard-1.filters")
    monkeypatch.setenv("SHARD_NAME", "shard-1")
    monkeypatch.setenv("SLOT_NAME", "pgcyan_slot")
    monkeypatch.setenv("PUBLICATION_NAME", "pgcyan_pub")
    runner.invoke(cli_module.cli, ["copy"])
    assert _StubDataShard.last_kwargs["source_pguri"] == "postgres://src/db"
    assert _StubDataShard.last_kwargs["target_pguri"] == "postgres://tgt/db"
    assert _StubDataShard.last_kwargs["snapshot_id"] == "0000000B-2"
    assert _StubDataShard.last_kwargs["filter_file"] == "shard-1.filters"
    assert _StubDataShard.last_kwargs["slot_name"] == "pgcyan_slot"
    assert _StubDataShard.last_kwargs["publication_name"] == "pgcyan_pub"


def test_copy_shard_no_count_rows_flag(runner):
    runner.invoke(cli_module.cli, [
        "copy", "--target-pguri", "postgres://good/db",
        "--snapshot", "x", "--filter-file", "shard-0.filters", "--no-count-rows",
    ])
    assert _StubDataShard.last_kwargs["count_rows"] is False


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


def test_build_indexes_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, [
        "indexes", "--source-pguri", "postgres://src/db",
        "--target-pguri", "postgres://good/db",
        "--filter-file", "shard-0.filters", "--shard-name", "idx-0",
    ])
    assert result.exit_code == 0
    assert _StubPostIndex.last_kwargs["filter_file"] == "shard-0.filters"
    assert _StubPostIndex.last_kwargs["shard_name"] == "idx-0"


def test_build_indexes_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, [
        "indexes", "--source-pguri", "postgres://src/db",
        "--target-pguri", "postgres://bad/db", "--filter-file", "shard-0.filters",
    ])
    assert result.exit_code == 1


def test_copy_sequences_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, [
        "sequences", "--source-pguri", "postgres://src/db",
        "--target-pguri", "postgres://good/db",
    ])
    assert result.exit_code == 0
    assert _StubSequences.last_kwargs["source_pguri"] == "postgres://src/db"


def test_copy_sequences_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, [
        "sequences", "--target-pguri", "postgres://bad/db",
    ])
    assert result.exit_code == 1


def test_build_foreign_keys_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, [
        "foreign-keys", "--source-pguri", "postgres://src/db",
        "--target-pguri", "postgres://good/db",
    ])
    assert result.exit_code == 0
    assert _StubForeignKeys.last_kwargs["source_pguri"] == "postgres://src/db"


def test_build_foreign_keys_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, [
        "foreign-keys", "--target-pguri", "postgres://bad/db",
    ])
    assert result.exit_code == 1


def test_run_migration_exit_zero_on_success(runner):
    result = runner.invoke(cli_module.cli, ["run-migration", "--namespace", "ok_ns"])
    assert result.exit_code == 0
    assert _StubController.last_kwargs["namespace"] == "ok_ns"


def test_run_migration_exit_one_on_failure(runner):
    result = runner.invoke(cli_module.cli, ["run-migration", "--namespace", "bad_ns"])
    assert result.exit_code == 1


def test_run_migration_wait_for_start_uses_serve_and_run(runner):
    # --wait-for-start routes through serve_and_run() (which the stub maps to a
    # successful run) rather than run()+park().
    result = runner.invoke(cli_module.cli,
                           ["run-migration", "--namespace", "ok_ns", "--wait-for-start"])
    assert result.exit_code == 0


def test_run_migration_kubeconfig_flag(runner):
    runner.invoke(cli_module.cli, ["run-migration", "--namespace", "ok_ns", "--kubeconfig"])
    assert _StubController.last_kwargs["in_cluster"] is False


def test_run_migration_stop_after_flows_through(runner):
    result = runner.invoke(cli_module.cli, ["run-migration", "--namespace", "ok_ns", "--stop-after", "copy"])
    assert result.exit_code == 0
    assert _StubController.last_kwargs["stop_after"] == "copy"


def test_run_migration_follow_cdc_flows_through(runner):
    # --follow-cdc must reach the controller (the manifest sets PGCYAN_FOLLOW_CDC);
    # a regression here silently skips CDC.
    result = runner.invoke(cli_module.cli, ["run-migration", "--namespace", "ok_ns", "--follow-cdc"])
    assert result.exit_code == 0
    assert _StubController.last_kwargs["follow_cdc"] is True
    # explicit off
    runner.invoke(cli_module.cli, ["run-migration", "--namespace", "ok_ns", "--no-follow-cdc"])
    assert _StubController.last_kwargs["follow_cdc"] is False


def test_run_migration_stop_after_rejects_unknown_step(runner):
    result = runner.invoke(cli_module.cli, ["run-migration", "--namespace", "ok_ns", "--stop-after", "99"])
    assert result.exit_code == 2   # click.Choice rejects at parse time
