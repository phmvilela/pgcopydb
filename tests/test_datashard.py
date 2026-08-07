"""
Unit tests for the sharded initial copy (`pg_cyan.cli copy`).

See also: pg_cyan/datashard.py

DB-free and process-free: the pgcopydb child AND the kubectl copy-log publish are
stubbed via a fake ``subprocess.run``, and the target connection via a fake
``psycopg2.connect``, so nothing spawns a real process or touches a database /
cluster. Tests exercise the command build (``pgcopydb copy table-data``, DATA
ONLY), the snapshot/filter guards, the filter-file parsing, the per-table row
counting (happy path, connection failure, per-table failure), the per-shard
copy-log ConfigMap publish, the run() happy path + failure branches, and the
output-file shape.
"""
import json
import subprocess
from collections import deque

import pytest

from pg_cyan import datashard as datashard_module
from pg_cyan.datashard import PgCyanDataShard


# ------------------------------------------------------------------- fakes

class _FakeCompleted:
    def __init__(self, returncode=0, stdout=""):
        self.returncode = returncode
        self.stdout = stdout


def _fake_run_factory(recorder, returncodes=None):
    """Fake subprocess.run for BOTH pgcopydb (no check) and kubectl (check=True).

    ``returncodes`` maps a substring of the command (e.g. "pgcopydb", "kubectl")
    to a return code; missing -> 0. Honours ``check=True`` by raising
    CalledProcessError, and echoes any ``input=`` back as stdout so the
    create|label|apply kubectl pipeline threads through.
    """
    returncodes = returncodes or {}

    def _fake_run(cmd, **kwargs):
        recorder.append(cmd)
        joined = " ".join(cmd)
        rc = 0
        for needle, code in returncodes.items():
            if needle in joined:
                rc = code
                break
        # kubectl steps pass their rendered YAML on stdin; echo it onward.
        stdout = kwargs.get("input") or "rendered-configmap-yaml\n"
        if kwargs.get("check") and rc != 0:
            raise subprocess.CalledProcessError(rc, cmd, output=stdout, stderr="boom")
        return _FakeCompleted(returncode=rc, stdout=stdout)
    return _fake_run


class _FakeCursor:
    def __init__(self, counts, fail_on=None):
        self._counts = deque(counts)
        self._fail_on = fail_on
        self.executed = []

    def execute(self, query, params=None):
        text = str(query)
        self.executed.append(text)
        if "statement_timeout" in text:
            return
        if self._fail_on and self._fail_on in text:
            raise datashard_module.PgError("relation blew up")

    def fetchone(self):
        return (self._counts.popleft(),)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, cursor):
        self.autocommit = False
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def _connect_factory(cursor=None, raise_connect=False):
    def _connect(dsn):
        if raise_connect:
            raise datashard_module.PgError("could not connect")
        return _FakeConn(cursor)
    return _connect


FILTER_BODY = "[include-only-table]\npublic.orders\npublic.customers\nshop.items\n"


@pytest.fixture
def filter_file(tmp_path):
    path = tmp_path / "shard-0.filters"
    path.write_text(FILTER_BODY, encoding="utf-8")
    return str(path)


def _make_shard(tmp_path, filter_file, **kwargs):
    defaults = dict(
        source_pguri="postgres://src/db",
        target_pguri="postgres://tgt/db",
        snapshot_id="0000000A-1",
        filter_file=filter_file,          # absolute -> used as-is
        shard_name="shard-0",
        slot_name="pgcyan_slot",
        publication_name="pgcyan_pub",
        work_dir=str(tmp_path),
        output_dir=str(tmp_path),
        namespace="pgcyan-migration",
    )
    defaults.update(kwargs)
    return PgCyanDataShard(**defaults)


# ----------------------------------------------------------- command build

def test_build_copy_command_is_data_only_table_data(tmp_path, filter_file):
    shard = _make_shard(tmp_path, filter_file, table_jobs=4, split_tables_larger_than="50GB")
    cmd = shard.build_copy_command()
    joined = " ".join(cmd)
    # DATA ONLY: copy table-data, never `clone`.
    assert cmd[:3] == ["pgcopydb", "copy", "table-data"]
    assert "clone" not in cmd
    assert "--filters" in cmd and filter_file in cmd
    assert "--snapshot" in cmd and "0000000A-1" in cmd
    assert "--table-jobs" in cmd and "4" in cmd
    assert "--split-tables-larger-than" in cmd and "50GB" in cmd
    assert "--skip-vacuum" in joined
    # --resume (continue on the RWO disk), and --restart deliberately absent:
    # `copy table-data` rejects `--restart=false`, and restart WIPES temp files.
    assert "--resume" in cmd
    assert not any(a.startswith("--restart") for a in cmd)
    # the copy step builds no indexes; that is inherent to table-data (no --index-jobs).
    assert "--index-jobs" not in joined


# ------------------------------------------------------------ filter parse

def test_parse_filter_tables_reads_include_only_section(tmp_path, filter_file):
    shard = _make_shard(tmp_path, filter_file)
    assert shard.parse_filter_tables() == [
        ("public", "orders"), ("public", "customers"), ("shop", "items"),
    ]


def test_parse_filter_tables_missing_file_records_error(tmp_path):
    shard = _make_shard(tmp_path, str(tmp_path / "nope.filters"))
    assert shard.parse_filter_tables() == []
    assert any("filter file" in e for e in shard.errors)


# --------------------------------------------------------------- counting

def test_count_target_rows_happy_path(tmp_path, filter_file, monkeypatch):
    cursor = _FakeCursor(counts=[10, 20, 30])
    monkeypatch.setattr(datashard_module.psycopg2, "connect",
                        _connect_factory(cursor=cursor))
    shard = _make_shard(tmp_path, filter_file)
    counts = shard.count_target_rows(shard.parse_filter_tables())
    assert counts == {"public.orders": 10, "public.customers": 20, "shop.items": 30}


def test_count_target_rows_connection_failure_returns_all_none(tmp_path, filter_file, monkeypatch):
    monkeypatch.setattr(datashard_module.psycopg2, "connect",
                        _connect_factory(raise_connect=True))
    shard = _make_shard(tmp_path, filter_file)
    counts = shard.count_target_rows(shard.parse_filter_tables())
    assert set(counts.values()) == {None}
    assert any("connect to target" in e for e in shard.errors)


def test_count_target_rows_per_table_failure_keeps_the_rest(tmp_path, filter_file, monkeypatch):
    cursor = _FakeCursor(counts=[10, 30], fail_on="customers")
    monkeypatch.setattr(datashard_module.psycopg2, "connect",
                        _connect_factory(cursor=cursor))
    shard = _make_shard(tmp_path, filter_file)
    counts = shard.count_target_rows(shard.parse_filter_tables())
    assert counts["public.orders"] == 10
    assert counts["public.customers"] is None
    assert counts["shop.items"] == 30
    assert any("customers" in e for e in shard.errors)


# ------------------------------------------------------ copy-log ConfigMap

def test_publish_copylog_configmap_invokes_kubectl(tmp_path, filter_file):
    recorder = []
    shard = _make_shard(tmp_path, filter_file, kubectl_bin="kubectl")
    # datashard derives the name from the shard name.
    assert shard.copylog_configmap == "pgcyan-copylog-shard-0"
    log_path = str(tmp_path / "migration-copy-log.json")
    (tmp_path / "migration-copy-log.json").write_text("{}", encoding="utf-8")

    import subprocess as sp
    orig = sp.run
    try:
        sp.run = _fake_run_factory(recorder)
        assert shard.publish_copylog_configmap(log_path) is True
    finally:
        sp.run = orig

    verbs = [c[1] for c in recorder if len(c) > 1]
    assert "create" in verbs and "label" in verbs and "apply" in verbs
    # the label carries the selector the orchestrator lists on
    label_cmd = next(c for c in recorder if "label" in c)
    assert f"{PgCyanDataShard.COPY_LOG_LABEL}=shard" in label_cmd


def test_publish_copylog_configmap_noop_without_name(tmp_path, filter_file):
    shard = _make_shard(tmp_path, filter_file, shard_name=None, copylog_configmap=None)
    # no shard name and no explicit configmap -> nothing to publish to
    assert shard.copylog_configmap is None
    assert shard.publish_copylog_configmap("/whatever") is True


def test_publish_copylog_configmap_failure_returns_false(tmp_path, filter_file, monkeypatch):
    recorder = []
    monkeypatch.setattr(datashard_module.subprocess, "run",
                        _fake_run_factory(recorder, returncodes={"kubectl": 1}))
    shard = _make_shard(tmp_path, filter_file)
    assert shard.publish_copylog_configmap(str(tmp_path / "x.json")) is False
    assert any("copy-log ConfigMap" in e for e in shard.errors)


# --------------------------------------------------------------- run()

def test_run_happy_path_writes_and_publishes(tmp_path, filter_file, monkeypatch):
    recorder = []
    monkeypatch.setattr(datashard_module.subprocess, "run", _fake_run_factory(recorder))
    cursor = _FakeCursor(counts=[10, 20, 30])
    monkeypatch.setattr(datashard_module.psycopg2, "connect",
                        _connect_factory(cursor=cursor))
    shard = _make_shard(tmp_path, filter_file)

    summary = shard.run()

    assert summary["status"] == "success"
    assert summary["row_count_total"] == 60
    assert summary["table_count"] == 3
    assert summary["published"] is True

    log = json.loads((tmp_path / PgCyanDataShard.LOG_OUTPUT_FILENAME).read_text())
    assert log["status"] == "completed"
    assert log["slot"] == "pgcyan_slot"
    assert log["publication"] == "pgcyan_pub"
    assert log["snapshot"] == "0000000A-1"
    assert log["tables"] == {"public.orders": 10, "public.customers": 20, "shop.items": 30}
    assert log["row_count_total"] == 60

    # the pgcopydb child was `copy table-data`, and kubectl published the log
    assert any(c[:3] == ["pgcopydb", "copy", "table-data"] for c in recorder)
    assert any(c[0] == "kubectl" and "apply" in c for c in recorder)


def test_run_no_count_rows_skips_counting(tmp_path, filter_file, monkeypatch):
    recorder = []
    monkeypatch.setattr(datashard_module.subprocess, "run", _fake_run_factory(recorder))

    def _boom(dsn):
        raise AssertionError("connect must not be called with --no-count-rows")
    monkeypatch.setattr(datashard_module.psycopg2, "connect", _boom)

    shard = _make_shard(tmp_path, filter_file, count_rows=False)
    summary = shard.run()
    assert summary["status"] == "success"
    log = json.loads((tmp_path / PgCyanDataShard.LOG_OUTPUT_FILENAME).read_text())
    assert set(log["tables"].values()) == {None}
    assert log["counted"] is False


def test_run_missing_snapshot_fails_before_copy(tmp_path, filter_file, monkeypatch):
    called = []
    monkeypatch.setattr(datashard_module.subprocess, "run", _fake_run_factory(called))
    shard = _make_shard(tmp_path, filter_file, snapshot_id=None)
    summary = shard.run()
    assert summary["status"] == "failure"
    assert not called  # never launched pgcopydb
    assert (tmp_path / PgCyanDataShard.LOG_ERROR_FILENAME).exists()


def test_run_missing_filter_file_fails(tmp_path, monkeypatch):
    called = []
    monkeypatch.setattr(datashard_module.subprocess, "run", _fake_run_factory(called))
    shard = _make_shard(tmp_path, str(tmp_path / "absent.filters"))
    summary = shard.run()
    assert summary["status"] == "failure"
    assert not called
    err = json.loads((tmp_path / PgCyanDataShard.LOG_ERROR_FILENAME).read_text())
    assert err["status"] == "failure"


def test_run_copy_failure_writes_error_and_skips_count(tmp_path, filter_file, monkeypatch):
    monkeypatch.setattr(datashard_module.subprocess, "run",
                        _fake_run_factory([], returncodes={"pgcopydb": 7}))

    def _boom(dsn):
        raise AssertionError("must not count after a failed copy")
    monkeypatch.setattr(datashard_module.psycopg2, "connect", _boom)

    shard = _make_shard(tmp_path, filter_file)
    summary = shard.run()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanDataShard.LOG_ERROR_FILENAME).exists()
    assert not (tmp_path / PgCyanDataShard.LOG_OUTPUT_FILENAME).exists()


def test_run_publish_failure_does_not_fail_the_copy(tmp_path, filter_file, monkeypatch):
    # pgcopydb copy succeeds (rc 0); kubectl publish fails (rc 1). The copy is the
    # deliverable, so the shard still succeeds -- just with published=False.
    monkeypatch.setattr(datashard_module.subprocess, "run",
                        _fake_run_factory([], returncodes={"kubectl": 1}))
    cursor = _FakeCursor(counts=[10, 20, 30])
    monkeypatch.setattr(datashard_module.psycopg2, "connect",
                        _connect_factory(cursor=cursor))
    shard = _make_shard(tmp_path, filter_file)
    summary = shard.run()
    assert summary["status"] == "success"
    assert summary["published"] is False
    assert (tmp_path / PgCyanDataShard.LOG_OUTPUT_FILENAME).exists()
