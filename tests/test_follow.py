"""
Unit tests for the CDC follow component (`PgCyanFollow`, #26, Option A).

DB-free: the pgcopydb child is a fake ``subprocess.Popen`` and the slot-state poll
is a fake psycopg2 connection. Tests cover the command build (follow on the
controller's --dir, no --resume), the slot/URI guards, the attach happy path (writes log +
readiness file), the child-dies-early and attach-timeout failures, and supervise().
"""
import json

import pytest

from pg_cyan import follow as follow_module
from pg_cyan.follow import PgCyanFollow


class _FakeProc:
    """Fake Popen: ``poll`` returns None until ``exit_after`` polls, then rc."""
    def __init__(self, exit_after=None, returncode=0):
        self._polls = 0
        self._exit_after = exit_after
        self.returncode = None
        self._rc = returncode
        self.terminated = False

    def poll(self):
        self._polls += 1
        if self._exit_after is not None and self._polls >= self._exit_after:
            self.returncode = self._rc
            return self._rc
        return None

    def wait(self, timeout=None):
        self.returncode = self._rc
        return self._rc

    def terminate(self):
        self.terminated = True
        self.returncode = self._rc


class _Completed:
    """Fake subprocess.run result (for `stream sentinel set apply`)."""
    def __init__(self, returncode=0, stdout=""):
        self.returncode = returncode
        self.stdout = stdout


def _fake_run_ok(monkeypatch, returncode=0):
    """Patch subprocess.run (enable_apply) to a canned result."""
    monkeypatch.setattr(follow_module.subprocess, "run",
                        lambda *a, **k: _Completed(returncode=returncode, stdout="ok"))


class _SlotCursor:
    def __init__(self, active_after, found=True):
        self._active_after = active_after   # become active on the Nth call
        self._found = found
        self._calls = 0

    def execute(self, query, params=None):
        self._calls += 1

    def fetchone(self):
        if not self._found:
            return None
        return (self._calls >= self._active_after,)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _SlotConn:
    def __init__(self, cursor):
        self.autocommit = False
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _make(tmp_path, **kwargs):
    defaults = dict(source_pguri="postgres://src/db", slot_name="pgcyan_slot",
                    work_dir=str(tmp_path), output_dir=str(tmp_path),
                    ready_file=str(tmp_path / "ready"), poll_interval=0, attach_timeout=5)
    defaults.update(kwargs)
    return PgCyanFollow(**defaults)


class _OriginCursor:
    """Records SQL; reports the origin as existing iff `exists`."""
    def __init__(self, exists):
        self._exists = exists
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append(str(query))

    def fetchone(self):
        return (1,) if self._exists else None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_drop_stale_origin_drops_when_present(monkeypatch, tmp_path):
    cur = _OriginCursor(exists=True)
    monkeypatch.setattr(follow_module.psycopg2, "connect", lambda dsn: _SlotConn(cur))
    w = _make(tmp_path, target_pguri="postgres://tgt/db")
    assert w.drop_stale_origin() is True
    assert any("pg_replication_origin_drop" in q for q in cur.executed)


def test_drop_stale_origin_noop_when_absent(monkeypatch, tmp_path):
    cur = _OriginCursor(exists=False)
    monkeypatch.setattr(follow_module.psycopg2, "connect", lambda dsn: _SlotConn(cur))
    w = _make(tmp_path, target_pguri="postgres://tgt/db")
    assert w.drop_stale_origin() is True
    assert not any("pg_replication_origin_drop" in q for q in cur.executed)  # nothing dropped


def test_drop_stale_origin_noop_without_target(tmp_path):
    # No target URI -> no-op (guards against an unnecessary connection).
    assert _make(tmp_path).drop_stale_origin() is False


def test_build_follow_command_on_workdir_no_resume():
    cmd = PgCyanFollow(slot_name="pgcyan_slot", plugin="pgoutput",
                       work_dir="/pgcopydb").build_follow_command()
    assert cmd[:2] == ["pgcopydb", "follow"]
    assert "--dir" in cmd and "/pgcopydb" in cmd        # the controller's --dir
    assert "--slot-name" in cmd and "pgcyan_slot" in cmd
    assert "--plugin" in cmd and "pgoutput" in cmd
    assert "--not-consistent" in cmd
    # NOT --resume: it would skip creating the sentinel that snapshot --follow
    # never wrote, so follow --resume aborts (rc=12). See build_follow_command.
    assert "--resume" not in cmd


def test_start_attaches_enables_apply_writes_log_and_ready(tmp_path, monkeypatch):
    proc = _FakeProc(exit_after=None)   # stays alive
    monkeypatch.setattr(follow_module.subprocess, "Popen", lambda *a, **k: proc)
    monkeypatch.setattr(follow_module.psycopg2, "connect",
                        lambda dsn: _SlotConn(_SlotCursor(active_after=1)))
    _fake_run_ok(monkeypatch)             # `stream sentinel set apply` succeeds
    summary = _make(tmp_path).start()

    assert summary["status"] == "success"
    log = json.loads((tmp_path / PgCyanFollow.LOG_OUTPUT_FILENAME).read_text())
    assert log["status"] == "streaming" and log["slot"] == "pgcyan_slot"
    assert (tmp_path / "ready").exists()          # readiness gate created


def test_start_fails_if_apply_cannot_be_enabled(tmp_path, monkeypatch):
    # Attached but `stream sentinel set apply` keeps failing -> CDC would prefetch
    # forever without replaying, so start() fails (not a silent success).
    proc = _FakeProc(exit_after=None)
    monkeypatch.setattr(follow_module.subprocess, "Popen", lambda *a, **k: proc)
    monkeypatch.setattr(follow_module.psycopg2, "connect",
                        lambda dsn: _SlotConn(_SlotCursor(active_after=1)))
    _fake_run_ok(monkeypatch, returncode=1)   # enable_apply never succeeds
    monkeypatch.setattr(follow_module.time, "sleep", lambda *_: None)
    summary = _make(tmp_path).start()
    assert summary["status"] == "failure"
    assert proc.terminated is True                 # child torn down
    assert (tmp_path / PgCyanFollow.LOG_ERROR_FILENAME).exists()


def test_enable_apply_command_targets_workdir(tmp_path, monkeypatch):
    captured = {}

    def _run(cmd, **k):
        captured["cmd"] = cmd
        return _Completed(0)

    monkeypatch.setattr(follow_module.subprocess, "run", _run)
    assert _make(tmp_path, work_dir="/pgcopydb").enable_apply() is True
    assert captured["cmd"][:5] == ["pgcopydb", "stream", "sentinel", "set", "apply"]
    assert "--dir" in captured["cmd"] and "/pgcopydb" in captured["cmd"]


def test_start_no_slot_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(follow_module.subprocess, "Popen",
                        lambda *a, **k: pytest.fail("should not launch without a slot"))
    summary = _make(tmp_path, slot_name=None).start()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanFollow.LOG_ERROR_FILENAME).exists()


def test_start_child_dies_before_attach(tmp_path, monkeypatch):
    proc = _FakeProc(exit_after=1, returncode=6)   # dead on first poll
    monkeypatch.setattr(follow_module.subprocess, "Popen", lambda *a, **k: proc)
    monkeypatch.setattr(follow_module.psycopg2, "connect",
                        lambda dsn: _SlotConn(_SlotCursor(active_after=99)))
    summary = _make(tmp_path).start()
    assert summary["status"] == "failure"
    assert not (tmp_path / "ready").exists()
    assert (tmp_path / PgCyanFollow.LOG_ERROR_FILENAME).exists()


def test_start_attach_timeout_terminates_child(tmp_path, monkeypatch):
    proc = _FakeProc(exit_after=None)              # alive but slot never activates
    monkeypatch.setattr(follow_module.subprocess, "Popen", lambda *a, **k: proc)
    monkeypatch.setattr(follow_module.psycopg2, "connect",
                        lambda dsn: _SlotConn(_SlotCursor(active_after=999)))
    monkeypatch.setattr(follow_module.time, "monotonic",
                        _monotonic_seq([0, 1, 2, 100]))   # blow past the deadline
    summary = _make(tmp_path, attach_timeout=10).start()
    assert summary["status"] == "failure"
    assert proc.terminated is True


def test_supervise_clears_ready_when_child_exits(tmp_path, monkeypatch):
    proc = _FakeProc(exit_after=None)
    monkeypatch.setattr(follow_module.subprocess, "Popen", lambda *a, **k: proc)
    monkeypatch.setattr(follow_module.psycopg2, "connect",
                        lambda dsn: _SlotConn(_SlotCursor(active_after=1)))
    _fake_run_ok(monkeypatch)
    w = _make(tmp_path)
    w.start()
    assert (tmp_path / "ready").exists()
    # supervise returns when the child exits; readiness is cleared
    rc = w.supervise(stop=lambda: False)   # _FakeProc.wait() returns immediately
    assert rc == 0
    assert not (tmp_path / "ready").exists()


def _monotonic_seq(values):
    it = iter(values)
    last = [values[-1]]

    def _fn():
        try:
            last[0] = next(it)
        except StopIteration:
            pass
        return last[0]
    return _fn
