"""
Unit tests for the CDC follow component (`PgCyanFollow`).

See also: pg_cyan/copy/follow.py

DB-free: the pgcopydb child is a fake ``subprocess.Popen`` and the slot-state poll
is a fake psycopg2 connection. Tests cover the command build (follow on the
controller's --dir, no --resume), the slot/URI guards, the attach happy path (writes log +
readiness file), the child-dies-early and attach-timeout failures, and supervise().
"""
import json
import subprocess

import pytest

from pg_cyan.copy import follow as follow_module
from pg_cyan.copy.follow import PgCyanFollow


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
    # The child never attached, so the endpos graceful stop can't apply; make
    # it fail so close() falls back to signalling the (never-healthy) child.
    _run_recorder(monkeypatch, returncode=1)
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


class _CloseProc:
    """Fake follow child for close(): alive until ``wait`` is allowed to return.

    ``exits_on_wait`` models the graceful endpos stop -- the child reaches
    endpos and exits, so ``wait`` returns; otherwise ``wait`` times out and the
    signal fallback kicks in.
    """
    def __init__(self, exits_on_wait=True, rc=0):
        self.returncode = None
        self._exits_on_wait = exits_on_wait
        self._rc = rc
        self.terminated = False
        self.killed = False
        self.wait_timeouts = []

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True
        self.returncode = -9

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if self._exits_on_wait:
            self.returncode = self._rc
            return self._rc
        if self.returncode is None:
            raise subprocess.TimeoutExpired(cmd="pgcopydb", timeout=timeout)
        return self.returncode


def _run_recorder(monkeypatch, returncode=0):
    """Patch subprocess.run and record the argv it was called with."""
    calls = []

    def _run(cmd, *a, **k):
        calls.append(cmd)
        return _Completed(returncode=returncode, stdout="0/ABC")
    monkeypatch.setattr(follow_module.subprocess, "run", _run)
    return calls


def test_close_requests_endpos_and_exits_without_sigkill(tmp_path, monkeypatch):
    """The graceful path: close() sets the sentinel endpos --current and the
    follow child exits on its own -- no SIGTERM, no SIGKILL."""
    calls = _run_recorder(monkeypatch, returncode=0)
    proc = _CloseProc(exits_on_wait=True)
    follower = _make(tmp_path)
    follower.proc = proc
    follower.close()

    # endpos was requested against this follow's --dir + source, not a signal.
    assert calls == [[
        "pgcopydb", "stream", "sentinel", "set", "endpos", "--current",
        "--dir", str(tmp_path), "--source", "postgres://src/db",
    ]]
    assert proc.wait_timeouts == [follower.stop_timeout]
    assert proc.terminated is False
    assert proc.killed is False


def test_close_falls_back_to_signal_when_endpos_request_fails(tmp_path, monkeypatch):
    """If the endpos can't be set, close() escalates to the bounded signal
    fallback (short grace + reap, kept inside the 60s pod grace)."""
    _run_recorder(monkeypatch, returncode=1)  # sentinel set endpos fails
    proc = _CloseProc(exits_on_wait=False)    # and the child never exits on its own
    follower = _make(tmp_path)
    follower.proc = proc
    follower.close()

    assert proc.killed is True
    assert proc.wait_timeouts == [
        follower.FALLBACK_GRACE_TIMEOUT, follower.FALLBACK_REAP_TIMEOUT]


def test_close_falls_back_to_signal_when_endpos_does_not_stop_in_time(tmp_path, monkeypatch):
    """endpos was set but the child overran stop_timeout -> bounded fallback."""
    _run_recorder(monkeypatch, returncode=0)  # endpos set OK
    proc = _CloseProc(exits_on_wait=False)    # but it doesn't reach endpos in time
    follower = _make(tmp_path)
    follower.proc = proc
    follower.close()

    assert proc.killed is True
    # First wait is the endpos grace window, then the fallback (grace, reap).
    assert proc.wait_timeouts == [
        follower.stop_timeout,
        follower.FALLBACK_GRACE_TIMEOUT,
        follower.FALLBACK_REAP_TIMEOUT,
    ]


def test_close_is_noop_when_child_already_gone(tmp_path, monkeypatch):
    calls = _run_recorder(monkeypatch, returncode=0)
    proc = _CloseProc()
    proc.returncode = 0  # already exited
    follower = _make(tmp_path)
    follower.proc = proc
    follower.close()
    assert calls == []            # no endpos request
    assert proc.wait_timeouts == []


def test_fallback_timeouts_fit_the_pod_grace_window(tmp_path):
    """The whole teardown (endpos command + wait + signal fallback) must stay
    inside terminationGracePeriodSeconds: 60."""
    f = PgCyanFollow()
    total = (f.STOP_REQUEST_TIMEOUT + f.DEFAULT_STOP_TIMEOUT
             + f.FALLBACK_GRACE_TIMEOUT + f.FALLBACK_REAP_TIMEOUT)
    assert total < 60
