"""
Unit tests for the shared graceful-shutdown helper (``pg_cyan.shutdown``).

DB-free: the pgcopydb child is a fake ``subprocess.Popen``. Covers the no-op
paths, the clean SIGTERM exit, and the SIGKILL escalation with the reconciled
(grace, reap) timeouts.

See also: pg_cyan/shutdown.py
"""
import signal
import subprocess
from unittest.mock import patch

from pg_cyan.shutdown import (
    graceful_terminate,
    GRACEFUL_SHUTDOWN_TIMEOUT,
    SIGKILL_REAP_TIMEOUT,
)


class _FakeProc:
    """A pgcopydb child stand-in that records how it was torn down.

    ``sigterm_exits`` controls whether the child stops on SIGTERM (clean) or
    ignores it and must be SIGKILLed.
    """

    def __init__(self, alive=True, returncode=None, sigterm_exits=True):
        self.returncode = None if alive else returncode
        self.sigterm_exits = sigterm_exits
        self.terminated = False
        self.killed = False
        self.wait_timeouts = []

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        if self.sigterm_exits:
            self.returncode = 0

    def kill(self):
        self.killed = True
        self.returncode = -9

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if self.returncode is None:  # ignored SIGTERM -> graceful wait times out
            raise subprocess.TimeoutExpired(cmd="pgcopydb", timeout=timeout)
        return self.returncode


def test_noop_when_proc_is_none():
    graceful_terminate(None)  # must not raise


def test_noop_when_already_exited():
    proc = _FakeProc(alive=False, returncode=0)
    graceful_terminate(proc)
    assert proc.terminated is False
    assert proc.killed is False


def test_clean_sigterm_exit_no_kill_no_log():
    events = []
    proc = _FakeProc(alive=True, sigterm_exits=True)
    graceful_terminate(proc, log_event=lambda *a, **k: events.append(a))
    assert proc.terminated is True
    assert proc.killed is False
    assert proc.wait_timeouts == [GRACEFUL_SHUTDOWN_TIMEOUT]
    assert events == []  # no SIGKILL escalation -> nothing logged


def test_escalates_to_sigkill_with_reconciled_timeouts():
    events = []
    proc = _FakeProc(alive=True, sigterm_exits=False)  # ignores SIGTERM
    graceful_terminate(proc, log_event=lambda *a, **k: events.append(a))
    assert proc.killed is True
    # Graceful window first, then the short reap -- both bounded (grace+reap <= 60).
    assert proc.wait_timeouts == [GRACEFUL_SHUTDOWN_TIMEOUT, SIGKILL_REAP_TIMEOUT]
    assert GRACEFUL_SHUTDOWN_TIMEOUT < 60
    assert GRACEFUL_SHUTDOWN_TIMEOUT + SIGKILL_REAP_TIMEOUT <= 60
    # The escalation is logged as a shutdown warning.
    assert events and events[0][0] == "shutdown" and events[0][1] == "warning"


def test_custom_timeouts_are_honored():
    proc = _FakeProc(alive=True, sigterm_exits=False)
    graceful_terminate(proc, grace_timeout=10, reap_timeout=2)
    assert proc.wait_timeouts == [10, 2]


def test_oserror_on_terminate_is_swallowed():
    class _Raises(_FakeProc):
        def terminate(self):
            raise OSError("already gone")

    proc = _Raises(alive=True)
    graceful_terminate(proc)  # must not raise
    assert proc.killed is False


class _FakeProcWithPid(_FakeProc):
    """A real-pid stand-in: exercises the process-group signalling path.

    ``pgcopydb follow`` forks prefetch/catchup workers that share its process
    group and only THEY check for SIGTERM while actively streaming (see
    ``pg_cyan/follow.py`` and ``shutdown._signal_group_or_proc``) -- so the
    fix signals the whole group, not just this one pid.
    """

    def __init__(self, pid=4242, **kwargs):
        super().__init__(**kwargs)
        self.pid = pid


def test_clean_exit_signals_the_process_group_not_just_the_pid():
    proc = _FakeProcWithPid(alive=True)

    def _killpg(pgid, sig):
        if sig == signal.SIGTERM:
            proc.returncode = 0  # simulate the group exiting cleanly

    with patch("pg_cyan.shutdown.os.getpgid", return_value=777) as getpgid, \
            patch("pg_cyan.shutdown.os.killpg", side_effect=_killpg) as killpg:
        graceful_terminate(proc)
    getpgid.assert_called_once_with(4242)
    killpg.assert_called_once_with(777, signal.SIGTERM)
    # The group signal path is used instead of the direct proc.terminate().
    assert proc.terminated is False


def test_sigkill_escalation_also_targets_the_process_group():
    proc = _FakeProcWithPid(alive=True)  # never responds to the group SIGTERM
    with patch("pg_cyan.shutdown.os.getpgid", return_value=777), \
            patch("pg_cyan.shutdown.os.killpg") as killpg:
        graceful_terminate(proc)
    assert killpg.call_args_list == [
        ((777, signal.SIGTERM),),
        ((777, signal.SIGKILL),),
    ]
    assert proc.killed is False  # group signal used, not the direct proc.kill()


def test_falls_back_to_direct_signal_when_process_group_lookup_fails():
    """No real pgcopydb process behind this pid (already reaped) -- degrade
    to signalling the pid directly rather than raising."""
    proc = _FakeProcWithPid(alive=True, sigterm_exits=True)
    with patch("pg_cyan.shutdown.os.getpgid", side_effect=OSError("no such process")):
        graceful_terminate(proc)
    assert proc.terminated is True
