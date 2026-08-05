"""
Shared graceful-shutdown helper for the long-lived pgcopydb children.

Both the snapshot holder (see ``pg_cyan/snapshot.py``) and the CDC follow stream
(see ``pg_cyan/follow.py``) run a long-lived ``pgcopydb`` child inside the
controller pod and must stop it cleanly on SIGTERM -- closing the source
replication connection / releasing the slot -- before the kubelet force-kills the
pod at ``terminationGracePeriodSeconds`` (60s; see
``deploy/k8s/35-statefulset-controller.yaml``).

Centralising the "SIGTERM, wait the grace window, escalate to SIGKILL, reap"
sequence here keeps the two call sites reconciled with the 60s grace window.
"""

import os
import signal
import subprocess
from typing import Callable, Optional

#: SIGTERM grace window (seconds) -- sized just under the controller pod's
#: ``terminationGracePeriodSeconds: 60`` so the child gets nearly the full window
#: to exit cleanly, while our own SIGKILL + reap still run *before* the kubelet
#: force-kills the pod at 60s.
GRACEFUL_SHUTDOWN_TIMEOUT = 55

#: Seconds to reap the child after SIGKILL. SIGKILL is uninterruptible so the reap
#: returns almost immediately; kept small so ``grace + reap <= 60`` -- the whole
#: teardown always fits inside the pod grace period.
SIGKILL_REAP_TIMEOUT = 5


def _signal_group_or_proc(proc: "subprocess.Popen", sig: int) -> None:
    """Send ``sig`` to ``proc``'s whole OS process group; fall back to just ``proc``.

    ``pgcopydb follow`` forks its ``prefetch``/``catchup`` workers with a plain
    ``fork()`` (no ``setsid``), so they share its process group. Its own
    top-level SIGTERM handling only takes effect *between* blocking waits on
    those workers -- while they're actively streaming it never observes the
    signal, so the workers (which DO each check for SIGTERM in their own
    loops) must be signalled directly. Callers launch the child with
    ``start_new_session=True`` precisely so this group is isolated to the
    child + its forks, not shared with our own process.

    Falls back to signalling just ``proc`` (matches prior behaviour) when the
    child has no real OS pid (e.g. a test double) or the group lookup fails.
    """
    pid = getattr(proc, "pid", None)
    if isinstance(pid, int):
        try:
            os.killpg(os.getpgid(pid), sig)
            return
        except OSError:
            pass
    if sig == signal.SIGKILL:
        proc.kill()
    else:
        proc.terminate()


def graceful_terminate(
    proc: "Optional[subprocess.Popen]",
    *,
    grace_timeout: int = GRACEFUL_SHUTDOWN_TIMEOUT,
    reap_timeout: int = SIGKILL_REAP_TIMEOUT,
    log_event: Optional[Callable[..., None]] = None,
) -> None:
    """SIGTERM ``proc``'s process group and reap it, escalating to SIGKILL if it overruns.

    No-op when ``proc`` is ``None`` or has already exited. The child (and any
    workers it forked into its own process group -- see
    ``_signal_group_or_proc``) is sent SIGTERM and given ``grace_timeout``
    seconds to exit; if it does not, it is SIGKILLed and reaped within
    ``reap_timeout`` seconds (both bounded so the total stays inside the
    pod's termination grace period).

    Args:
        proc: the pgcopydb child, or ``None``.
        grace_timeout: seconds to wait after SIGTERM before escalating.
        reap_timeout: seconds to wait after SIGKILL to reap the child.
        log_event: optional ``(event_type, status, message, details)`` callback
            (matching the stages' ``_log_event``) invoked on SIGKILL escalation.
    """
    if proc is None or proc.poll() is not None:
        return
    try:
        _signal_group_or_proc(proc, signal.SIGTERM)
    except OSError:
        return
    try:
        proc.wait(timeout=grace_timeout)
    except subprocess.TimeoutExpired:
        if log_event is not None:
            log_event(
                "shutdown", "warning",
                "pgcopydb did not exit in time; sending SIGKILL",
                {"timeout": grace_timeout},
            )
        try:
            _signal_group_or_proc(proc, signal.SIGKILL)
            proc.wait(timeout=reap_timeout)
        except (OSError, subprocess.TimeoutExpired):
            pass
