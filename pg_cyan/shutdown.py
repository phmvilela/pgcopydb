"""
Shared graceful-shutdown helper for the long-lived pgcopydb children.

Both the snapshot holder (``snapshot.py``) and the CDC follow stream
(``follow.py``) run a long-lived ``pgcopydb`` child inside the controller pod and
must stop it cleanly on SIGTERM -- closing the source replication connection /
releasing the slot -- before the kubelet force-kills the pod at
``terminationGracePeriodSeconds`` (60s; see
``deploy/k8s/35-statefulset-controller.yaml``).

Centralising the "SIGTERM, wait the grace window, escalate to SIGKILL, reap"
sequence here keeps the two call sites from drifting: issue #66 fixed the
snapshot side (the 30s SIGKILL timeout undercut the 60s grace); issue #81 found
``follow.close()`` still carried the same unreconciled 30s path and folded it
onto this shared helper.
"""

import subprocess
from typing import Callable, Optional

#: SIGTERM grace window (seconds) -- sized just under the controller pod's
#: ``terminationGracePeriodSeconds: 60`` so the child gets nearly the full window
#: to exit cleanly, while our own SIGKILL + reap still run *before* the kubelet
#: force-kills the pod at 60s (issue #66: the old 30s gave up at the halfway mark).
GRACEFUL_SHUTDOWN_TIMEOUT = 55

#: Seconds to reap the child after SIGKILL. SIGKILL is uninterruptible so the reap
#: returns almost immediately; kept small so ``grace + reap <= 60`` -- the whole
#: teardown always fits inside the pod grace period.
SIGKILL_REAP_TIMEOUT = 5


def graceful_terminate(
    proc: "Optional[subprocess.Popen]",
    *,
    grace_timeout: int = GRACEFUL_SHUTDOWN_TIMEOUT,
    reap_timeout: int = SIGKILL_REAP_TIMEOUT,
    log_event: Optional[Callable[..., None]] = None,
) -> None:
    """SIGTERM ``proc`` and reap it, escalating to SIGKILL if it overruns.

    No-op when ``proc`` is ``None`` or has already exited. The child is sent
    SIGTERM and given ``grace_timeout`` seconds to exit; if it does not, it is
    SIGKILLed and reaped within ``reap_timeout`` seconds (both bounded so the
    total stays inside the pod's termination grace period).

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
        proc.terminate()
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
            proc.kill()
            proc.wait(timeout=reap_timeout)
        except (OSError, subprocess.TimeoutExpired):
            pass
