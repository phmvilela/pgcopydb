"""
Tester-driven teardown of a migration (issue #90).

``POST /migrations/{id}/done`` ends a migration cleanly:

1. **Graceful CDC stop** -- scale the controller StatefulSet to 0 replicas. That
   is a real SIGTERM to the controller pod (with the pod's grace period) which,
   unlike deleting the pod, does *not* respawn it. The controller catches SIGTERM
   in ``park()`` and stops ``pgcopydb follow`` cleanly (no SIGKILL -- the window
   is reconciled with the grace period in #81/#83); the slot's consumer detaches.
2. **Wait** for the controller pod to terminate -- the observable proxy for "CDC
   stopped and the consumer detached" (the control plane holds no source creds to
   probe the slot directly; the slot itself persists with 0 consumers for cutover
   #27, asserted cluster-side by #83).
3. **Delete the namespace** (cascade) -- removes the Secret, PVC, Service, RBAC,
   and the now-idle StatefulSet.

The caller then marks the registry entry ``done``, freeing the singleton (#88).

Kubernetes I/O is injected (same pattern as the provisioner) so the ordering and
wait logic are unit-testable without a cluster.
"""
import time
from typing import Dict

#: The controller StatefulSet + the label its pods carry (deploy/k8s).
CONTROLLER_STATEFULSET = "pgcyan-controller"
CONTROLLER_POD_SELECTOR = "pgcyan.dev/component=controller"


class TeardownError(Exception):
    """Teardown could not complete (a Kubernetes operation failed)."""


class Teardown:
    """Scales the controller down, waits for it to stop, deletes the namespace."""

    def __init__(self, applier, *, grace_timeout: float = 90.0,
                 poll_interval: float = 2.0, sleep=time.sleep):
        # grace_timeout > the pod's 60s terminationGracePeriodSeconds, so a clean
        # SIGTERM shutdown has room to finish before we cascade-delete.
        self.applier = applier
        self.grace_timeout = grace_timeout
        self.poll_interval = poll_interval
        self._sleep = sleep

    def teardown(self, namespace: str) -> Dict:
        """Run the teardown; return ``{graceful, namespace}``. Raises TeardownError."""
        try:
            # 1. Graceful CDC stop (SIGTERM via scale-to-0, no respawn).
            self.applier.scale_statefulset(namespace, CONTROLLER_STATEFULSET, 0)
            # 2. Wait for the controller pod to terminate cleanly.
            graceful = self._await_controller_gone(namespace)
            # 3. Cascade-delete the namespace (removes everything else too).
            self.applier.delete_namespace(namespace)
        except TeardownError:
            raise
        except Exception as exc:  # noqa: BLE001 -- any k8s failure is a teardown failure
            raise TeardownError(f"teardown of {namespace} failed: {exc}") from exc
        return {"namespace": namespace, "graceful": graceful}

    def _await_controller_gone(self, namespace: str) -> bool:
        """Poll until no controller pod remains, or the grace timeout elapses.

        Returns ``True`` if the pod terminated within the window (clean stop),
        ``False`` if it was still present -- teardown proceeds either way (the
        namespace delete finishes termination), but the flag is surfaced.
        """
        waited = 0.0
        while True:
            if self.applier.count_pods(namespace, CONTROLLER_POD_SELECTOR) == 0:
                return True
            if waited >= self.grace_timeout:
                return False
            self._sleep(self.poll_interval)
            waited += self.poll_interval
