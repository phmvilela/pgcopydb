"""
Tester-driven teardown of a migration.

``POST /migrations/{id}/done`` ends a migration cleanly:

1. **Release the source** -- call the controller's ``POST /release`` (while it is
   still alive to serve it) so it drops the replication slot + publications it
   created. This is the one piece of state the k8s cascade cannot reclaim -- an
   orphaned slot pins WAL forever -- and the control plane holds no source creds
   to drop it directly, so the controller (which does) does it. Best-effort: a
   crashed/unreachable controller cannot release, and teardown proceeds anyway
   (the leaked slot is left for the reaper backstop), surfacing ``released``.
2. **Graceful CDC stop** -- scale the controller StatefulSet to 0 replicas. That
   is a real SIGTERM to the controller pod (with the pod's grace period) which,
   unlike deleting the pod, does *not* respawn it. The controller catches SIGTERM
   in ``park()`` and stops ``pgcopydb follow`` cleanly (the grace timeout leaves
   room to avoid SIGKILL).
3. **Wait** for the controller pod to terminate cleanly.
4. **Delete the namespace** (cascade) -- removes the Secret, PVC, Service, RBAC,
   and the now-idle StatefulSet.

The caller then marks the registry entry ``done``, freeing the singleton.

Kubernetes + controller I/O are injected (same pattern as the provisioner) so the
ordering and wait logic are unit-testable without a cluster.

See also: control-plane/api/app/k8s.py, control-plane/api/app/controller_client.py.
"""
import time
from typing import Dict

from app.controller_client import ControllerUnreachable

#: The controller StatefulSet + the label its pods carry (deploy/k8s).
CONTROLLER_STATEFULSET = "pgcyan-controller"
CONTROLLER_POD_SELECTOR = "pgcyan.dev/component=controller"


class TeardownError(Exception):
    """Teardown could not complete (a Kubernetes operation failed)."""


class Teardown:
    """Scales the controller down, waits for it to stop, deletes the namespace."""

    def __init__(self, applier, controller_client, *, grace_timeout: float = 90.0,
                 poll_interval: float = 2.0, sleep=time.sleep):
        # grace_timeout > the pod's 60s terminationGracePeriodSeconds, so a clean
        # SIGTERM shutdown has room to finish before we cascade-delete.
        self.applier = applier
        self.controller_client = controller_client
        self.grace_timeout = grace_timeout
        self.poll_interval = poll_interval
        self._sleep = sleep

    def teardown(self, namespace: str) -> Dict:
        """Run the teardown; return ``{released, graceful, namespace}``. Raises TeardownError."""
        try:
            # 1. Release the source (drop the replication slot + publications)
            #    while the controller is still alive to serve /release -- the k8s
            #    cascade cannot reach source-side state, and an orphaned slot pins
            #    WAL forever. Best-effort: a crashed/unreachable controller cannot
            #    release, but the namespace delete must still proceed.
            released = self._release_source(namespace)
            # 2. Graceful CDC stop (SIGTERM via scale-to-0, no respawn).
            self.applier.scale_statefulset(namespace, CONTROLLER_STATEFULSET, 0)
            # 3. Wait for the controller pod to terminate cleanly.
            graceful = self._await_controller_gone(namespace)
            # 4. Cascade-delete the namespace (removes everything else too).
            self.applier.delete_namespace(namespace)
        except TeardownError:
            raise
        except Exception as exc:  # noqa: BLE001 -- any k8s failure is a teardown failure
            raise TeardownError(f"teardown of {namespace} failed: {exc}") from exc
        return {"namespace": namespace, "graceful": graceful, "released": released}

    def _release_source(self, namespace: str) -> bool:
        """Ask the controller to drop the source slot + publications (best-effort).

        Returns ``True`` iff the controller reported a successful release. A
        controller that is unreachable (crashed before teardown) or that returns a
        non-2xx is swallowed -- the namespace delete still runs, and the leaked
        slot is left for the reaper backstop rather than blocking teardown.
        """
        try:
            code, body = self.controller_client.release(namespace)
        except ControllerUnreachable:
            return False
        except Exception:  # noqa: BLE001 -- release is best-effort; never block the delete
            return False
        return code == 200 and bool(body.get("released"))

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
