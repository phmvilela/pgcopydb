"""
Migration controller -- the folded snapshot-holder + DAG orchestrator (issue #70).

Runs as a **single-pod StatefulSet** (``replicas: 1``, under the
``pgcyan-orchestrator`` SA). One process both:

  1. **HOLDS the consistent snapshot in-pod** -- it drives ``PgCyanSnapshot``
     (``pgcopydb snapshot --follow``) as a supervised child, keeping the
     exporting transaction open for the whole initial copy; and
  2. **WALKS the migration DAG** -- it drives ``PgCyanOrchestrator``, injecting
     the snapshot id straight into the pre-data / data-shard pods (no
     ``pgcyan-snapshot`` ConfigMap handoff) and owning every workload it creates.

WHY FOLD (see #70): a separate holder Deployment can never be owned by a finite
orchestrator (owner-refs would GC it mid-copy), so it leaks -- and a leaked
holder is a **held replication slot = unbounded WAL growth on the source**. The
key realization: **a lost snapshot is already fatal** (a consistent snapshot is
only valid while its transaction is open; lose it and every shard was copying an
instant that no longer exists -> full re-copy). So binding the snapshot's
lifetime to this process, and letting it **own** its children (cascade-delete on
death), just makes the blast radius match a failure that was always total.

COMPOSITION, not merge: ``PgCyanSnapshot`` and ``PgCyanOrchestrator`` stay intact
and separately unit-tested; this controller only wires them together. The
snapshot child is a real subprocess, so a caught exception in the DAG walk cannot
drop the connection -- only a full process/container restart can.

REFUSE-TO-RESUME (issue #70): because any restart loses the in-pod snapshot (a
StatefulSet does not pin the pod -- eviction/OOM/node-drain kill it, and a
same-node container restart kills the subprocess too), a fresh pod must **not**
silently re-snapshot and restart a run that was already underway. On boot the
controller **atomically claims the run** by *creating* the ``pgcyan-migration-state``
ConfigMap; if it already exists (a prior process claimed it), the controller
**refuses** and **parks** -- it flips its readiness probe to not-ready, logs the
halt reason, and blocks (it does NOT crash-loop, since a StatefulSet pod is
``restartPolicy: Always``) until a human clears the leftover state (delete the
ConfigMap + any orphaned workloads) and relaunches. The same park holds the pod
in an observable terminal state after a run finishes.

Ordering: the snapshot is established **first** (planning is offline and fast, so
holding it from the start costs nothing and pins everything to one instant), then
the full walk runs (planner -> pre-data -> shards -> index -> FK -> CDC) with the
id injected. A ``health_check`` the barrier loop polls means a child that dies
mid-walk aborts the copy rather than "succeeding" against a dead snapshot.

Never raises: ``run()`` always tears the snapshot child down (``finally``) so a
graceful pod exit leaves no stale source connection.
"""
import json
import logging
import os
import signal
import socket
import sys
import time
from datetime import datetime
from typing import Callable, Dict, Optional

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException

from pg_cyan.follow import PgCyanFollow
from pg_cyan.orchestrator import PgCyanOrchestrator
from pg_cyan.snapshot import PgCyanSnapshot, _holder_from_env

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)


class PgCyanController:
    """Holds the snapshot in-pod and walks the migration DAG (issue #70)."""

    #: The run-state marker. Created (claimed) atomically on boot; its existence
    #: on a fresh pod means a prior run already began -> refuse-to-resume.
    STATE_CONFIGMAP = "pgcyan-migration-state"
    STATE_KEY = "status"
    #: File the readiness probe checks; present == Ready. Removed/absent == not
    #: ready (parked/halted/failed). Lives on the pod's node-local scratch.
    DEFAULT_READY_FILE = "/pgcopydb/ready"

    def __init__(
        self,
        holder: PgCyanSnapshot,
        namespace: str = PgCyanOrchestrator.DEFAULT_NAMESPACE,
        pod_name: Optional[str] = None,
        in_cluster: bool = True,
        ready_file: str = DEFAULT_READY_FILE,
        stop_after: Optional[str] = None,
        follow_cdc: bool = False,
        core_api=None,
        orchestrator_factory: Optional[Callable[..., PgCyanOrchestrator]] = None,
        follow_factory: Optional[Callable[..., PgCyanFollow]] = None,
    ) -> None:
        """
        Args:
            holder: The snapshot sub-component (``PgCyanSnapshot``). It must NOT be
                configured to publish a ``pgcyan-snapshot`` ConfigMap -- the id is
                injected into pods directly under this model.
            namespace: Namespace the migration workloads run in.
            pod_name: This pod's own name (defaults to the hostname, which is the
                pod name in-cluster) -- used to resolve the owning StatefulSet.
            in_cluster: Load in-cluster credentials (else local kubeconfig).
            ready_file: Path the readiness probe tests for; written when the
                controller is healthy, absent when parked/failed.
            follow_cdc: After a full successful walk, become the CDC consumer --
                release the snapshot then stream ``pgcopydb follow`` on this pod's
                own ``--dir`` (Option A, issue #26). ``False`` -> park after the
                walk (no CDC). Skipped when ``stop_after`` halts before post-data.
            core_api: Injectable CoreV1Api (own pod + state ConfigMap); built if omitted.
            orchestrator_factory: Injectable builder ``(**kwargs) -> orchestrator``
                for tests; defaults to constructing a real ``PgCyanOrchestrator``.
            follow_factory: Injectable builder ``(**kwargs) -> PgCyanFollow`` for the
                CDC consumer; defaults to building from the holder + env.
        """
        self.holder = holder
        self.namespace = namespace
        self.pod_name = pod_name or socket.gethostname()
        self.in_cluster = in_cluster
        self.ready_file = ready_file
        self.stop_after = stop_after
        self.follow_cdc = follow_cdc
        self.follow_factory = follow_factory or self._default_follow
        self.cdc: Optional[PgCyanFollow] = None       # the streaming CDC child, once started
        self._released = False       # snapshot deliberately released ahead of CDC (#26)
        self.errors = []

        if core_api is None:
            self._load_kube_credentials(in_cluster)
        self.core_api = core_api or k8s_client.CoreV1Api()
        self.orchestrator_factory = orchestrator_factory or self._default_orchestrator

    @staticmethod
    def _load_kube_credentials(in_cluster: bool) -> None:
        try:
            if in_cluster:
                k8s_config.load_incluster_config()
            else:
                k8s_config.load_kube_config()
        except Exception as e:  # noqa: BLE001 - config loading raises varied errors
            logger.info(json.dumps({"component": "controller", "event_type": "kube_config",
                                    "status": "warning", "message": str(e)}))

    def _default_orchestrator(self, **kwargs) -> PgCyanOrchestrator:
        return PgCyanOrchestrator(namespace=self.namespace, in_cluster=self.in_cluster, **kwargs)

    def _default_follow(self, **kwargs) -> PgCyanFollow:
        """Build the CDC consumer on THIS pod's --dir (so follow reuses the slot).

        The work dir mirrors the holder's ``--dir`` (``pgcopydb snapshot --follow``
        populated its ``source.db`` with the slot record), and the readiness file is
        shared with the controller so the probe reflects CDC health.
        """
        return PgCyanFollow(
            source_pguri=os.getenv("PGCOPYDB_SOURCE_PGURI") or None,
            target_pguri=os.getenv("PGCOPYDB_TARGET_PGURI") or None,
            work_dir=os.getenv("PGCYAN_WORK_DIR", PgCyanFollow.DEFAULT_WORK_DIR),
            ready_file=self.ready_file,
            # Optional CDC debug knob: run `pgcopydb follow --verbose` (logs each
            # change) when PGCYAN_FOLLOW_VERBOSE is set. Off by default.
            verbose=os.getenv("PGCYAN_FOLLOW_VERBOSE", "").strip().lower() in ("1", "true", "yes"),
            **kwargs,
        )

    def _log_event(self, event_type: str, status: str, message: str,
                   details: Optional[Dict] = None) -> None:
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "controller",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }
        logger.info(json.dumps(event, default=str))

    # -------------------------------------------------------- owner reference

    def resolve_owner_reference(self) -> Optional[Dict]:
        """
        Return a ``V1OwnerReference``-shaped dict for the owning StatefulSet.

        The controller pod is created by its StatefulSet, so the StatefulSet is in
        this pod's ``ownerReferences``. Stamping it onto every workload the
        orchestrator creates means ``kubectl delete statefulset pgcyan-controller``
        cascade-deletes the whole migration (issue #70). Returns ``None`` (unowned)
        if the pod or its StatefulSet owner cannot be resolved (e.g. a local run).
        """
        try:
            pod = self.core_api.read_namespaced_pod(name=self.pod_name, namespace=self.namespace)
        except ApiException as e:
            self._log_event(
                "owner_reference", "warning",
                f"Could not read own pod {self.pod_name!r} to resolve the owner",
                {"error": e.reason or str(e)},
            )
            return None

        owners = (pod.metadata.owner_references if pod.metadata else None) or []
        for ref in owners:
            if ref.kind == "StatefulSet":
                return {
                    "apiVersion": ref.api_version,
                    "kind": ref.kind,
                    "name": ref.name,
                    "uid": ref.uid,
                    "controller": True,
                    "blockOwnerDeletion": True,
                }
        self._log_event(
            "owner_reference", "warning",
            "No StatefulSet owner on this pod; workloads will be unowned (no cascade)",
            {"pod": self.pod_name},
        )
        return None

    def _snapshot_alive(self) -> bool:
        """``health_check`` for the orchestrator: is the snapshot child still up?

        ``True`` when the child is running -- or when there is no child at all
        (a ``--not-consistent`` run holds nothing), so we never spuriously abort.
        Also ``True`` once we have DELIBERATELY released the snapshot before CDC
        (issue #26): from that point its absence is by design, not a failure.
        """
        if self._released:
            return True
        proc = self.holder.proc
        return proc is None or proc.poll() is None

    @staticmethod
    def _walk_reached_postdata(result: Dict) -> bool:
        """Did the walk complete through post-data (the last phase) -- i.e. is the
        target ready for CDC to apply against? True when it ran the whole DAG, or
        stopped after the last phase (``PgCyanOrchestrator.PHASES[-1]``)."""
        last_phase = PgCyanOrchestrator.PHASES[-1]["id"]
        return result.get("stopped_after") in (None, last_phase)

    def _start_cdc(self, snapshot_id: Optional[str], summary: Dict) -> bool:
        """
        Release the held snapshot, then start streaming CDC on this pod's --dir.
        Returns whether the controller is Ready (CDC attached). Updates ``summary``.
        """
        # The held snapshot keeps the slot acquired; release it so `follow` can
        # attach. The slot is persistent + the --dir catalog survives, so follow
        # --resume reuses the slot instead of re-creating it.
        self._release_snapshot()
        self.cdc = self.follow_factory(slot_name=self.holder.slot_name,
                                       plugin=self.holder.plugin)
        cdc = self.cdc.start()
        if cdc.get("status") == "success":
            summary["cdc"] = "streaming"
            self._log_event("cdc", "success", "CDC streaming; controller is the consumer",
                            {"slot": self.holder.slot_name})
            return True
        summary["status"] = "failure"
        summary["cdc"] = "failed"
        summary["errors"].extend(cdc.get("errors", []))
        self._log_event("cdc", "failure", "CDC failed to start after the walk", {})
        return False

    def _release_snapshot(self) -> None:
        """
        Close the held snapshot so its slot frees up for CDC (issue #26).

        The controller's ``pgcopydb snapshot --follow`` child keeps the slot
        acquired while its exported-snapshot connection is open, so a second
        consumer (``pgcopydb follow``) cannot attach. The snapshot's last user was
        the phase-2 copy; the slot is persistent (WAL retained), so closing here
        loses nothing. Idempotent -- the orchestrator calls it once, before phase 4.
        """
        if self._released:
            return
        self._log_event("release_snapshot", "success",
                        "Closing the held snapshot ahead of CDC", {})
        self.holder.close()
        self._released = True

    # ----------------------------------------------------------- run-state

    def claim_run(self) -> bool:
        """
        Atomically claim the run by CREATING the state ConfigMap.

        Returns ``True`` if this process claimed it (fresh run). Returns ``False``
        if it already exists -- a prior process began this run and (since the
        in-pod snapshot cannot survive a restart) we must refuse to resume. A
        non-409 API error is logged and treated as claimed (degraded: the
        StatefulSet's at-most-one already prevents a concurrent second holder;
        the refuse-on-restart guard is best-effort).
        """
        body = k8s_client.V1ConfigMap(
            metadata=k8s_client.V1ObjectMeta(
                name=self.STATE_CONFIGMAP, namespace=self.namespace,
                labels={"pgcyan.dev/component": "controller"},
            ),
            data={self.STATE_KEY: "running",
                  "claimed_by": self.pod_name,
                  "claimed_at": datetime.utcnow().isoformat() + "Z"},
        )
        try:
            self.core_api.create_namespaced_config_map(namespace=self.namespace, body=body)
            return True
        except ApiException as e:
            if e.status == 409:
                return False
            self._log_event(
                "claim_run", "warning",
                "Could not create the run-state ConfigMap; proceeding (degraded)",
                {"error": e.reason or str(e)},
            )
            return True

    def _update_run_status(self, status: str, snapshot: Optional[str] = None) -> None:
        """Best-effort patch of the state ConfigMap's status (never raises)."""
        data = {self.STATE_KEY: status,
                "updated_at": datetime.utcnow().isoformat() + "Z"}
        if snapshot:
            data["snapshot"] = snapshot
        try:
            self.core_api.patch_namespaced_config_map(
                name=self.STATE_CONFIGMAP, namespace=self.namespace,
                body={"data": data},
            )
        except ApiException as e:
            self._log_event("run_state", "warning",
                            f"Could not update run-state to {status!r}",
                            {"error": e.reason or str(e)})

    def _read_run_status(self) -> Optional[str]:
        """Return the existing run-state status, or ``None`` if unreadable."""
        try:
            cm = self.core_api.read_namespaced_config_map(
                name=self.STATE_CONFIGMAP, namespace=self.namespace)
            return (cm.data or {}).get(self.STATE_KEY)
        except ApiException:
            return None

    # ------------------------------------------------------------ readiness

    def _set_ready(self, ready: bool) -> None:
        """Reflect readiness via the probe file: present == Ready."""
        try:
            if ready:
                os.makedirs(os.path.dirname(self.ready_file) or ".", exist_ok=True)
                with open(self.ready_file, "w", encoding="utf-8") as fh:
                    fh.write("ready\n")
            elif os.path.exists(self.ready_file):
                os.remove(self.ready_file)
        except OSError as e:
            self._log_event("readiness", "warning",
                            f"Could not set readiness file to {ready}", {"error": str(e)})

    def park(self) -> None:
        """
        Block until SIGTERM/SIGINT, holding the pod in its current (readiness)
        state. This is the "refuse-to-resume" / terminal park: the pod stays up
        and observable (Ready iff the migration is healthy) rather than exiting
        and letting the StatefulSet crash-loop it. A signal tears it down cleanly.
        """
        # If CDC is streaming (Option A, #26), supervise the follow child: block
        # while it streams, and if it exits on its own, drop readiness and stay
        # parked not-ready (observable, no crash-loop) rather than exiting.
        if self.cdc is not None:
            self._log_event("park", "success", "Streaming CDC; waiting for SIGTERM")
            try:
                rc = self.cdc.supervise(stop=lambda: False)
                self._set_ready(False)
                self._log_event("park", "warning" if rc else "success",
                                f"CDC stream ended (rc={rc}); parking not-ready")
            except KeyboardInterrupt:
                self.cdc.close()
                self._set_ready(False)
                self._log_event("park", "success", "Signalled; stopped CDC and exiting park")
                return

        self._log_event("park", "success", "Parked; waiting for SIGTERM (kubectl delete to release)")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            self._log_event("park", "success", "Signalled; exiting park")

    # --------------------------------------------------------------- run

    def run(self) -> Dict:
        """
        Claim the run, establish the snapshot, walk the DAG with it injected +
        owned, then release the snapshot. Does NOT block (the caller parks). Never
        raises. Returns:
            {
              "status": "success" | "failure" | "halted",
              "ready": <bool>,          # what the readiness probe now reflects
              "snapshot": <id or None>,
              "owned": <bool>,
              "phases": [...],
              "errors": [...],
            }
        ``halted`` == refused to resume a run a prior pod already began.
        """
        summary = {"status": "failure", "ready": False, "snapshot": None,
                   "owned": False, "phases": [], "errors": self.errors}

        # SIGTERM/SIGINT -> tear the child down and stop. Raising KeyboardInterrupt
        # routes both the walk and the park() below through clean shutdown.
        def _on_signal(signum, _frame):
            self._log_event("signal", "success", f"Received signal {signum}; shutting down")
            raise KeyboardInterrupt()
        try:
            signal.signal(signal.SIGTERM, _on_signal)
            signal.signal(signal.SIGINT, _on_signal)
        except ValueError:
            pass  # not on the main thread (e.g. under test) -- skip signal setup

        # REFUSE-TO-RESUME: a pre-existing run-state marker means a prior pod
        # already began this run; its in-pod snapshot is gone and we must not
        # silently re-snapshot. Park not-ready for a human to clear the state.
        if not self.claim_run():
            prior = self._read_run_status()
            self._set_ready(False)
            self.errors.append(
                f"Refused to resume: a prior run (state={prior!r}) already claimed "
                f"{self.STATE_CONFIGMAP}. The in-pod snapshot cannot survive a "
                f"restart -- delete the {self.STATE_CONFIGMAP} ConfigMap (and any "
                f"orphaned workloads) to start a fresh migration."
            )
            self._log_event("refuse_resume", "failure",
                            "Refusing to resume a prior run; parking not-ready",
                            {"prior_status": prior, "state_configmap": self.STATE_CONFIGMAP})
            summary["status"] = "halted"
            return summary

        try:
            # 1) Establish + HOLD the snapshot in-pod (child stays alive).
            snap = self.holder.establish()
            if snap.get("status") != "success":
                self.errors.append("Snapshot establishment failed; migration not started")
                self._update_run_status("failed")
                self._set_ready(False)
                self._log_event("establish", "failure",
                                "Could not establish the consistent snapshot", snap)
                return summary
            snapshot_id = snap.get("snapshot_name")
            summary["snapshot"] = snapshot_id
            self._update_run_status("running", snapshot=snapshot_id)
            self._log_event("establish", "success",
                            "Consistent snapshot established and held in-pod",
                            {"snapshot": snapshot_id})

            # 2) Resolve the owning StatefulSet for the cascade.
            owner_ref = self.resolve_owner_reference()
            summary["owned"] = owner_ref is not None

            # Ready: the migration is underway. The probe passes while the walk
            # progresses (and stays passing on success).
            self._set_ready(True)

            # 3) Walk the DAG with the snapshot injected + workloads owned, and a
            #    health check so a dropped snapshot aborts the copy.
            orch = self.orchestrator_factory(
                snapshot_id=snapshot_id,
                owner_reference=owner_ref,
                health_check=self._snapshot_alive,
                stop_after=self.stop_after,
            )
            result = orch.run()
            summary["phases"] = result.get("phases", [])
            summary["errors"].extend(result.get("errors", []))
            summary["status"] = result.get("status", "failure")
            walk_ok = summary["status"] == "success"

            # 4) CDC (Option A, issue #26): after a FULL successful walk, become the
            #    CDC consumer -- release the snapshot (freeing the slot) then stream
            #    `pgcopydb follow` on THIS pod's --dir, where the slot record lives
            #    so follow reuses it. Skipped when stopped before post-data.
            if walk_ok and self.follow_cdc and self._walk_reached_postdata(result):
                summary["ready"] = self._start_cdc(snapshot_id, summary)
            else:
                summary["ready"] = walk_ok

            self._update_run_status("succeeded" if summary["ready"] else "failed",
                                    snapshot=snapshot_id)
            self._set_ready(summary["ready"])
            self._log_event(
                "walk", "success" if summary["status"] == "success" else "failure",
                f"Migration DAG walk finished: {summary['status']}",
                {"snapshot": snapshot_id, "owned": summary["owned"], "cdc": summary.get("cdc")},
            )
            return summary
        except KeyboardInterrupt:
            self.errors.append("Interrupted; snapshot released and migration stopped")
            self._set_ready(False)
            if self.cdc is not None:
                self.cdc.close()
            return summary
        finally:
            # Always release the snapshot child so no stale source connection /
            # held slot is left behind.
            try:
                self.holder.close()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass


def run_migration_from_env() -> int:
    """Entry point for the StatefulSet: build from env, run, then park."""
    holder = _holder_from_env()
    controller = PgCyanController(
        holder=holder,
        namespace=os.getenv("PGCYAN_NAMESPACE", PgCyanOrchestrator.DEFAULT_NAMESPACE),
        in_cluster=os.getenv("PGCYAN_IN_CLUSTER", "true").strip().lower() not in ("0", "false", "no"),
        ready_file=os.getenv("PGCYAN_READY_FILE", PgCyanController.DEFAULT_READY_FILE),
        stop_after=os.getenv("PGCYAN_STOP_AFTER") or None,
        follow_cdc=os.getenv("PGCYAN_FOLLOW_CDC", "").strip().lower() in ("1", "true", "yes"),
    )
    summary = controller.run()
    print(json.dumps(summary, indent=2, default=str))
    # Park in the terminal state (Ready iff the migration succeeded) rather than
    # exiting -- a StatefulSet pod would otherwise be restarted and crash-loop.
    controller.park()
    return 0 if summary["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(run_migration_from_env())
