"""
Migration DAG orchestrator (issue #40).

**Design decision (the ticket's open question): custom Python, not Argo
Workflows.** Rationale:

  * ``01-rbac.yaml`` already scopes a plain ``pgcyan-orchestrator``
    ServiceAccount with Job/Deployment/ConfigMap verbs -- exactly what the
    official ``kubernetes`` Python client needs (in-cluster config + that
    ServiceAccount's token), with zero new CRDs, controllers, or
    cluster-admin bootstrap. Argo Workflows would require installing and
    RBAC-ing its own controller + CRDs (``Workflow``/``WorkflowTemplate``)
    before a single pgcyan Job could run.
  * Every other pgcyan stage (preflight, parameters, metrics, lobs, planner)
    is already a ``PgCyan<Stage>`` class driven by ``pg_cyan.cli``, sharing
    one container image and one ``_log_event`` structured-logging
    convention (see ``CLAUDE.md``). This orchestrator is the same pattern's
    eighth instance rather than a second orchestration paradigm bolted on
    alongside it.
  * The barriers this ticket needs are simple: "wait for N Jobs to reach
    ``Complete``" and "wait for a Deployment to become Ready". The
    Kubernetes client's status APIs are sufficient for that; it does not
    need Argo's retry/artifact/DSL machinery, and operators keep debugging
    with ``kubectl`` against these same ``deploy/k8s/`` files instead of
    learning a second workflow language.
  * Argo remains a fine drop-in later -- the node objects/YAML do not
    change (see ``deploy/k8s/README.md``) -- if the DAG grows real
    conditional/fan-in complexity beyond linear phase barriers.

Implements the phase walk from ``deploy/k8s/README.md`` /
``notes/k8s-sharded-migration-topology.md``::

    planner(-1) -> pre-data(0) -> data shards(2, xN)
    -> index(3a, xM) -> FK(3b)

There is NO CDC phase: under Option A (issue #26) the controller streams CDC
itself after the walk (see ``pg_cyan/controller.py`` / ``pg_cyan/follow.py``).
There is NO snapshot-holder phase: under the folded model (issue #70) the
CONTROLLER that drives this orchestrator (``pg_cyan/controller.py``) holds the
consistent snapshot in its OWN pod and passes the id in via ``snapshot_id``,
which the orchestrator injects as ``SNAPSHOT_ID`` into the ``needs_snapshot``
phases (pre-data pins its schema dump to it; the shards copy it). The controller
also passes its StatefulSet as ``owner_reference``, stamped onto every workload
so deleting the controller cascade-deletes the whole migration, and a
``health_check`` the barrier loop polls so a dropped snapshot aborts the walk.

Each phase is applied from the ``deploy/k8s/`` manifests -- templated phases
(data shards, index shards) are stamped out once per ``plan.json`` entry,
substituting the ``${...}`` placeholders -- and the orchestrator BLOCKS on
that phase's barrier before moving on:

  * Job phases block until every Job in the phase reports ``Complete``
    (or ``Failed``/exceeded ``backoffLimit``).
  * Deployment phases block until the Deployment has its desired ready replica
    count -- a long-running Deployment never "completes", so its barrier means
    *ready*, not *completed*. (No current phase is a Deployment: CDC/follow is
    now streamed in-pod by the controller, #26/#70, not a phase Deployment. The
    handling is retained as generic capability for any future Deployment phase.)

Fails fast: the first phase whose barrier does not resolve to success stops
the walk immediately (``run()`` returns without applying later phases).

ASSUMPTIONS (out of scope for this ticket, tracked in deploy/k8s/README.md):
  * The namespace/RBAC/PVC/Secrets (00/01/10/20) and the input collector +
    planner-inputs ConfigMap (11/25, phase -2) are already applied -- the
    ticket's phase list starts at the planner (-1).
  * ``plan.json`` is read from the ``pgcyan-plan`` ConfigMap the planner Job
    itself publishes (``30-job-planner.yaml``'s ``kubectl apply``), fetched
    via the API *after* phase -1's barrier succeeds -- not mounted as a
    volume, since the ConfigMap does not exist until the planner Job creates
    it.
"""
import json
import logging
import os
import re
import socket
import time
from datetime import datetime
from typing import Dict, List, Optional

import yaml
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException

from pg_cyan.datashard import PgCyanDataShard

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)

_PLACEHOLDER_RE = re.compile(r"\$\{([A-Z0-9_]+)\}")


class OrchestratorError(Exception):
    """Raised for orchestrator-fatal problems (bad manifest, unreadable plan, ...)."""


def _data_shard_substitutions(entry: Dict, plan: Dict) -> Dict[str, str]:
    """Map a ``plan.json > data_shards[i]`` entry onto 60-*.template.yaml placeholders.

    Slot/publication names are plan-LEVEL (one snapshot/slot for the whole run),
    so they come from ``plan`` rather than the per-shard entry; the shard records
    them in its migration-copy-log.json.
    """
    resources = entry.get("resources") or {}
    cpu = str(resources.get("cpu", "1"))
    memory = str(resources.get("memory", "1Gi"))
    return {
        "SHARD_NAME": str(entry["name"]),
        "FILTER_FILE": str(entry["filter_file"]),
        "TABLE_JOBS": str(entry["table_jobs"]),
        "SPLIT_TABLES_LARGER_THAN": str(entry["split_tables_larger_than"]),
        "SLOT_NAME": str((plan.get("slot") or {}).get("name", "")),
        "PUBLICATION_NAME": str((plan.get("publication") or {}).get("name", "")),
        "CPU_REQUEST": cpu,
        "MEM_REQUEST": memory,
        "CPU_LIMIT": cpu,
        "MEM_LIMIT": memory,
    }


def _index_shard_substitutions(entry: Dict, plan: Dict) -> Dict[str, str]:
    """Map a ``plan.json > index_shards[i]`` entry onto 70-*.template.yaml placeholders."""
    return {
        "INDEX_SHARD_NAME": str(entry["name"]),
        "FILTER_FILE": str(entry["filter_file"]),
    }


class PgCyanOrchestrator:
    """Walks the migration DAG, applying each phase and blocking on its barrier."""

    DEFAULT_NAMESPACE = "pgcyan-migration"
    #: deploy/k8s next to the pg_cyan package, both in a repo checkout and in the image.
    DEFAULT_MANIFESTS_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "deploy", "k8s"
    )
    DEFAULT_PLAN_CONFIGMAP = "pgcyan-plan"
    PLAN_KEY = "plan.json"
    #: The combined per-table copy log the orchestrator aggregates from the
    #: per-shard copy-log ConfigMaps and publishes for operators (mirrors
    #: pgcyan-plan / pgcyan-snapshot). Its single key holds the merged
    #: migration-copy-log.json; it is labelled ``copylog=combined`` so the
    #: per-shard selector below never reads it back in.
    COPY_LOG_CONFIGMAP = "pgcyan-copylog"
    COPY_LOG_KEY = PgCyanDataShard.COPY_LOG_CONFIGMAP_KEY
    #: Label selector for the per-shard copy-log ConfigMaps each data shard (#25)
    #: publishes (pgcyan-copylog-<shard>). The orchestrator lists these and merges
    #: them -- the shards run under an API-less-except-configmaps SA and write to
    #: private RWO disks, so the ConfigMap is the durable handoff.
    SHARD_COPYLOG_SELECTOR = PgCyanDataShard.COPY_LOG_LABEL + "=shard"
    DEFAULT_POLL_INTERVAL = 5
    DEFAULT_JOB_TIMEOUT = 3600
    #: Placeholder every manifest carries in place of a concrete image; the
    #: orchestrator fills it with its OWN running image so every phase pod it
    #: launches matches its tag (issue: single image per migration run).
    IMAGE_PLACEHOLDER = "PGCYAN_IMAGE"
    IMAGE_TOKEN = "${" + IMAGE_PLACEHOLDER + "}"
    #: Container names that identify the pgcyan app container in the pod spec.
    IMAGE_CONTAINER_NAMES = ("orchestrator", "pgcyan-orchestrator", "pgcyan")

    #: The DAG, in apply order. Static phases carry "manifest"; templated
    #: phases (stamped out once per plan.json entry) carry "template" +
    #: "plan_key" + "substitutions".
    #: NOTE: there is NO snapshot-holder phase here anymore. Under the folded
    #: model (issue #70) the CONTROLLER that runs this orchestrator holds the
    #: consistent snapshot in its OWN pod and passes the id in via ``snapshot_id``;
    #: the orchestrator injects it as ``SNAPSHOT_ID`` into the phases flagged
    #: ``needs_snapshot`` (pre-data pins its schema dump to it; the data shards
    #: copy it), so no separate holder Deployment and no ``pgcyan-snapshot``
    #: ConfigMap handoff. When ``snapshot_id`` is unset (standalone runs), no
    #: injection happens and the manifests' own env applies.
    PHASES: List[Dict] = [
        {"id": "-1", "name": "planner", "manifest": "30-job-planner.yaml"},
        {"id": "0", "name": "pre-data", "manifest": "40-job-phase0-predata.yaml",
         "needs_snapshot": True},
        {
            "id": "2", "name": "data-shards", "template": "60-job-phase2-datashard.template.yaml",
            "plan_key": "data_shards", "needs_plan": True, "substitutions": _data_shard_substitutions,
            "aggregate_copy_logs": True, "needs_snapshot": True,
        },
        # Sequence sync right after the copy: the COPY-based load leaves target
        # identity/serial sequences at their start, so CDC/post-cutover inserts would
        # collide on id=1 (issue #78). Custom psycopg2 (source pg_sequences ->
        # target setval); no snapshot (sequences are not MVCC).
        {"id": "2s", "name": "sequences", "manifest": "62-job-sequences.yaml"},
        {
            "id": "3a", "name": "index", "template": "70-job-phase3-index.template.yaml",
            "plan_key": "index_shards", "needs_plan": True, "substitutions": _index_shard_substitutions,
            # Phases 3a (index/PK/unique) and 3b (FK) are both pure psycopg2 (#28):
            # they read the source's DDL definitions directly, so -- like the other
            # collectors -- they need no injected snapshot.
        },
        {"id": "3b", "name": "fk", "manifest": "71-job-phase3-fk.yaml"},
        # Phase 4 (CDC) is NOT an orchestrator phase: under Option A (issue #26) the
        # controller itself becomes the CDC consumer after the walk, streaming
        # `pgcopydb follow` on its own --dir (the slot-creator and the streamer must
        # share a --dir; a separate pod cannot reuse the slot). See controller.py.
    ]

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        manifests_dir: str = DEFAULT_MANIFESTS_DIR,
        plan_configmap: str = DEFAULT_PLAN_CONFIGMAP,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        job_timeout: int = DEFAULT_JOB_TIMEOUT,
        in_cluster: bool = True,
        snapshot_id: Optional[str] = None,
        owner_reference: Optional[Dict] = None,
        health_check=None,
        stop_after: Optional[str] = None,
        batch_api=None,
        apps_api=None,
        policy_api=None,
        core_api=None,
    ):
        """
        Initialize the orchestrator.

        Args:
            namespace: Namespace the migration workloads run in.
            manifests_dir: Directory containing the deploy/k8s phase manifests.
            plan_configmap: ConfigMap the planner Job publishes plan.json/shard filters to.
            poll_interval: Seconds between Job/Deployment status polls.
            job_timeout: Seconds to wait for a phase barrier before failing it.
            in_cluster: Load in-cluster ServiceAccount credentials (else local kubeconfig).
            snapshot_id: The consistent snapshot id the controller holds in-pod
                (issue #70). When set, injected as ``SNAPSHOT_ID`` into every
                ``needs_snapshot`` phase pod (replacing any manifest env of that
                name). ``None`` -> no injection (standalone runs).
            owner_reference: A ``V1OwnerReference``-shaped dict (the controller's
                StatefulSet). When set, stamped onto every object this orchestrator
                creates, so deleting the StatefulSet cascade-deletes the whole
                migration (issue #70). ``None`` -> unowned (standalone runs).
            health_check: Optional ``() -> bool`` the barrier loop calls each poll;
                returning ``False`` (the controller's snapshot child died) aborts
                the wait -- a lost snapshot invalidates the in-flight copy.
            stop_after: Phase id (e.g. ``"2"``) to stop after -- the walk applies
                phases through this one (inclusive) and returns success without
                applying any later phase. ``None`` runs the whole DAG. Lets an
                operator bring the pipeline up to a checkpoint (e.g. through the
                data copy) and, under the controller, park Ready there.
            batch_api, apps_api, policy_api, core_api: Inject fake/mock Kubernetes API
                clients for testing; when any is omitted, real clients are built from
                the ambient (in-cluster or kubeconfig) credentials.
        """
        self.namespace = namespace
        self.manifests_dir = manifests_dir
        self.plan_configmap = plan_configmap
        self.poll_interval = poll_interval
        self.job_timeout = job_timeout
        self.snapshot_id = snapshot_id
        self.owner_reference = owner_reference
        self.health_check = health_check
        self.stop_after = stop_after
        #: The image every phase Job/Deployment runs -- the orchestrator's OWN
        #: image, resolved lazily on first use (see _resolve_image).
        self._image: Optional[str] = None

        if None in (batch_api, apps_api, policy_api, core_api):
            self._load_kube_credentials(in_cluster)
        self.batch_api = batch_api or k8s_client.BatchV1Api()
        self.apps_api = apps_api or k8s_client.AppsV1Api()
        self.policy_api = policy_api or k8s_client.PolicyV1Api()
        self.core_api = core_api or k8s_client.CoreV1Api()

    @staticmethod
    def _load_kube_credentials(in_cluster: bool) -> None:
        try:
            if in_cluster:
                k8s_config.load_incluster_config()
            else:
                k8s_config.load_kube_config()
        except Exception as e:  # noqa: BLE001 - config loading raises varied errors across versions
            raise OrchestratorError(f"Could not load Kubernetes credentials: {e}") from e

    def _log_event(self, event_type: str, status: str, message: str, details: Optional[Dict] = None) -> None:
        """Log a structured JSON event (mirrors the other stages)."""
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "orchestrator",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }
        logger.info(json.dumps(event, default=str))

    # -------------------------------------------------------------- own image

    def _resolve_image(self) -> str:
        """
        Resolve the image every phase pod should run: the orchestrator's OWN.

        Honours a ``PGCYAN_IMAGE`` env override (handy for local/dev runs);
        otherwise reads the orchestrator's own pod -- its name is the pod
        hostname in-cluster (``socket.gethostname()``) -- and returns the
        pgcyan app container's image. Reusing the orchestrator's exact image
        keeps a migration run on a single, self-consistent build (the same
        commit-short-SHA tag it was launched with). Cached after the first call.
        """
        if self._image is not None:
            return self._image

        override = os.getenv("PGCYAN_IMAGE")
        if override:
            self._image = override
            return override

        pod_name = socket.gethostname()
        try:
            pod = self.core_api.read_namespaced_pod(name=pod_name, namespace=self.namespace)
        except ApiException as e:
            raise OrchestratorError(
                f"Could not read own pod '{pod_name}' to resolve the image "
                f"(set PGCYAN_IMAGE to override): {e.reason or e}"
            ) from e

        containers = (pod.spec.containers if pod.spec else None) or []
        image = next(
            (c.image for c in containers if c.name in self.IMAGE_CONTAINER_NAMES),
            None,
        )
        if image is None and containers:
            image = containers[0].image
        if not image:
            raise OrchestratorError(
                f"Own pod '{pod_name}' has no resolvable container image "
                "(set PGCYAN_IMAGE to override)"
            )
        self._image = image
        return image

    def _substitute_image(self, value):
        """Non-strict, targeted replace of only the ``${PGCYAN_IMAGE}`` token.

        Unlike the strict ``_substitute`` used for template placeholders, this
        leaves every OTHER ``${...}``/``$(...)`` intact -- static manifests
        carry runtime shell/env references (``${RESTORE_JOBS}``,
        ``$(SNAPSHOT_ID)``) that must survive to the applied object untouched.
        """
        image = self._resolve_image()

        def _walk(node):
            if isinstance(node, str):
                return node.replace(self.IMAGE_TOKEN, image)
            if isinstance(node, list):
                return [_walk(item) for item in node]
            if isinstance(node, dict):
                return {key: _walk(item) for key, item in node.items()}
            return node

        return _walk(value)

    # ------------------------------------------------------------ manifests

    def _read_manifest_file(self, filename: str) -> str:
        path = os.path.join(self.manifests_dir, filename)
        try:
            with open(path, encoding="utf-8") as fh:
                return fh.read()
        except OSError as e:
            raise OrchestratorError(f"Could not read manifest {path}: {e}") from e

    def _render_yaml(self, template_text: str, substitutions: Dict[str, str]) -> List[Dict]:
        """
        Parse ``template_text`` as (multi-document) YAML, then substitute every
        ``${KEY}`` in its string values, returning one dict per document.

        Templated phase files carry MORE than one document -- each per-shard Job
        is preceded by its own ReadWriteOnce work-dir PVC (issue #58) -- so this
        returns a list. Parses BEFORE substituting (rather than regex-replacing
        the raw text) so that YAML comments -- which mention placeholders like
        ``${SNAPSHOT_ID}`` purely as prose -- are already stripped by the parser
        and never seen by the substitution pass.
        """
        try:
            docs = [doc for doc in yaml.safe_load_all(template_text) if doc]
        except yaml.YAMLError as e:
            raise OrchestratorError(f"Manifest template is not valid YAML: {e}") from e
        return [self._substitute(doc, substitutions) for doc in docs]

    def _substitute(self, value, substitutions: Dict[str, str]):
        if isinstance(value, str):
            def _sub(match: "re.Match") -> str:
                key = match.group(1)
                if key not in substitutions:
                    raise OrchestratorError(f"Template references unknown placeholder \"${{{key}}}\"")
                return substitutions[key]
            return _PLACEHOLDER_RE.sub(_sub, value)
        if isinstance(value, list):
            return [self._substitute(item, substitutions) for item in value]
        if isinstance(value, dict):
            return {key: self._substitute(item, substitutions) for key, item in value.items()}
        return value

    def _load_phase_manifests(self, phase: Dict, plan: Optional[Dict]) -> List[Dict]:
        """Return the list of manifest documents (dicts) to apply for ``phase``."""
        if "template" in phase:
            entries = (plan or {}).get(phase["plan_key"]) or []
            if not entries:
                raise OrchestratorError(
                    f"No '{phase['plan_key']}' entries in the plan for phase "
                    f"{phase['id']} ({phase['name']})"
                )
            template_text = self._read_manifest_file(phase["template"])
            image = self._resolve_image()
            docs = []
            for entry in entries:
                subs = dict(phase["substitutions"](entry, plan or {}))
                subs[self.IMAGE_PLACEHOLDER] = image  # strict _substitute fills the image too
                # Templated phase files carry MORE than one document -- a per-shard
                # RWO work-dir PVC precedes each Job (issue #58) -- so extend.
                docs.extend(self._render_yaml(template_text, subs))
            return docs

        text = self._read_manifest_file(phase["manifest"])
        try:
            docs = [doc for doc in yaml.safe_load_all(text) if doc]
        except yaml.YAMLError as e:
            raise OrchestratorError(f"{phase['manifest']} is not valid YAML: {e}") from e
        # Static manifests aren't run through the strict template substitution
        # (they carry runtime ${RESTORE_JOBS}/$(SNAPSHOT_ID) that must survive);
        # fill only the image placeholder here.
        return [self._substitute_image(doc) for doc in docs]

    # ---------------------------------------------------------------- apply

    def _apply(self, doc: Dict) -> None:
        """Create ``doc`` in the cluster; treat "already exists" as idempotent no-op."""
        kind = doc.get("kind")
        metadata = doc.setdefault("metadata", {})
        metadata.setdefault("namespace", self.namespace)
        namespace = metadata["namespace"]
        name = metadata.get("name")

        # Stamp the controller's StatefulSet as owner (issue #70) so that deleting
        # the controller cascade-deletes every workload the migration created --
        # correct semantics now that the snapshot lives in the controller (a lost
        # controller == a lost snapshot == a run that must restart from scratch).
        if self.owner_reference is not None:
            metadata.setdefault("ownerReferences", []).append(self.owner_reference)

        try:
            if kind == "Job":
                self.batch_api.create_namespaced_job(namespace=namespace, body=doc)
            elif kind == "Deployment":
                self.apps_api.create_namespaced_deployment(namespace=namespace, body=doc)
            elif kind == "PodDisruptionBudget":
                self.policy_api.create_namespaced_pod_disruption_budget(namespace=namespace, body=doc)
            elif kind == "PersistentVolumeClaim":
                # Per-shard RWO work-dir disks stamped alongside the data-shard
                # Jobs (issue #58; the index/FK phases are pure psycopg2 and use
                # emptyDir). Applied, not awaited -- binding happens when the shard
                # pod schedules.
                self.core_api.create_namespaced_persistent_volume_claim(namespace=namespace, body=doc)
            else:
                raise OrchestratorError(f"Unsupported manifest kind for orchestrator apply: {kind}")
        except ApiException as e:
            if e.status == 409:
                self._log_event(
                    "apply", "warning", f"{kind}/{name} already exists; treating as already applied",
                    {"kind": kind, "name": name, "namespace": namespace},
                )
                return
            raise OrchestratorError(f"Failed to apply {kind}/{name}: {e.reason or e}") from e
        self._log_event(
            "apply", "success", f"Applied {kind}/{name}",
            {"kind": kind, "name": name, "namespace": namespace},
        )

    # ---------------------------------------------------------------- watch

    @staticmethod
    def _job_outcome(job) -> "tuple[bool, bool, Optional[str]]":
        """Return ``(success, done, message)`` for a ``V1Job``."""
        status = job.status
        for condition in status.conditions or []:
            if condition.type == "Complete" and condition.status == "True":
                return True, True, None
            if condition.type == "Failed" and condition.status == "True":
                return False, True, condition.message or "Job failed"
        if status.succeeded and status.succeeded >= 1:
            return True, True, None
        backoff_limit = job.spec.backoff_limit if job.spec else None
        if status.failed and backoff_limit is not None and status.failed > backoff_limit:
            return False, True, f"Job failed {status.failed} time(s), exceeding backoffLimit={backoff_limit}"
        return False, False, None

    @staticmethod
    def _deployment_outcome(deployment) -> "tuple[bool, bool, Optional[str]]":
        """Return ``(success, done, message)`` for a ``V1Deployment`` -- "done" means ready."""
        status = deployment.status
        for condition in (status.conditions if status else None) or []:
            if condition.type == "ReplicaFailure" and condition.status == "True":
                return False, True, condition.message or "Deployment has a ReplicaFailure condition"
        desired = deployment.spec.replicas if deployment.spec and deployment.spec.replicas is not None else 1
        ready = (status.ready_replicas if status else None) or 0
        if desired > 0 and ready >= desired:
            return True, True, None
        return False, False, None

    def _wait_for(self, kind: str, name: str, timeout: int) -> "tuple[bool, Optional[str]]":
        start = time.monotonic()
        while True:
            # Abort the wait if the controller's in-pod snapshot child has died
            # (issue #70): the consistent point is gone, so anything copying
            # against it is now invalid -- fail fast rather than let the barrier
            # "succeed" against a dead snapshot.
            if self.health_check is not None and not self.health_check():
                return False, (
                    "Snapshot holder died while waiting for "
                    f"{kind}/{name}; the consistent snapshot is lost"
                )
            try:
                if kind == "Job":
                    obj = self.batch_api.read_namespaced_job_status(name=name, namespace=self.namespace)
                    success, done, message = self._job_outcome(obj)
                else:
                    obj = self.apps_api.read_namespaced_deployment_status(name=name, namespace=self.namespace)
                    success, done, message = self._deployment_outcome(obj)
            except ApiException as e:
                return False, f"Could not read status of {kind}/{name}: {e.reason or e}"

            if done:
                return success, message
            if time.monotonic() - start > timeout:
                verb = "complete" if kind == "Job" else "become ready"
                return False, f"Timed out after {timeout}s waiting for {kind}/{name} to {verb}"
            time.sleep(self.poll_interval)

    # ------------------------------------------------------------- plan i/o

    def _read_plan_configmap(self) -> Dict:
        try:
            config_map = self.core_api.read_namespaced_config_map(
                name=self.plan_configmap, namespace=self.namespace
            )
        except ApiException as e:
            raise OrchestratorError(
                f"Could not read plan ConfigMap {self.plan_configmap}: {e.reason or e}"
            ) from e

        raw = (config_map.data or {}).get(self.PLAN_KEY)
        if not raw:
            raise OrchestratorError(
                f"Plan ConfigMap {self.plan_configmap} has no '{self.PLAN_KEY}' key"
            )
        try:
            return json.loads(raw)
        except ValueError as e:
            raise OrchestratorError(
                f"Plan ConfigMap {self.plan_configmap} '{self.PLAN_KEY}' is not valid JSON: {e}"
            ) from e

    # ----------------------------------------------------- copy-log aggregation

    def _read_shard_copy_logs(self) -> List[Dict]:
        """
        Read every per-shard copy-log ConfigMap and return the parsed dicts.

        Each data shard (#25) publishes its ``migration-copy-log.json`` to a
        labelled ``pgcyan-copylog-<shard>`` ConfigMap (the shard's work dir is a
        private RWO disk the orchestrator cannot mount, so the ConfigMap is the
        durable, structured handoff). We list them by label and parse the one key.
        Dedups by shard name. Never raises -- an unreadable/garbage ConfigMap is
        skipped with a warning.
        """
        try:
            cms = self.core_api.list_namespaced_config_map(
                namespace=self.namespace, label_selector=self.SHARD_COPYLOG_SELECTOR
            )
        except ApiException as e:
            self._log_event(
                "aggregate", "warning",
                "Could not list per-shard copy-log ConfigMaps to aggregate",
                {"error": e.reason or str(e)},
            )
            return []

        by_shard: Dict[str, Dict] = {}
        for cm in cms.items or []:
            name = cm.metadata.name if cm.metadata else "<unknown>"
            raw = (cm.data or {}).get(self.COPY_LOG_KEY)
            if not raw:
                self._log_event(
                    "aggregate", "warning",
                    f"Copy-log ConfigMap {name} has no '{self.COPY_LOG_KEY}' key",
                    {"configmap": name},
                )
                continue
            try:
                block = json.loads(raw)
            except ValueError as e:
                self._log_event(
                    "aggregate", "warning", f"Copy-log ConfigMap {name} is not valid JSON",
                    {"configmap": name, "error": str(e)},
                )
                continue
            if not isinstance(block, dict):
                continue
            shard_key = str(block.get("shard") or name)
            by_shard[shard_key] = block
        return list(by_shard.values())

    def _merge_copy_logs(self, shard_logs: List[Dict]) -> Dict:
        """
        Merge the per-shard copy logs into one combined migration-copy-log.json.

        Shard filter sets are disjoint by construction (planner #38), so the
        combined ``tables`` map is their union; ``row_count_total`` sums the
        counted rows. Slot/publication/snapshot are one-per-run, so a shard
        disagreement is surfaced as a warning but does not fail aggregation.
        """
        tables: Dict[str, Optional[int]] = {}
        warnings: List[str] = []
        singletons: Dict[str, set] = {"slot": set(), "publication": set(), "snapshot": set()}
        shard_names: List[str] = []

        for log in shard_logs:
            shard_names.append(str(log.get("shard")))
            for field in singletons:
                value = log.get(field)
                if value:
                    singletons[field].add(str(value))
            for name, count in (log.get("tables") or {}).items():
                if name in tables:
                    warnings.append(f"Table {name} appeared in more than one shard log")
                tables[name] = count

        for field, values in singletons.items():
            if len(values) > 1:
                warnings.append(f"Shards disagree on {field}: {sorted(values)}")

        counted = [v for v in tables.values() if isinstance(v, int)]
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "orchestrator",
            "status": "completed",
            "slot": next(iter(singletons["slot"]), None),
            "publication": next(iter(singletons["publication"]), None),
            "snapshot": next(iter(singletons["snapshot"]), None),
            "shard_count": len(shard_logs),
            "shards": sorted(shard_names),
            "table_count": len(tables),
            "row_count_total": sum(counted) if counted else 0,
            "tables": tables,
            "warnings": warnings,
        }

    def _publish_copy_log_configmap(self, payload: Dict) -> bool:
        """
        Publish the combined copy log into the ``pgcyan-copylog`` ConfigMap.

        Mirrors how the planner/holder publish their artifacts, so operators (and
        the read-only viewer SA) can ``kubectl get configmap pgcyan-copylog``.
        Create-or-replace so a re-run overwrites. Never raises.
        """
        body = k8s_client.V1ConfigMap(
            metadata=k8s_client.V1ObjectMeta(
                name=self.COPY_LOG_CONFIGMAP,
                namespace=self.namespace,
                # 'combined' (not 'shard') so SHARD_COPYLOG_SELECTOR never lists
                # this aggregate back in as if it were a per-shard input.
                labels={"pgcyan.dev/phase": "data",
                        PgCyanDataShard.COPY_LOG_LABEL: "combined"},
            ),
            data={self.COPY_LOG_KEY: json.dumps(payload, indent=2, default=str) + "\n"},
        )
        try:
            self.core_api.create_namespaced_config_map(namespace=self.namespace, body=body)
        except ApiException as e:
            if e.status == 409:
                try:
                    self.core_api.replace_namespaced_config_map(
                        name=self.COPY_LOG_CONFIGMAP, namespace=self.namespace, body=body
                    )
                except ApiException as e2:
                    self._log_event(
                        "aggregate", "warning", "Could not replace the copy-log ConfigMap",
                        {"configmap": self.COPY_LOG_CONFIGMAP, "error": e2.reason or str(e2)},
                    )
                    return False
            else:
                self._log_event(
                    "aggregate", "warning", "Could not publish the copy-log ConfigMap",
                    {"configmap": self.COPY_LOG_CONFIGMAP, "error": e.reason or str(e)},
                )
                return False
        return True

    def _aggregate_copy_logs(self) -> Dict:
        """
        Aggregate the phase-2 shard copy logs and publish the combined result.

        Best-effort by design: the COPY barrier has already passed, so a failure
        to aggregate the *reporting* artifact is a warning, not a migration
        failure. Returns the combined payload (plus a ``published`` flag) so the
        caller can attach it to the phase result and the run summary.
        """
        shard_logs = self._read_shard_copy_logs()
        combined = self._merge_copy_logs(shard_logs)
        if not shard_logs:
            combined["warnings"].append(
                "No shard copy logs found; the combined migration-copy-log.json is empty"
            )
        published = self._publish_copy_log_configmap(combined)
        combined["published"] = published
        self._log_event(
            "aggregate",
            "success" if shard_logs and published else "warning",
            f"Aggregated {combined['shard_count']} shard copy log(s): "
            f"{combined['table_count']} tables, {combined['row_count_total']} rows",
            {"configmap": self.COPY_LOG_CONFIGMAP, "published": published,
             "warnings": combined["warnings"]},
        )
        return combined

    # --------------------------------------------------------------- phases

    def _inject_snapshot_env(self, doc: Dict) -> None:
        """
        Set ``SNAPSHOT_ID`` on every container of ``doc``'s pod template.

        The folded controller (issue #70) holds the snapshot in its own pod and
        knows the id directly, so it injects it here instead of the shards reading
        a ``pgcyan-snapshot`` ConfigMap. Any pre-existing ``SNAPSHOT_ID`` env entry
        (e.g. a leftover ``configMapKeyRef``) is REPLACED, so there is never a
        duplicate. No-op when ``snapshot_id`` is unset.
        """
        if not self.snapshot_id:
            return
        pod_spec = (
            (doc.get("spec") or {}).get("template", {}).get("spec", {})
        )
        for container in pod_spec.get("containers") or []:
            env = [e for e in (container.get("env") or []) if e.get("name") != "SNAPSHOT_ID"]
            env.append({"name": "SNAPSHOT_ID", "value": self.snapshot_id})
            container["env"] = env

    def _apply_phase(self, phase: Dict, plan: Optional[Dict]) -> Dict:
        docs = self._load_phase_manifests(phase, plan)

        targets = []
        for doc in docs:
            if phase.get("needs_snapshot"):
                self._inject_snapshot_env(doc)
            self._apply(doc)
            if doc.get("kind") in ("Job", "Deployment"):
                targets.append((doc["kind"], doc["metadata"]["name"]))

        watched = []
        for kind, name in targets:
            success, message = self._wait_for(kind, name, self.job_timeout)
            watched.append({"kind": kind, "name": name, "success": success, "error": message})

        success = bool(watched) and all(w["success"] for w in watched)
        return {"phase": phase["id"], "name": phase["name"], "success": success, "targets": watched}

    def run(self) -> Dict:
        """
        Apply every phase in order, blocking on each phase's barrier.

        Stops (and returns) at the first phase whose barrier does not resolve
        to success -- later phases are never applied. If ``stop_after`` is set,
        also stops -- successfully -- once that phase clears. Never raises.

        Returns:
            {
              "status": "success" | "failure",
              "namespace": <str>,
              "run_id": <plan run_id, once known> | None,
              "stopped_after": <phase id> | None,
              "phases": [{"phase": id, "name": ..., "success": bool, "targets": [...]}],
              "errors": [...],
            }
        """
        summary = {
            "status": "failure",
            "namespace": self.namespace,
            "run_id": None,
            "stopped_after": None,
            "phases": [],
            "errors": [],
        }

        # Guard a mistyped stop_after up front: silently running the whole DAG when
        # the caller asked to stop early would be a surprising, costly no-op.
        if self.stop_after is not None and self.stop_after not in {p["id"] for p in self.PHASES}:
            self._log_event(
                "run", "failure", f"Unknown stop_after phase id {self.stop_after!r}",
                {"valid_phase_ids": [p["id"] for p in self.PHASES]},
            )
            summary["errors"].append(
                f"Unknown stop_after phase id {self.stop_after!r}; "
                f"valid ids: {[p['id'] for p in self.PHASES]}"
            )
            return summary

        plan: Optional[Dict] = None
        for phase in self.PHASES:
            try:
                if phase.get("needs_plan") and plan is None:
                    plan = self._read_plan_configmap()
                    summary["run_id"] = plan.get("run_id")
                result = self._apply_phase(phase, plan)
            except OrchestratorError as e:
                result = {
                    "phase": phase["id"], "name": phase["name"], "success": False,
                    "targets": [], "error": str(e),
                }

            # Once a phase whose shards emit copy logs has cleared its barrier,
            # aggregate the per-shard logs into the combined migration-copy-log.json
            # (issue #25). Best-effort: never flips a passed barrier to failure.
            if result["success"] and phase.get("aggregate_copy_logs"):
                try:
                    result["copy_log"] = self._aggregate_copy_logs()
                except Exception as e:  # noqa: BLE001 - reporting must not fail the DAG
                    self._log_event(
                        "aggregate", "warning", "Copy-log aggregation raised; continuing",
                        {"error": str(e)},
                    )

            self._log_event(
                "phase",
                "success" if result["success"] else "failure",
                f"Phase {phase['id']} ({phase['name']}) {'succeeded' if result['success'] else 'failed'}",
                result,
            )
            summary["phases"].append(result)

            if not result["success"]:
                summary["errors"].append(f"Phase {phase['id']} ({phase['name']}) failed; DAG stopped")
                return summary

            if self.stop_after is not None and phase["id"] == self.stop_after:
                self._log_event(
                    "run", "success",
                    f"Stopping after phase {phase['id']} ({phase['name']}) as requested",
                    {"stop_after": self.stop_after},
                )
                summary["stopped_after"] = self.stop_after
                summary["status"] = "success"
                return summary

        summary["status"] = "success"
        return summary
