"""
Per-migration namespace provisioning.

``POST /migrations`` hands a source+target here; this creates a fresh namespace
and applies the shared ``deploy/k8s`` manifests (RBAC, work-dir PVC, DB Secret,
the controller in ``PGCYAN_WAIT_FOR_START`` mode) so the pod comes up parked
pre-snapshot. It does NOT start the migration.

The manifests are read from ``deploy/k8s`` (baked into the image), never
re-declared here, so there is no drift. Two apply-time transforms:

* **namespace retarget** — the manifests hardcode the fixed ``pgcyan-migration``
  namespace (including RoleBinding subject namespaces), so every ``namespace:``
  value equal to that token is rewritten to the per-migration namespace.
* **placeholder fill** — the ``${PGCYAN_*}`` env placeholders in the controller
  manifest are filled (image, wait-for-start=true, etc.).

On top of the shared manifests it also stamps ONE extra RoleBinding: the
out-of-cluster, read-only *observer* (the devcontainer's fixed ``pgcyan-viewer``
SA) is bound into each dynamic namespace so an operator watching with that
identity sees the per-migration namespaces the same way it sees the fixed
integration-test one. It is built in code — NOT taken through ``retarget`` — so
its subject keeps pointing at the observer's own namespace instead of being
rewritten to the new one, and it reuses the per-namespace ``pgcyan-viewer`` Role
so the read-only rules stay defined once (deploy/k8s/01-rbac.yaml).

Kubernetes I/O goes through an injected ``applier`` so the provisioning logic
(ordering, retarget, fill, secret contents) is unit-testable without a cluster.

See also: control-plane/api/app/k8s.py.
"""
import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import yaml

#: The fixed namespace baked into deploy/k8s that we retarget per migration.
NAMESPACE_TOKEN = "pgcyan-migration"

#: Per-migration namespace naming, shared with the endpoint so it can reserve the
#: registry row under the same name the provisioner will create.
NAMESPACE_PREFIX = "pgcyan-mig-"

#: Out-of-cluster read-only observer bound into every per-migration namespace.
#: Defaults to the devcontainer's fixed viewer identity (deploy/k8s/01-rbac.yaml,
#: ServiceAccount pgcyan-viewer in namespace pgcyan-migration). Overridable /
#: disable-able from the control-plane env (see app/main.py).
DEFAULT_VIEWER_NAMESPACE = "pgcyan-migration"
DEFAULT_VIEWER_SERVICE_ACCOUNT = "pgcyan-viewer"

#: The per-namespace read-only Role that 01-rbac.yaml stamps (retargeted) into
#: each migration namespace; the external-observer binding reuses it so the
#: read-only rules live in exactly one place.
VIEWER_ROLE_NAME = "pgcyan-viewer"

#: Name of the extra binding, distinct from the in-namespace ``pgcyan-viewer``
#: RoleBinding 01-rbac.yaml already creates for the local viewer SA.
EXTERNAL_VIEWER_BINDING_NAME = "pgcyan-viewer-external"


def new_migration_id() -> str:
    """A short, unique migration id."""
    return uuid.uuid4().hex[:10]


def namespace_for(migration_id: str) -> str:
    """The per-migration namespace for a given migration id."""
    return f"{NAMESPACE_PREFIX}{migration_id}"

#: Manifests applied to stand up a parked migration (order matters: identities +
#: storage + secret before the controller that consumes them).
RBAC_MANIFEST = "01-rbac.yaml"
PVC_MANIFEST = "20-pvc-workdir.yaml"
CONTROLLER_MANIFEST = "35-statefulset-controller.yaml"

#: ConfigMap that carries the operator-declared capacity bundle into the
#: namespace, and its single key. This is the exact declared-file shape the
#: `capacity-inputs` provider reads (pg_cyan/capinputs.py): the collect step
#: mounts it and explodes it into the planner-inputs ConfigMap. Delivering it is
#: this component's job; consuming it in the DAG is a follow-up (like the
#: collectors, it feeds the planner-inputs ConfigMap the same way).
CAPACITY_CONFIGMAP = "pgcyan-capacity-declared"
CAPACITY_DECLARED_KEY = "capacity-declared.json"


class ProvisionError(Exception):
    """Provisioning could not proceed (e.g. missing configuration)."""


def render_placeholders(text: str, subs: Dict[str, str]) -> str:
    """Fill ``${KEY}`` placeholders for the given keys only (envsubst-whitelist).

    Unlisted ``${...}`` / ``$(...)`` tokens are left untouched so runtime refs
    the pods resolve themselves are preserved.
    """
    for key, value in subs.items():
        text = text.replace("${%s}" % key, value)
    return text


def retarget_namespace(obj, new_ns: str, old_ns: str = NAMESPACE_TOKEN) -> None:
    """Recursively rewrite every ``namespace: <old_ns>`` to ``new_ns`` in place.

    Covers ``metadata.namespace`` and nested references (RoleBinding subjects),
    without touching resource names or unrelated strings.
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "namespace" and value == old_ns:
                obj[key] = new_ns
            else:
                retarget_namespace(value, new_ns, old_ns)
    elif isinstance(obj, list):
        for item in obj:
            retarget_namespace(item, new_ns, old_ns)


def _inject_env(statefulset: dict, name: str, value: str) -> None:
    """Append an env var to the controller StatefulSet's first container."""
    container = statefulset["spec"]["template"]["spec"]["containers"][0]
    container.setdefault("env", []).append({"name": name, "value": value})


class Provisioner:
    """Provisions a parked per-migration namespace from the shared manifests."""

    def __init__(self, manifests_dir, migration_image: Optional[str], applier,
                 *, follow_cdc: bool = False,
                 viewer_sa: Optional[str] = DEFAULT_VIEWER_SERVICE_ACCOUNT,
                 viewer_ns: Optional[str] = DEFAULT_VIEWER_NAMESPACE):
        self.manifests_dir = Path(manifests_dir)
        self.migration_image = migration_image
        self.applier = applier
        self.follow_cdc = follow_cdc
        # The out-of-cluster observer to also bind read-only into each namespace.
        # Both parts must be set to enable it; either empty disables the stamp.
        self.viewer_sa = viewer_sa or None
        self.viewer_ns = viewer_ns or None

    def _external_viewer_binding(self, ns: str) -> dict:
        """RoleBinding granting the out-of-cluster observer read-only sight of ns.

        Binds the fixed observer SA (``self.viewer_ns``/``self.viewer_sa``) to the
        per-namespace ``pgcyan-viewer`` Role. Returned as a plain dict and applied
        WITHOUT retarget, so the subject namespace stays the observer's own.
        """
        return {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": EXTERNAL_VIEWER_BINDING_NAME, "namespace": ns},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": VIEWER_ROLE_NAME,
            },
            "subjects": [{
                "kind": "ServiceAccount",
                "name": self.viewer_sa,
                "namespace": self.viewer_ns,
            }],
        }

    def _docs(self, name: str, subs: Optional[Dict[str, str]] = None) -> List[dict]:
        text = (self.manifests_dir / name).read_text()
        if subs:
            text = render_placeholders(text, subs)
        return [d for d in yaml.safe_load_all(text) if d]

    def provision(self, req, migration_id: Optional[str] = None) -> Dict[str, str]:
        """Create the namespace + scaffolding + parked controller; return id+ns."""
        if not self.migration_image:
            raise ProvisionError(
                "control plane has no migration image configured "
                "(set PGCYAN_MIGRATION_IMAGE on the API deployment)")

        mid = migration_id or new_migration_id()
        ns = namespace_for(mid)
        labels = {
            "app.kubernetes.io/part-of": "pgcyan",
            "app.kubernetes.io/component": "migration",
            "pgcyan.dev/migration-id": mid,
        }

        self.applier.create_namespace(ns, labels)

        # Identities + storage first.
        for name in (RBAC_MANIFEST, PVC_MANIFEST):
            for doc in self._docs(name):
                retarget_namespace(doc, ns)
                self.applier.apply_doc(doc)

        # Bind the out-of-cluster read-only observer into this dynamic namespace
        # so it can watch the migration exactly as it watches the fixed
        # integration-test namespace. Stamped directly (never through retarget) so
        # the subject keeps pointing at the observer's own namespace; it reuses the
        # per-namespace pgcyan-viewer Role applied just above.
        if self.viewer_sa and self.viewer_ns:
            self.applier.apply_doc(self._external_viewer_binding(ns))

        # DB credentials the controller reads via envFrom (never in git).
        self.applier.create_secret(ns, "pgcyan-db-uris", {
            "PGCOPYDB_SOURCE_PGURI": req.source.to_pguri(),
            "PGCOPYDB_TARGET_PGURI": req.target.to_pguri(),
        }, labels={"app.kubernetes.io/part-of": "pgcyan"})

        # Operator-declared capacity bundle -> a namespace ConfigMap the planner
        # side consumes (validated at the endpoint against the shared contract).
        capacity_declared = self._deliver_capacity(req, ns, labels)

        # The controller, parked pre-snapshot (wait-to-start). Full walk on start
        # (STOP_AFTER empty); CDC off unless the control plane is configured for it.
        subs = {
            "PGCYAN_IMAGE": self.migration_image,
            "PGCYAN_STOP_AFTER": "",
            "PGCYAN_FOLLOW_CDC": "true" if self.follow_cdc else "",
            "PGCYAN_FOLLOW_VERBOSE": "",
            "PGCYAN_WAIT_FOR_START": "true",
        }
        for doc in self._docs(CONTROLLER_MANIFEST, subs):
            retarget_namespace(doc, ns)
            # Capture the requested shard count on the controller pod. Full
            # propagation to the planner Job is completed with start.
            if req.shards and doc.get("kind") == "StatefulSet":
                _inject_env(doc, "PGCYAN_SHARDS", str(req.shards))
            self.applier.apply_doc(doc)

        return {"id": mid, "namespace": ns, "capacity_declared": capacity_declared}

    def _deliver_capacity(self, req, ns: str, labels: Dict[str, str]) -> List[str]:
        """Stamp the declared capacity bundle into the namespace as a ConfigMap.

        Returns the delivered bundle keys (``[]`` when none were declared, so no
        ConfigMap is created). The bundle is written verbatim under
        ``capacity-declared.json`` -- the shape ``capacity-inputs`` reads.
        """
        capacity = getattr(req, "capacity", None)
        bundle = capacity.to_bundle() if capacity is not None else {}
        if not bundle:
            return []
        self.applier.apply_doc({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": CAPACITY_CONFIGMAP, "namespace": ns, "labels": labels},
            "data": {CAPACITY_DECLARED_KEY: json.dumps(bundle, indent=2)},
        })
        return sorted(bundle)
