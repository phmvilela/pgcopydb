"""
Per-migration namespace provisioning (issue #87).

`POST /migrations` hands a source+target here; this creates a fresh namespace and
applies the SAME deploy/k8s manifests the integration test uses (RBAC, work-dir
PVC, DB Secret, the controller in ``PGCYAN_WAIT_FOR_START`` mode) so the pod comes
up parked pre-snapshot. It does NOT start the migration.

No drift: the manifests are read from ``deploy/k8s`` (baked into the image),
never re-declared here. Two apply-time transforms mirror what
``integration-tests.yml`` does with ``kubectl -n`` + ``envsubst``:

* **namespace retarget** — the manifests hardcode the fixed ``pgcyan-migration``
  namespace (including RoleBinding subject namespaces), so every ``namespace:``
  value equal to that token is rewritten to the per-migration namespace.
* **placeholder fill** — the ``${PGCYAN_*}`` env placeholders in the controller
  manifest are filled (image, wait-for-start=true, etc.).

Kubernetes I/O goes through an injected ``applier`` so the provisioning logic
(ordering, retarget, fill, secret contents) is unit-testable without a cluster.
"""
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import yaml

#: The fixed namespace baked into deploy/k8s that we retarget per migration.
NAMESPACE_TOKEN = "pgcyan-migration"

#: Manifests applied to stand up a parked migration (order matters: identities +
#: storage + secret before the controller that consumes them).
RBAC_MANIFEST = "01-rbac.yaml"
PVC_MANIFEST = "20-pvc-workdir.yaml"
CONTROLLER_MANIFEST = "35-statefulset-controller.yaml"


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
                 *, follow_cdc: bool = False):
        self.manifests_dir = Path(manifests_dir)
        self.migration_image = migration_image
        self.applier = applier
        self.follow_cdc = follow_cdc

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

        mid = migration_id or uuid.uuid4().hex[:10]
        ns = f"pgcyan-mig-{mid}"
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

        # DB credentials the controller reads via envFrom (never in git).
        self.applier.create_secret(ns, "pgcyan-db-uris", {
            "PGCOPYDB_SOURCE_PGURI": req.source.to_pguri(),
            "PGCOPYDB_TARGET_PGURI": req.target.to_pguri(),
        }, labels={"app.kubernetes.io/part-of": "pgcyan"})

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
            # propagation to the planner Job is completed with start (#89).
            if req.shards and doc.get("kind") == "StatefulSet":
                _inject_env(doc, "PGCYAN_SHARDS", str(req.shards))
            self.applier.apply_doc(doc)

        return {"id": mid, "namespace": ns}
