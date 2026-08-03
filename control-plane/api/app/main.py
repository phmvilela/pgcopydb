"""
pgcyan control-plane API (issues #86, #87, #88).

The control plane is a single, long-lived FastAPI service in the fixed
``pgcyan-system`` namespace. It provisions a per-migration namespace with the
full migration scaffolding (RBAC, work-dir PVC, DB Secret, the controller in
wait-to-start mode) and will (later tickets) drive start/status/teardown. A
node-local SQLite registry on a ReadWriteOnce volume tracks migrations and
enforces one active control-plane migration at a time.

Endpoints so far:
* ``GET /health``      -- liveness/readiness.
* ``POST /migrations`` -- provision a parked per-migration namespace (#87),
  gated by the singleton + source-clash rules (#88).
* ``GET /migrations``  -- list tracked migrations from the registry (#88).
* ``POST /migrations/{id}/start``  -- proxy the controller's ``POST /start`` (#89).
* ``GET /migrations/{id}/status``  -- aggregate the controller's ``/status`` with
  the registry state (#89).

``PGCYAN_DATA_DIR`` (default ``/data``) is the mounted RWO volume that holds the
SQLite registry; ``PGCYAN_MANIFESTS_DIR`` (default ``/app/deploy/k8s``) is where
the shared migration manifests are baked; ``PGCYAN_MIGRATION_IMAGE`` is the
pgcyan image the provisioned controller runs.
"""
import os
from datetime import datetime, timezone
from typing import List

from fastapi import Depends, FastAPI, HTTPException

from app.controller_client import ControllerClient, ControllerUnreachable
from app.models import (
    MigrationCreated,
    MigrationRecord,
    MigrationRequest,
    MigrationStarted,
    MigrationStatus,
)
from app.provisioner import (
    Provisioner,
    ProvisionError,
    namespace_for,
    new_migration_id,
)
from app.registry import Registry
from app.source_probe import SourceProbe

#: Controller lifecycle states that are terminal (mirrors the registry's).
TERMINAL_STATES = ("succeeded", "failed", "halted")


def _registry_state_for(controller_state: str) -> str:
    """Map a controller lifecycle state onto the registry's vocabulary.

    The two vocabularies match, except the controller's transient ``starting`` /
    ``initialized`` collapse onto the registry's active states.
    """
    if controller_state in TERMINAL_STATES:
        return controller_state
    if controller_state in ("running", "starting"):
        return "running"
    return "waiting-to-start"

DATA_DIR = os.getenv("PGCYAN_DATA_DIR", "/data")
MANIFESTS_DIR = os.getenv("PGCYAN_MANIFESTS_DIR", "/app/deploy/k8s")
REGISTRY_PATH = os.path.join(DATA_DIR, "registry.db")

app = FastAPI(title="pgcyan control-plane API", version="0.1.0")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_provisioner() -> Provisioner:
    """Build a cluster-backed provisioner. Overridden in tests via
    ``app.dependency_overrides`` so no in-cluster config is needed there."""
    from app.k8s import K8sApplier

    follow_cdc = os.getenv("PGCYAN_FOLLOW_CDC", "").strip().lower() in ("1", "true", "yes")
    return Provisioner(
        MANIFESTS_DIR,
        os.getenv("PGCYAN_MIGRATION_IMAGE"),
        K8sApplier(),
        follow_cdc=follow_cdc,
    )


def get_registry() -> Registry:
    """The node-local SQLite registry on the RWO PVC. Overridden in tests."""
    return Registry(REGISTRY_PATH)


def get_source_probe() -> SourceProbe:
    """Best-effort source clash detector. Overridden in tests."""
    return SourceProbe()


def get_controller_client() -> ControllerClient:
    """HTTP client for a migration's in-namespace controller. Overridden in tests."""
    return ControllerClient()


@app.get("/health")
def health() -> dict:
    """Liveness/readiness probe target (see #86)."""
    return {
        "status": "ok",
        "service": "pgcyan-control-plane",
        "data_dir": DATA_DIR,
        "data_dir_writable": os.path.isdir(DATA_DIR) and os.access(DATA_DIR, os.W_OK),
    }


@app.get("/migrations", response_model=List[MigrationRecord])
def list_migrations(registry: Registry = Depends(get_registry)) -> List[MigrationRecord]:
    """List tracked migrations (newest first), credentials never included."""
    return [MigrationRecord(**row) for row in registry.list()]


@app.post("/migrations", status_code=201, response_model=MigrationCreated)
def create_migration(
    req: MigrationRequest,
    provisioner: Provisioner = Depends(get_provisioner),
    registry: Registry = Depends(get_registry),
    probe: SourceProbe = Depends(get_source_probe),
) -> MigrationCreated:
    """Provision a fresh per-migration namespace with the controller parked
    pre-snapshot (wait-to-start), after enforcing the singleton + clash rules.

    Does NOT start the migration (start is a later ticket).
    """
    mid = new_migration_id()
    namespace = namespace_for(mid)

    # Singleton: claim the slot before doing any expensive work. The registry's
    # partial unique index lets the database -- not a check-then-act here -- pick
    # the winner of concurrent requests; the loser gets no row back.
    if not registry.reserve(mid, namespace, req.source.endpoint(),
                            req.target.endpoint(), _now()):
        active = registry.active()
        held = active[0] if active else None
        detail = (
            f"a migration is already active (id={held['id']}, "
            f"namespace={held['namespace']}, state={held['state']}); tear it down "
            "before starting another"
        ) if held else "could not reserve a migration slot"
        raise HTTPException(status_code=409, detail=detail)

    # Clash: a pre-existing pgcyan slot/publication means the source is in use by
    # a migration this registry can't see (torn-down or run externally).
    artifacts = probe.existing_artifacts(req.source.to_pguri())
    if artifacts:
        registry.remove(mid)  # release the slot we just claimed
        raise HTTPException(
            status_code=409,
            detail=(
                f"source {req.source.endpoint()} already carries pgcyan "
                f"replication state ({', '.join(artifacts)}); drop it or reuse that "
                "migration before provisioning a new one"
            ),
        )

    try:
        result = provisioner.provision(req, migration_id=mid)
    except ProvisionError as exc:
        registry.remove(mid)
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 -- surface any k8s failure as 502
        registry.remove(mid)
        raise HTTPException(status_code=502, detail=f"provisioning failed: {exc}")

    return MigrationCreated(**result)


def _lookup(registry: Registry, migration_id: str) -> dict:
    row = registry.get(migration_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"no migration {migration_id!r}")
    return row


@app.post("/migrations/{migration_id}/start", response_model=MigrationStarted)
def start_migration(
    migration_id: str,
    registry: Registry = Depends(get_registry),
    controller: ControllerClient = Depends(get_controller_client),
) -> MigrationStarted:
    """Tell the migration's controller to take the snapshot and walk the DAG.

    Proxies to the controller's ``POST /start`` (idempotent server-side) and
    advances the registry to the resulting state (#89).
    """
    row = _lookup(registry, migration_id)
    if row["state"] in TERMINAL_STATES:
        raise HTTPException(
            status_code=409,
            detail=f"migration {migration_id} already finished (state={row['state']})")

    ns = row["namespace"]
    try:
        code, body = controller.start(ns)
    except ControllerUnreachable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    if code not in (200, 202):
        raise HTTPException(
            status_code=502,
            detail=f"controller rejected start (HTTP {code}): {body}")

    new_state = _registry_state_for(body.get("state", "running"))
    if new_state != row["state"]:
        registry.set_state(migration_id, new_state, _now())
    return MigrationStarted(
        id=migration_id, namespace=ns, state=new_state,
        message=body.get("message", "start accepted"))


@app.get("/migrations/{migration_id}/status", response_model=MigrationStatus)
def migration_status(
    migration_id: str,
    registry: Registry = Depends(get_registry),
    controller: ControllerClient = Depends(get_controller_client),
) -> MigrationStatus:
    """Aggregate the controller's live ``/status`` with the registry state (#89).

    Best-effort: if the controller can't be reached (e.g. torn down) the registry
    state is still returned. A controller-reported transition is written back to
    the registry so it converges -- and a terminal state frees the singleton slot.
    """
    row = _lookup(registry, migration_id)
    state = row["state"]

    controller_body = None
    reachable = False
    try:
        code, body = controller.status(row["namespace"])
        if code == 200:
            reachable = True
            controller_body = body
            cs = body.get("state")
            if cs:
                mapped = _registry_state_for(cs)
                if mapped != state:
                    registry.set_state(migration_id, mapped, _now())
                    state = mapped
    except ControllerUnreachable:
        # Leave the registry state as-is; report the controller as unreachable.
        pass

    return MigrationStatus(
        id=migration_id, namespace=row["namespace"], state=state,
        controller_reachable=reachable, controller=controller_body)
