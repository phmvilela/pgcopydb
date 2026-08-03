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

``PGCYAN_DATA_DIR`` (default ``/data``) is the mounted RWO volume that holds the
SQLite registry; ``PGCYAN_MANIFESTS_DIR`` (default ``/app/deploy/k8s``) is where
the shared migration manifests are baked; ``PGCYAN_MIGRATION_IMAGE`` is the
pgcyan image the provisioned controller runs.
"""
import os
from datetime import datetime, timezone
from typing import List

from fastapi import Depends, FastAPI, HTTPException

from app.models import MigrationCreated, MigrationRecord, MigrationRequest
from app.provisioner import (
    Provisioner,
    ProvisionError,
    namespace_for,
    new_migration_id,
)
from app.registry import Registry
from app.source_probe import SourceProbe

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
