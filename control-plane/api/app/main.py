"""
pgcyan control-plane API (issues #86, #87).

The control plane is a single, long-lived FastAPI service in the fixed
``pgcyan-system`` namespace. It provisions a per-migration namespace with the
full migration scaffolding (RBAC, work-dir PVC, DB Secret, the controller in
wait-to-start mode) and will (later tickets) drive start/status/teardown; a
node-local SQLite registry on a ReadWriteOnce volume tracks migrations.

Endpoints so far:
* ``GET /health``     -- liveness/readiness.
* ``POST /migrations`` -- provision a parked per-migration namespace (#87).

``DATA_DIR`` (default ``/data``) is the mounted RWO volume for the SQLite
registry (later tickets); ``PGCYAN_MANIFESTS_DIR`` (default ``/app/deploy/k8s``)
is where the shared migration manifests are baked; ``PGCYAN_MIGRATION_IMAGE`` is
the pgcyan image the provisioned controller runs.
"""
import os

from fastapi import Depends, FastAPI, HTTPException

from app.models import MigrationCreated, MigrationRequest
from app.provisioner import Provisioner, ProvisionError

DATA_DIR = os.getenv("PGCYAN_DATA_DIR", "/data")
MANIFESTS_DIR = os.getenv("PGCYAN_MANIFESTS_DIR", "/app/deploy/k8s")

app = FastAPI(title="pgcyan control-plane API", version="0.1.0")


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


@app.get("/health")
def health() -> dict:
    """Liveness/readiness probe target (see #86)."""
    return {
        "status": "ok",
        "service": "pgcyan-control-plane",
        "data_dir": DATA_DIR,
        "data_dir_writable": os.path.isdir(DATA_DIR) and os.access(DATA_DIR, os.W_OK),
    }


@app.post("/migrations", status_code=201, response_model=MigrationCreated)
def create_migration(
    req: MigrationRequest,
    provisioner: Provisioner = Depends(get_provisioner),
) -> MigrationCreated:
    """Provision a fresh per-migration namespace with the controller parked
    pre-snapshot (wait-to-start). Does NOT start the migration (#87)."""
    try:
        result = provisioner.provision(req)
    except ProvisionError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 -- surface any k8s failure as 502
        raise HTTPException(status_code=502, detail=f"provisioning failed: {exc}")
    return MigrationCreated(**result)
