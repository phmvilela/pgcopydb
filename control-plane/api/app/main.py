"""
pgcyan control-plane API (skeleton -- issue #86).

The control plane is a single, long-lived FastAPI service in the fixed
``pgcyan-system`` namespace. It will (in later tickets) provision a per-migration
namespace + scaffolding and drive the controller's wait-to-start gate; a
node-local SQLite registry on a ReadWriteOnce volume tracks migrations.

This skeleton exposes only a liveness/readiness endpoint so the Deployment can
be stood up and health-checked. Provisioning, the registry, and start/status/
teardown land in follow-up tickets (#87/#88/#89/#90).

``DATA_DIR`` (default ``/data``) is the mounted RWO volume where the SQLite
registry file will live; the skeleton only reports that it is writable so the
"file under /data survives a restart" acceptance check has something to target.
"""
import os

from fastapi import FastAPI

#: Mounted RWO volume for the SQLite registry (later tickets). Overridable for
#: local runs / tests where no volume is mounted.
DATA_DIR = os.getenv("PGCYAN_DATA_DIR", "/data")

app = FastAPI(title="pgcyan control-plane API", version="0.1.0")


@app.get("/health")
def health() -> dict:
    """Liveness/readiness probe target.

    Returns 200 with a small JSON body. ``data_dir_writable`` lets an operator
    confirm the registry volume is mounted read-write without a separate check;
    it never fails the probe (the skeleton has no registry yet).
    """
    return {
        "status": "ok",
        "service": "pgcyan-control-plane",
        "data_dir": DATA_DIR,
        "data_dir_writable": os.path.isdir(DATA_DIR) and os.access(DATA_DIR, os.W_OK),
    }
