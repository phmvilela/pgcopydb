# pgcyan control-plane API

FastAPI service for the pgcyan control plane (issue #86, design #84). It runs in
the fixed `pgcyan-system` namespace and will provision/observe per-migration
namespaces via its in-cluster ServiceAccount. This is the **skeleton**: only
`GET /health` today; provisioning, the SQLite registry, and start/status/
teardown land in #87–#90.

## Layout

```
control-plane/api/
  app/main.py            # FastAPI app: GET /health
  requirements.txt       # runtime: fastapi, uvicorn
  requirements-dev.txt   # + pytest, httpx (TestClient)
  Dockerfile             # slim, non-root (uid 1000)
  tests/test_health.py
```

## Run locally

```bash
cd control-plane/api
pip install -r requirements-dev.txt
uvicorn app.main:app --reload            # http://127.0.0.1:8000/health
python -m pytest                         # tests
```

## Build the image

```bash
docker build -t pgcyan-control-plane:dev control-plane/api
docker run --rm -p 8000:8000 pgcyan-control-plane:dev
curl localhost:8000/health
```

Deployment manifests: [`../../deploy/control-plane/`](../../deploy/control-plane/).

## Notes

- `PGCYAN_DATA_DIR` (default `/data`) is the mounted RWO volume for the SQLite
  registry (later tickets); `/health` reports whether it is writable.
- Kept intentionally minimal — the Kubernetes client and DB drivers arrive with
  the provisioning/registry tickets.
