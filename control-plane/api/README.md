# pgcyan control-plane API

FastAPI service for the pgcyan control plane (design #84). It runs in the fixed
`pgcyan-system` namespace and provisions/observes per-migration namespaces via
its in-cluster ServiceAccount.

## Endpoints

| Method | Path | What |
|---|---|---|
| GET | `/health` | liveness/readiness (#86) |
| POST | `/migrations` | provision a fresh per-migration namespace with the controller parked pre-snapshot; returns `{id, namespace}` (#87). **Does not start** the migration. |

`POST /migrations` body:

```json
{
  "source": {"host": "src.pg", "user": "u", "password": "p", "database": "db"},
  "target": {"uri": "postgres://u:p@tgt.pg:5432/db?sslmode=require"},
  "shards": 4
}
```

`source`/`target` accept either a full `uri` or discrete parts (`host`+`user`+
`database` required; assembled into a URI with userinfo percent-encoded).
`shards` is optional.

## Layout

```
control-plane/api/
  app/main.py            # FastAPI app: GET /health, POST /migrations
  app/models.py          # request/response models
  app/provisioner.py     # load deploy/k8s, retarget namespace, fill env, apply
  app/k8s.py             # in-cluster applier (thin k8s-client I/O)
  requirements.txt       # fastapi, uvicorn, kubernetes, PyYAML
  requirements-dev.txt   # + pytest, httpx
  Dockerfile             # slim, non-root; built from the REPO ROOT context
  tests/
```

## How provisioning stays drift-free

The provisioner reads the **same** `deploy/k8s` manifests the integration test
applies (baked into the image at `/app/deploy/k8s`) and only transforms them at
apply time, mirroring `kubectl -n` + `envsubst`:
- **namespace retarget** — rewrites the hardcoded `pgcyan-migration` namespace
  (incl. RoleBinding subjects) to the per-migration namespace;
- **placeholder fill** — fills the controller's `${PGCYAN_*}` env (image,
  `PGCYAN_WAIT_FOR_START=true`, …).

## Run locally

```bash
cd control-plane/api
pip install -r requirements-dev.txt
uvicorn app.main:app --reload          # http://127.0.0.1:8000/health
python -m pytest                       # tests
```

## Build the image (from the repo root)

The build context is the **repo root** (like the migration image) so `deploy/k8s`
can be baked in — pass the Dockerfile explicitly:

```bash
docker build -f control-plane/api/Dockerfile -t pgcyan-control-plane:dev .
az acr build -r <acr> -f control-plane/api/Dockerfile \
    --image pgcyan-control-plane:<tag> .
```

Deployment manifests: [`../../deploy/control-plane/`](../../deploy/control-plane/).
The controller image the provisioner stamps into each migration comes from the
`PGCYAN_MIGRATION_IMAGE` env on the API Deployment.
