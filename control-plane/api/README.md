# pgcyan control-plane API

FastAPI service for the pgcyan control plane (design #84). It runs in the fixed
`pgcyan-system` namespace and provisions/observes per-migration namespaces via
its in-cluster ServiceAccount.

## Endpoints

| Method | Path | What |
|---|---|---|
| GET | `/health` | liveness/readiness (#86) |
| POST | `/migrations` | provision a fresh per-migration namespace with the controller parked pre-snapshot; returns `{id, namespace}` (#87). **Does not start** the migration. Rejected with **409** if another migration is active (singleton) or the source already carries the pgcyan slot/publication (clash, #88). |
| GET | `/migrations` | list tracked migrations from the registry, newest first — no credentials (#88). |

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
  app/main.py            # FastAPI app: GET /health, POST/GET /migrations
  app/models.py          # request/response models
  app/provisioner.py     # load deploy/k8s, retarget namespace, fill env, apply
  app/k8s.py             # in-cluster applier (thin k8s-client I/O)
  app/registry.py        # node-local SQLite registry on the RWO PVC (#88)
  app/source_probe.py    # best-effort source slot/publication clash probe (#88)
  requirements.txt       # fastapi, uvicorn, kubernetes, PyYAML, psycopg2-binary
  requirements-dev.txt   # + pytest, httpx
  Dockerfile             # slim, non-root; built from the REPO ROOT context
  tests/
```

## Registry, singleton & clash detection (#88)

The registry is a SQLite file on the RWO PVC (`/data/registry.db`, #86) tracking
`{id, namespace, source, target, state, timestamps}` — **labels only, never
credentials**. It survives API pod restarts (the PVC is durable; the Deployment
is `Recreate`, single replica → single writer). `POST /migrations` is gated by:
- **singleton** — one active (`waiting-to-start`/`running`) control-plane
  migration at a time → **409**. Enforced by **SQLite itself**, not an
  application-level check-then-act: a partial unique index over a constant,
  restricted to the active states, makes a second active row impossible, so the
  database picks the winner of concurrent requests. The endpoint *reserves* the
  row before provisioning and rolls it back on clash/failure. A migration
  reaching a terminal state drops out of the index and **frees the slot
  automatically** — no explicit release, so a crashed pod can't wedge the plane.
- **clash** — a best-effort psycopg2 probe of the source for a pre-existing
  `pgcyan_slot`/`pgcyan_pub` (a torn-down or externally run migration the
  registry can't see) → **409**. Probe connection/permission failures don't
  block (non-clashing databases stay the operator's responsibility).

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
