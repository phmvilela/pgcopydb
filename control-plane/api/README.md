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
| POST | `/migrations/{id}/start` | proxy the controller's `POST /start` so it takes the snapshot and walks the DAG; advances the registry to `running` (#89). Idempotent. **404** unknown id, **409** already finished, **503** controller unreachable. |
| GET | `/migrations/{id}/status` | aggregate the controller's live `/status` with the registry state; a controller-reported transition is written back (a terminal state frees the singleton slot). Falls back to the registry view when the controller is unreachable (#89). |
| POST | `/migrations/{id}/done` | teardown: stop CDC gracefully (scale the controller to 0 → SIGTERM), delete the namespace (cascade), mark the registry `done` and free the singleton (#90). Idempotent. **404** unknown id, **502** teardown failure. |

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
  app/controller_client.py  # stdlib-urllib client for the controller API (#89)
  app/teardown.py        # graceful stop (scale-to-0) + namespace delete (#90)
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

## Start & status (#89)

The controller (#85) serves a control API on `:9999`, fronted by a **headless
Service** `pgcyan-controller` in the migration's namespace; it makes itself Ready
while parked in wait-to-start, so the Service routes to it *before* the migration
begins. The control plane reaches it by cross-namespace DNS —
`http://pgcyan-controller.<namespace>.svc.cluster.local:9999` — via a thin,
injectable stdlib-`urllib` client (`app/controller_client.py`, no new runtime
dep):
- **`POST /migrations/{id}/start`** proxies the controller's `POST /start`
  (idempotent) and advances the registry to `running`.
- **`GET /migrations/{id}/status`** aggregates the controller's live `/status`
  with the registry state. The controller and registry share one state
  vocabulary (`waiting-to-start`→`running`→`succeeded`|`failed`|`halted`), so a
  reported transition is written straight back to the registry; a **terminal
  state frees the singleton slot** (#88). If the controller is unreachable (e.g.
  torn down) the registry view is returned with `controller_reachable=false`.

## Teardown (#90)

`POST /migrations/{id}/done` ends a migration cleanly (`app/teardown.py`):
1. **Graceful CDC stop** — scale the controller StatefulSet to `0`. That is a real
   SIGTERM to the pod (honouring its grace period) which, unlike deleting the
   pod, does **not** respawn it. The controller catches SIGTERM in `park()` and
   stops `pgcopydb follow` cleanly — no SIGKILL, since the shutdown window is
   reconciled with the grace period in **#81/#83** (this endpoint's *no-SIGKILL*
   guarantee is delivered jointly with that change).
2. **Wait** for the controller pod to terminate — the observable proxy for "CDC
   stopped, consumer detached". The control plane holds no source credentials to
   probe the slot directly; the slot itself persists with 0 consumers for cutover
   (#27), which #83 asserts cluster-side.
3. **Delete the namespace** (cascade), then mark the registry `done` — freeing the
   singleton so the next migration can start.

Idempotent (an already-`done` migration is a no-op) and best-effort on the wait:
if the pod doesn't confirm a clean stop within the window the namespace is still
deleted, with `graceful=false` surfaced in the response.

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
