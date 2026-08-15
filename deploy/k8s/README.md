# deploy/k8s — sharded pgcopydb migration manifests

Kubernetes materialization of the shared-nothing, planner-driven migration
architecture. Full explanation of how the objects form the DAG:
[../../notes/k8s-sharded-migration-topology.md](../../notes/k8s-sharded-migration-topology.md).

These are the **node** objects. Ordering/barriers between steps are enforced by
the **orchestrator** (`pg_cyan.cli orchestrate`, `pg_cyan/orchestrator.py`,
issue #40 — custom Python using the Kubernetes client and the RBAC in
`01-rbac.yaml`, chosen over Argo Workflows so no new CRDs/controller are
needed; see the module docstring for the full rationale) — plain YAML cannot
express "wait for all shards". Files are numbered in apply order; the
orchestrator applies the plan → foreign-keys steps (namespace/RBAC/PVC/Secrets
and the collect input collector are assumed already applied).

| File | Step | Kind | Role |
|---|---|---|---|
| `00-namespace.yaml` | — | Namespace | scope + teardown unit |
| `01-rbac.yaml` | — | SA/Role/Binding | orchestrator (creates Jobs) + worker (DB-only) identities |
| `10-secrets.example.yaml` | — | Secret (EXAMPLE) | DB URIs; **do not commit real values** — prefer Workload Identity |
| `11-configmap-inputs.yaml`  | plan | ConfigMap | planner inputs (metrics/params/constraints) |
| `12-configmap-plan.yaml`  | plan | ConfigMap | planner OUTPUT: `plan.json` + per-shard `--filters` |
| `20-pvc-workdir.yaml` | — | PVC (RWO ×3) | per-workload pgcopydb `--dir` disks (schema/foreign-keys/follow); shards get one each, stamped by the templates — issue #58 |
| `25-job-collect-inputs.yaml`  | collect | Job | collectors hit the source DB, `capacity-inputs` explodes any declared bundle (optional `pgcyan-capacity-declared` ConfigMap, #142), `network-probe` measures RTT+throughput, `source-capacity` + `target-capacity` merge catalog fields (connections, WAL rate) → writes the inputs ConfigMap |
| `30-job-planner.yaml`  | plan | Job | runs the planner (`pg_cyan.cli plan`) |
| `35-statefulset-controller.yaml` | — (drives all) | StatefulSet(1)+Service+PDB | `pg_cyan.cli run-migration`: the folded snapshot-holder + DAG orchestrator (#70). HOLDS the consistent snapshot in-pod and walks plan→schema→copy→sequences→indexes→foreign-keys→follow, injecting `SNAPSHOT_ID` into the pods and owning every workload (delete it → cascade) |
| `40-job-schema.yaml`  | schema | Job | `pg_cyan.cli schema`: pre-data schema only (tables/sequences) — NO secondary indexes, PK/unique, FK, or triggers (those are post-data, #28); the controller injects `SNAPSHOT_ID` so the dump pins to the same instant the shards copy |
| `60-job-copy.template.yaml`  | copy | PVC+Job × N | `pg_cyan.cli copy`: concurrent DATA-ONLY (`pgcopydb copy table-data`) COPY shards, each on its own RWO work disk; writes per-shard `migration-copy-log.json` and publishes it to its `pgcyan-copylog-<shard>` ConfigMap (slot/publication/snapshot + per-table row counts), which the orchestrator merges into the combined `pgcyan-copylog` ConfigMap |
| `70-job-indexes.template.yaml`  | indexes | PVC+Job × M | secondary index builds (shardable), each on its own RWO work disk |
| `71-job-foreign-keys.yaml`  | foreign-keys | Job (single) | FK/constraint single coordinated pass |
| *(CDC — no manifest)* | — | the controller itself | Under Option A (#26) the controller streams `pgcopydb follow` on its own `--dir` after the walk; there is no separate CDC Deployment (the slot-creator + streamer must share a `--dir`). |
| `90-cronjob-slot-reaper.example.yaml` | — | CronJob (suspended EXAMPLE) | specification (#70) for reaping the orphaned SOURCE replication slot — the one leaked resource owner-ref cascade can't reach; customize the guard before enabling |

`.template.yaml` files contain `${...}` placeholders the orchestrator substitutes
once per plan entry — they are **not** directly `kubectl apply`-able as-is.

Every file carries its assumptions and TODOs as inline comments.

## The image

Every pod runs one image — bundling **pgcopydb**, **kubectl**, and the
**`pg_cyan` CLI** (issue #39). The manifests do **not** hard-code it: each
carries an `image: "${PGCYAN_IMAGE}"` placeholder filled at apply time. The
image is built and pushed to the ACR that `terraform/aks` provisions by
`.github/workflows/build.yml`, tagged by the **commit short SHA** only (no
`latest`, no version tag) — so `<acr>.azurecr.io/pgcyan:<short-sha>` ties a
running pod straight back to its source commit.

Fill the placeholder before applying:

```sh
export PGCYAN_IMAGE=pgcyanacr.azurecr.io/pgcyan:$(git rev-parse --short HEAD)
envsubst '${PGCYAN_IMAGE}' < deploy/k8s/30-job-planner.yaml | kubectl apply -f -
```

`envsubst '${PGCYAN_IMAGE}'` substitutes **only** that variable, leaving the
manifests' runtime `$(SNAPSHOT_ID)` / `${RESTORE_JOBS}` refs untouched. When the
**orchestrator** applies the step manifests, it fills the same placeholder with
its *own* running image (resolved from its pod spec), so every Job/Deployment it
launches shares its exact build. If your `acr_name` differs from the default
`pgcyanacr`, just set `PGCYAN_IMAGE` to your registry's login server.

## Previewing / applying the whole bundle

`kustomization.yaml` bundles the directly-appliable manifests (everything
except the example Secret, the two generated ConfigMap stubs, and the two
non-standalone `.template.yaml` files — see the comments in that file):

```sh
kubectl kustomize deploy/k8s                                 # preview (placeholder image)
PGCYAN_IMAGE=pgcyanacr.azurecr.io/pgcyan:$(git rev-parse --short HEAD) \
  kubectl kustomize deploy/k8s | envsubst '${PGCYAN_IMAGE}' | kubectl apply -f -
```

## Running just the planning steps end-to-end

To execute the **planning half only** — the collect step (`25`) and the
plan step (`30`), producing the `pgcyan-plan` ConfigMap without moving
any data — use the `planner` job of the manual
`.github/workflows/integration-tests.yml` workflow (issue #50). It logs in to
the cluster (OIDC), materializes the source-DB `pgcyan-source-pg` Secret from
GitHub secrets, fills `${PGCYAN_IMAGE}` with the commit-short-SHA image, applies
jobs `25` then `30` with a completion barrier between them, and uploads
`plan.json` + both Jobs' logs as a build artifact. A source with any large
object correctly fails the planner (LOB abort gate) with no plan written. That
same workflow's `controller` job (#70) stands up the folded controller
StatefulSet with `PGCYAN_STOP_AFTER=schema` — it holds the snapshot in-pod and walks
plan → schema, then asserts the real target has tables but no secondary
indexes/FK/triggers (needs `PG_TARGET_*` secrets/variables). The cluster itself is
provisioned from `terraform/aks` by a human running `terraform apply` (Azure
Cloud Shell or local — it needs Owner-level rights, so it is not a CI action;
the companion `.github/workflows/terraform-aks.yml` only `fmt`/`validate`s the
module). See each workflow's header for the required repo secrets/variables.
