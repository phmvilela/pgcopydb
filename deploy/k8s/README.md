# deploy/k8s — sharded pgcopydb migration manifests

Kubernetes materialization of the shared-nothing, planner-driven migration
architecture. Full explanation of how the objects form the DAG:
[../../notes/k8s-sharded-migration-topology.md](../../notes/k8s-sharded-migration-topology.md).

These are the **node** objects. Ordering/barriers between phases are enforced by
the **orchestrator** (`pg_cyan.cli orchestrate`, `pg_cyan/orchestrator.py`,
issue #40 — custom Python using the Kubernetes client and the RBAC in
`01-rbac.yaml`, chosen over Argo Workflows so no new CRDs/controller are
needed; see the module docstring for the full rationale) — plain YAML cannot
express "wait for all shards". Files are numbered in apply order; the
orchestrator applies phases −1 through 4 (namespace/RBAC/PVC/Secrets and the
phase −2 input collector are assumed already applied).

| File | Phase | Kind | Role |
|---|---|---|---|
| `00-namespace.yaml` | — | Namespace | scope + teardown unit |
| `01-rbac.yaml` | — | SA/Role/Binding | orchestrator (creates Jobs) + worker (DB-only) identities |
| `10-secrets.example.yaml` | — | Secret (EXAMPLE) | DB URIs; **do not commit real values** — prefer Workload Identity |
| `11-configmap-inputs.yaml` | −1 | ConfigMap | planner inputs (metrics/params/constraints) |
| `12-configmap-plan.yaml` | −1 | ConfigMap | planner OUTPUT: `plan.json` + per-shard `--filters` |
| `20-pvc-workdir.yaml` | — | PVC (RWO ×3) | per-workload pgcopydb `--dir` disks (pre-data/FK/CDC); shards get one each, stamped by the templates — issue #58 |
| `25-job-collect-inputs.yaml` | −2 | Job | collectors hit the source DB → write the inputs ConfigMap |
| `30-job-planner.yaml` | −1 | Job | runs the planner (`pg_cyan.cli plan`) |
| `40-job-phase0-predata.yaml` | 0 | Job | schema without secondary idx/FK |
| `50-deployment-snapshot-holder.yaml` | 1 | Deployment(1)+PDB | `pg_cyan.cli snapshot-hold`: creates publication + pgoutput slot, exports & HOLDS the consistent snapshot, publishes the id to the `pgcyan-snapshot` ConfigMap |
| `60-job-phase2-datashard.template.yaml` | 2 | PVC+Job × N | concurrent HEAP-only COPY shards, each on its own RWO work disk |
| `70-job-phase3-index.template.yaml` | 3a | PVC+Job × M | secondary index builds (shardable), each on its own RWO work disk |
| `71-job-phase3-fk.yaml` | 3b | Job (single) | FK/constraint single coordinated pass |
| `80-deployment-follow-cdc.yaml` | 4 | Deployment(1)+PDB | optional CDC catch-up → cutover |

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
**orchestrator** applies the phase manifests, it fills the same placeholder with
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

## Running just the planning phases end-to-end

To execute the **planning half only** — the phase −2 collector (`25`) and the
phase −1 planner (`30`), producing the `pgcyan-plan` ConfigMap without moving
any data — use the `planner` job of the manual
`.github/workflows/integration-tests.yml` workflow (issue #50). It logs in to
the cluster (OIDC), materializes the source-DB `pgcyan-source-pg` Secret from
GitHub secrets, fills `${PGCYAN_IMAGE}` with the commit-short-SHA image, applies
jobs `25` then `30` with a completion barrier between them, and uploads
`plan.json` + both Jobs' logs as a build artifact. A source with any large
object correctly fails the planner (LOB abort gate) with no plan written. That
same workflow's `snapshot-holder` job exercises phase 1. The cluster itself is
provisioned from `terraform/aks` by a human running `terraform apply` (Azure
Cloud Shell or local — it needs Owner-level rights, so it is not a CI action;
the companion `.github/workflows/terraform-aks.yml` only `fmt`/`validate`s the
module). See each workflow's header for the required repo secrets/variables.
