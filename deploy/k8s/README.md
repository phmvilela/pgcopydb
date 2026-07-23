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
| `20-pvc-workdir.yaml` | — | PVC (RWX) | shared `/work`: snapshot id + resume state |
| `25-job-collect-inputs.yaml` | −2 | Job | collectors hit the source DB → write the inputs ConfigMap |
| `30-job-planner.yaml` | −1 | Job | runs the planner (`pg_cyan.cli plan`) |
| `40-job-phase0-predata.yaml` | 0 | Job | schema without secondary idx/FK |
| `50-deployment-snapshot-holder.yaml` | 1 | Deployment(1)+PDB | `pg_cyan.cli snapshot-hold`: creates publication + pgoutput slot, exports & HOLDS the consistent snapshot |
| `60-job-phase2-datashard.template.yaml` | 2 | Job × N | concurrent HEAP-only COPY shards |
| `70-job-phase3-index.template.yaml` | 3a | Job × M | secondary index builds (shardable) |
| `71-job-phase3-fk.yaml` | 3b | Job (single) | FK/constraint single coordinated pass |
| `80-deployment-follow-cdc.yaml` | 4 | Deployment(1)+PDB | optional CDC catch-up → cutover |

`.template.yaml` files contain `${...}` placeholders the orchestrator substitutes
once per plan entry — they are **not** directly `kubectl apply`-able as-is.

Every file carries its assumptions and TODOs as inline comments.

## The image

Every pod runs one image — `pgcyanacr.azurecr.io/pgcyan:0.1.0`, digest-pinned
`@sha256:…` for immutability — bundling **pgcopydb**, **kubectl**, and the
**`pg_cyan` CLI** (issue #39). Build and push it to the ACR that `terraform/aks`
provisions:

```sh
# server-side build in ACR Tasks (no local Docker needed)
../../scripts/build-and-push-image.sh
```

or let CI do it (`.github/workflows/publish-image.yml`, on a `v*` tag or manual
dispatch). If your `acr_name` differs from the default `pgcyanacr`, repoint the
`image:` field in each manifest at your registry's login server (and prefer
pinning by `@sha256:` digest — the build script and workflow both print it).

## Pinning the digest across every manifest at once

`kustomization.yaml` bundles the directly-appliable manifests (everything
except the example Secret, the two generated ConfigMap stubs, and the two
non-standalone `.template.yaml` files — see the comments in that file) with an
`images:` override, so a fresh digest only needs to be set in **one** place:

```sh
kubectl kustomize deploy/k8s              # preview the rendered, pinned bundle
kubectl apply -k deploy/k8s               # apply the whole bundle yourself
```

CI's `deploy` job in `publish-image.yml` (manual-only —
`workflow_dispatch` with `deploy_to_cluster: true`) does this automatically
after a successful publish: it rewrites the `digest:` in `kustomization.yaml`
to the image it just pushed, uploads the fully rendered bundle as a build
artifact, but **applies only `00-namespace.yaml` / `01-rbac.yaml` /
`20-pvc-workdir.yaml`** — the cluster-scoped prerequisites the orchestrator
already assumes exist. It deliberately does not apply the phase manifests or
start the orchestrator itself; that starts a real migration run against real
databases, and stays a separate, deliberate action a human takes (e.g. the
`kubectl run ... -- python -m pg_cyan.cli orchestrate` one-liner printed in
that job's summary). See the workflow file's header comment for the Azure
role assignment and `AKS_RESOURCE_GROUP` / `AKS_CLUSTER_NAME` repo variables
it needs.

## Running just the planning phases end-to-end

To execute the **planning half only** — the phase −2 collector (`25`) and the
phase −1 planner (`30`), producing the `pgcyan-plan` ConfigMap without moving
any data — use the manual `.github/workflows/run-planner.yml` workflow (issue
#50). It logs in to the cluster (OIDC), materializes the source-DB
`pgcyan-source-pg` Secret from GitHub secrets, applies jobs `25` then `30` with
a completion barrier between them, and uploads `plan.json` + both Jobs' logs as
a build artifact. A source with any large object correctly fails the planner
(LOB abort gate) with no plan written. The cluster itself is provisioned from
`terraform/aks` by a human running `terraform apply` (Azure Cloud Shell or
local — it needs Owner-level rights, so it is not a CI action; the companion
`.github/workflows/terraform-aks.yml` only `fmt`/`validate`s the module). See
each workflow's header for the required repo secrets/variables.
