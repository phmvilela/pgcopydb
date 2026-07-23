# deploy/k8s — sharded pgcopydb migration manifests

Kubernetes materialization of the shared-nothing, planner-driven migration
architecture. Full explanation of how the objects form the DAG:
[../../notes/k8s-sharded-migration-topology.md](../../notes/k8s-sharded-migration-topology.md).

These are the **node** objects. Ordering/barriers between phases are enforced by
the **orchestrator** (imperatively, using the RBAC in `01-rbac.yaml`) or by a
workflow engine — plain YAML cannot express "wait for all shards". Files are
numbered in apply order.

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
| `50-deployment-snapshot-holder.yaml` | 1 | Deployment(1)+PDB | holds the consistent snapshot |
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
