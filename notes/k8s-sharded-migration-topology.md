# Kubernetes topology for the sharded pgcopydb migration (one-pager)

**What this is.** The `deploy/k8s/` manifests materialize the shared-nothing,
planner-driven migration architecture we scrutinized: an intelligent planner
emits a plan, an orchestrator walks that plan as a **phased DAG with barriers**,
and each phase is one or more plain Kubernetes objects (Jobs, a couple of
Deployments, one RWX PVC). Nothing here is a running product yet — the planner
app and the container image are `TODO` — but the object graph and the seams
between them are concrete.

## The graph

```
                 ┌───────────────────────┐
 inputs CM ─────▶│ Job: planner (30)     │─────▶ plan CM (12): plan.json + shard-*.filters
 (11: metrics,   │  bin-pack, FK graph,  │            │
  params,        │  emit filters         │            │  every downstream object reads
  constraints)   └───────────────────────┘            │  ONLY its own slice from here
                                                       ▼
   PHASE 1 (long-lived)              PHASE 0 (barrier)
 ┌──────────────────────────┐     ┌────────────────────────┐
 │ Deploy: snapshot-holder  │     │ Job: phase0 pre-data   │  schema, NO 2ndary idx/FK
 │ (50) replicas=1, PDB     │     │ (40)                   │
 │ holds pg snapshot,       │     └───────────┬────────────┘
 │ writes /work/coordinator │                 │  barrier: pre-data must succeed
 │ /snapshot                │                 ▼
 └────────────┬─────────────┘     ┌────────────────────────────────────────┐
              │ snapshot id via    │ PHASE 2: Jobs datashard-0..N (60 tmpl) │
              └───────────────────▶│ concurrent, disjoint --filters,        │
                shared RWX PVC     │ --snapshot <id>, HEAP ONLY             │
                                   │ (--index-jobs=0 --skip-vacuum)         │
                                   └───────────────────┬────────────────────┘
                                                       │  BARRIER: all shards succeed
                                                       ▼
                            ┌──────────────────────────────────────────────┐
                            │ PHASE 3a: Jobs index-0..M (70 tmpl)  shardable│
                            │ PHASE 3b: Job phase3-fk (71)         single   │
                            └───────────────────┬──────────────────────────┘
                                                │  BARRIER: schema+data equivalent
                                                ▼
                            ┌──────────────────────────────────────────────┐
                            │ PHASE 4 (optional): Deploy follow-cdc (80)    │
                            │ replicas=1 (one slot, one consumer) → cutover │
                            └──────────────────────────────────────────────┘
```

Shared spine: **PVC `pgcyan-workdir` (20)** — one ReadWriteMany volume mounted by
every worker pod at `/work`, each using its own `subPath`. It carries two things:
the **snapshot id** (holder writes it, phases 0/2 read it → all copy the same
instant) and **pgcopydb resume state** (SQLite catalogs + per-table done markers,
so a killed pod resumes per-table instead of from zero).

## Why each object is the kind it is

| Object | Kind | Why |
|---|---|---|
| planner (30) | Job | runs once, produces the plan, exits |
| snapshot-holder (50) | **Deployment** replicas=1 + PDB | must stay Running for all of phases 0–2; a Postgres snapshot is only valid while its transaction is open. If it dies the initial copy must `--restart`. Singleton pet. |
| pre-data (40) | Job | one coordinator pass, then done |
| data shards (60) | **Jobs, N of them** | horizontal scale-out = *more Jobs*, not more pods per Job. COPY is **network-bound**, so more pods = more aggregate throughput. |
| index (70) | Jobs, M of them | index builds are independent → shardable |
| FK (71) | **single** Job | an FK spans two tables that may be in different shards; applied in one coordinated pass (internal parallelism via `--restore-jobs`) |
| follow/CDC (80) | Deployment replicas=1 + PDB | logical decoding = one slot, one consumer; **cannot** be sharded |

## The three things that make it correct (and where it bites)

1. **No overlap is enforced physically, not by convention.** Each data shard gets
   `--filters shard-n.filters` with a disjoint `[include-only-table]` list authored
   by the planner. Disjoint lists cannot collide. This is the whole safety basis of
   the shared-nothing shards.

2. **Consistency comes from one snapshot, held by one pod.** Because the shards are
   separate pods, none can own the snapshot for the others — hence the dedicated
   holder Deployment. Alternative: a frozen source + `--not-consistent`, in which
   case the holder is dropped (planner records `snapshot.mode`).

3. **The resource model FLIPS at the phase-2/3 barrier — the key insight.**
   - Phase 2 (COPY) runs *through* the pod (source→pod→target); bottleneck is the
     pod's **network**. Fan out pods to go faster.
   - Phase 3 (index/FK) runs *on the target server*; the pod just issues DDL and
     waits. Fattening these pods does nothing. Go faster by **target-side** levers:
     concurrent build connections (`--index-jobs`/`--restore-jobs`, capped by target
     vCPU and `max_connections`), `maintenance_work_mem`,
     `max_parallel_maintenance_workers`, and — the real "grant more power" move —
     temporarily scaling the **target SKU** up for the window, then back down.
     So phase-3 pods are deliberately **thin**.

## Barriers are the orchestrator's job, not YAML's

Plain manifests can't express "wait until every shard succeeded." The
**orchestrator** (`pg_cyan.cli orchestrate`, `pg_cyan/orchestrator.py`, issue
#40, RBAC in `01`) applies a phase, watches Job completion (or Deployment
readiness for the two long-lived phases), and only then applies the next,
failing fast — stopping the walk — the moment a phase's barrier does not
resolve to success. The manifests are the *nodes*; the orchestrator is the
*edges*. Implemented as custom Python against the Kubernetes client rather
than Argo Workflows (see the module docstring for the rationale — mainly:
`01-rbac.yaml`'s plain ServiceAccount is already exactly what it needs, no new
CRDs/controller). If you'd rather make the DAG declarative later, Argo
Workflows / Jobs `dependsOn` is still a drop-in — the node objects don't
change.

## Biggest open TODOs (tracked inline in the YAML)

- ~~**Planner app doesn't exist**~~ (`30`) — done (issue #38): `pg_cyan.cli plan`
  (`pg_cyan/planner.py`) LPT bin-packs tables into disjoint heap-balanced shards,
  emits `plan.json` + `shard-<n>.filters`, and aborts (writing no plan) when the
  source has large objects. FK graph and cross-shard range-split are deferred
  follow-ups (see below).
- ~~**`metrics.py` conflates heap+index size**~~ — fixed (issue #37):
  `source-pg-metrics.json` now reports `heap_bytes`/`index_bytes` per table and
  in `table_totals`, so phase 2 (COPY, heap) and phase 3 (index build) can be
  sized independently.
- ~~**Orchestrator app doesn't exist**~~ — done (issue #40): `pg_cyan.cli
  orchestrate` (`pg_cyan/orchestrator.py`) walks planner(-1) → pre-data(0) →
  snapshot-holder(1) → data shards(2) → index(3a) → FK(3b) → follow-CDC(4),
  substituting the `${...}` template placeholders from `plan.json` and
  blocking on each phase's barrier (Job completion, or Deployment readiness
  for the two long-lived phases) before applying the next; fails fast on the
  first phase that doesn't succeed.
- **Range-split of a single table across shards** — pgcopydb splits *within* a job
  (`--split-tables-larger-than`); splitting one giant table *across* shards is a
  planner responsibility not yet designed. The `plan` command (issue #38) keeps
  oversized tables whole on their own shard and records `split_tables_larger_than`
  for the within-job split; cross-shard splitting stays deferred.
- **Planner FK graph is a placeholder** — `plan.json` carries
  `fk_graph.status: "deferred"` (empty edges); no artifact collects FK edges yet.
  Phase-3b applies all constraints in one non-sharded pass regardless, so this is
  informational once implemented.
- ~~**Image + secrets**~~ — image built (issue #39, `.github/workflows/publish-image.yml`);
  every manifest is digest-pinned. Prefer Azure Workload Identity over the
  example Secret (`10`) still.
- ~~**No way to sync a fresh digest across every manifest**~~ — done (issue #40
  follow-up): `deploy/k8s/kustomization.yaml` bundles the directly-appliable
  manifests with an `images:` override; `publish-image.yml`'s manual-only
  `deploy` job rewrites the digest there after each publish and applies the
  00/01/20 cluster prerequisites (never the phase manifests — starting those
  is a deliberate separate human action).
- **Self-referential image digest** — the phase manifests (25/30/40/50/60/70/71/80)
  reference the SAME image whose build bundles them (`Dockerfile` `COPY
  deploy/k8s/`), so a manifest cannot know its own build's digest at the
  moment it's baked in — the digest committed/rendered into a manifest is
  always from a *previous* build. Fine for now (the orchestrator's own default
  `--manifests-dir` is only a convenience fallback; `kustomization.yaml`'s
  `images:` override is the actual source of truth for a given deploy), but a
  cleaner fix would have the orchestrator read manifests from a freshly
  rendered ConfigMap/volume supplied at run time instead of its own image's
  bundled copy. Not yet designed.

**Related:** [pgcopydb-and-pgdump-tooling-assessment.md](pgcopydb-and-pgdump-tooling-assessment.md)
(where custom code earns its keep), [issue-22-migration-stage-assessment.md](issue-22-migration-stage-assessment.md)
(pgcopydb auto-creates the publication/slot — phase 4 relies on this).
