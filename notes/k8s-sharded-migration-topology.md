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
**orchestrator** (RBAC in `01`) applies a phase, watches Job completion, and only
then applies the next. The manifests are the *nodes*; the orchestrator is the
*edges*. If you'd rather make the DAG declarative, Argo Workflows / Jobs `dependsOn`
is a drop-in — the node objects don't change.

## Biggest open TODOs (tracked inline in the YAML)

- **Planner app doesn't exist** (`30`): needs a `pg_cyan.cli plan` subcommand.
- **`metrics.py` conflates heap+index size** — the planner needs them separate to
  size phase 2 vs phase 3. Fix before trusting the byte splits.
- **Range-split of a single table across shards** — pgcopydb splits *within* a job
  (`--split-tables-larger-than`); splitting one giant table *across* shards is a
  planner responsibility not yet designed.
- **Image + secrets** — `REPLACE_ME/pgcyan:latest` must be built (pgcopydb + planner
  CLI); prefer Azure Workload Identity over the example Secret (`10`).

**Related:** [pgcopydb-and-pgdump-tooling-assessment.md](pgcopydb-and-pgdump-tooling-assessment.md)
(where custom code earns its keep), [issue-22-migration-stage-assessment.md](issue-22-migration-stage-assessment.md)
(pgcopydb auto-creates the publication/slot — phase 4 relies on this).
