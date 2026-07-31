# Kubernetes topology for the sharded pgcopydb migration (one-pager)

**What this is.** The `deploy/k8s/` manifests materialize the shared-nothing,
planner-driven migration: an intelligent planner emits a plan, a **controller**
holds one consistent snapshot and walks that plan as a **phased DAG with
barriers**, and each phase is one or more plain Kubernetes objects (Jobs, per-shard
RWO disks, one CDC Deployment). This is a cross-cutting map of the *engine*; the
per-node detail lives in the module docstrings (`pg_cyan/*.py`) and the manifests.
It has run green end-to-end on AKS (7 tables / ~4.8M rows across 4 shards).

## The graph

```
 inputs CM (11) ──▶ Job: planner (30) ──▶ plan CM (12): plan.json + shard-*.filters
 metrics/params/         bin-pack tables            │  every downstream object reads
 constraints             into N disjoint shards     │  ONLY its own slice from here
                                                     ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │ CONTROLLER  StatefulSet(1) pgcyan-controller (35)   -- run-migration, #70     │
 │   • HOLDS the consistent snapshot in-pod (pgcopydb snapshot --follow child)   │
 │   • WALKS the DAG below, INJECTING SNAPSHOT_ID into each phase pod            │
 │   • OWNS every workload it creates (ownerRef → delete controller = cascade)   │
 │   • refuses to resume a lost-snapshot restart (parks not-ready)              │
 └───────┬─────────────────────────────────────────────────────────────────────┘
         │ applies + barriers, in order:
         ▼
   pre-data (0)  Job (40)          schema only — NO 2ndary idx / PK / FK / triggers
         │  barrier: must succeed
         ▼
   data shards (2)  Jobs datashard-0..N (60 tmpl)   concurrent, disjoint --filters,
         │  each on its OWN ReadWriteOnce disk       --snapshot <id>, DATA ONLY
         │  (pgcyan-workdir-shard-*, #58)            (pgcopydb copy table-data)
         │  each publishes pgcyan-copylog-<shard> ──▶ orchestrator merges ──▶ pgcyan-copylog
         │  BARRIER: all shards succeed
         ▼
   index (3a) Jobs 0..M (70 tmpl, shardable) + FK (3b) Job (71, single)
         │  BARRIER: schema+data equivalent
         ▼
   CDC   NOT a DAG node — the CONTROLLER streams it (Option A, #26): after the walk
         it releases the snapshot and runs `pgcopydb follow --resume` on its OWN
         --dir (one slot, one consumer → cutover). No follow-cdc Deployment.
```

The old **shared RWX PVC** and separate **snapshot-holder Deployment** are gone:
the snapshot is held *in the controller pod* and its id is **injected** into the
phase pods (no shared volume, no `pgcyan-snapshot` ConfigMap — #70), and each
shard gets its **own RWO managed disk** for pgcopydb's SQLite/WAL catalog, which
Azure Files (SMB) cannot host (#57/#58).

## Why each object is the kind it is

| Object | Kind | Why |
|---|---|---|
| planner (30) | Job | runs once, produces the plan, exits |
| **controller (35)** | **StatefulSet(1) + PDB + RWO --dir PVC** | holds the snapshot in-pod for the whole copy, drives the DAG, AND streams CDC after it (Option A, #26). StatefulSet for **at-most-one** (a Deployment can transiently run two during a rollout) + stable identity — **not** reschedule protection (see the fold, below). Its `--dir` is a managed-disk RWO volumeClaimTemplate (POSIX locking + CDC-store capacity). |
| pre-data (40) | Job | one coordinator pass, then done |
| data shards (60) | **Jobs, N of them**, each + its own RWO disk | horizontal scale-out = *more Jobs*, not fatter pods. COPY is **network-bound**, so more pods = more aggregate throughput. RWO disk = real POSIX locking for pgcopydb's catalog + resume state (#58). |
| index (70) | Jobs, M of them | index builds are independent → shardable |
| FK (71) | **single** Job | an FK spans two tables that may be in different shards; one coordinated pass (internal parallelism via `--restore-jobs`) |
| CDC (follow) | **the controller itself** (Option A, #26) — no separate object | logical decoding = one slot, one consumer; **cannot** be sharded. The slot-creator + streamer must share one pgcopydb `--dir`, so the controller (which created the slot) streams `follow --resume` on its own `--dir` after the walk — a separate pod with a fresh `--dir` cannot reuse the slot/sentinel. |

## The three things that make it correct (and where it bites)

1. **No overlap is enforced physically, not by convention.** Each data shard gets
   `--filters shard-n.filters` with a disjoint `[include-only-table]` list authored
   by the planner. Disjoint lists cannot collide — the whole safety basis of the
   shared-nothing shards. (The combined `pgcyan-copylog` records `warnings: []`
   when the union really was disjoint.)

2. **Consistency comes from one snapshot, held by the controller.** A Postgres
   snapshot is only valid while its transaction stays open, and the shards are
   separate pods that can't own it — so the *controller* holds it in-pod for all
   of phases 0–2 and injects the id. Alternative: a frozen source +
   `--not-consistent`, in which case nothing is held (planner records
   `snapshot.mode`).

3. **The resource model FLIPS at the phase-2/3 barrier — the key insight.**
   - Phase 2 (COPY) runs *through* the pod (source→pod→target); bottleneck is the
     pod's **network**. Fan out pods to go faster.
   - Phase 3 (index/FK) runs *on the target server*; the pod just issues DDL and
     waits. Fattening these pods does nothing. Go faster with **target-side**
     levers: concurrent build connections (`--index-jobs`/`--restore-jobs`, capped
     by target vCPU + `max_connections`), `maintenance_work_mem`,
     `max_parallel_maintenance_workers`, and — the real "grant more power" move —
     temporarily scaling the **target SKU** up for the window, then back down.
     So phase-3 pods are deliberately **thin**.

## The fold: why the controller owns everything (#70)

Earlier the snapshot holder was a *separate* Deployment. A finite orchestrator
could never own it (owner-refs would GC it mid-copy), so it leaked — and a leaked
holder is a **held replication slot = unbounded WAL growth on the source**. The
resolving realization: **a lost snapshot is already fatal** — if it dies, every
completed and in-flight shard was copying an instant that no longer exists → full
re-copy. So binding the snapshot's lifetime to the controller, and letting it
**own** the children (cascade-delete on death), just makes the blast radius match
a failure that was always total. A StatefulSet gives at-most-one but **not**
reschedule protection: an eviction still kills the pod and the snapshot with it —
fine, because that loss was always fatal, and on restart the controller **refuses
to resume** (parks not-ready) rather than silently re-snapshot. The one leaked
resource owner-ref cascade can't reach is the **source replication slot** (not a
k8s object) — specified as the suspended `90-cronjob-slot-reaper.example.yaml`.

## Barriers are the controller's job, not YAML's

Plain manifests can't express "wait until every shard succeeded." The controller
drives `PgCyanOrchestrator` (`pg_cyan/orchestrator.py`, #40; RBAC in `01`), which
applies a phase, watches Job completion (or Deployment readiness for CDC) via the
`*/status` subresources — **`jobs/status` / `deployments/status`, which RBAC does
NOT fold into a grant on `jobs`/`deployments`** — and only then applies the next,
failing fast the moment a barrier doesn't resolve. Manifests are the *nodes*; the
controller/orchestrator is the *edges*. Custom Python over the Kubernetes client
rather than Argo (the plain ServiceAccount in `01-rbac.yaml` is already exactly
what it needs, no CRDs) — but the node objects don't change, so Argo `dependsOn`
stays a drop-in later.

## Still open (tracked inline in the YAML / issues)

- **Range-split of one table across shards** — pgcopydb splits *within* a job
  (`--split-tables-larger-than`); splitting a giant table *across* shards is a
  planner responsibility not yet designed (#45). Oversized tables stay whole on
  their own shard.
- **Planner FK graph is a placeholder** — `plan.json` carries
  `fk_graph.status: "deferred"`; phase-3b applies all constraints in one pass
  regardless, so it's informational until collected (#44).
- **Phases 3/4 not built** — the integration test walks `--stop-after 2`; bump it
  as index/FK (#28) and CDC (#26) land.
- **Row-count source** — the shards count rows on the target (`count(*)`);
  revisiting vs. pgcopydb's own summary is #69.

**Related:** the module docstrings (per-node detail), the `run-integration-test`
skill (`.claude/skills/`, running/debugging the whole thing on-cluster),
`migration-phases-lessons-learned.md` (the non-obvious failures),
[issue-23-pgcopydb-vs-custom-python.md](issue-23-pgcopydb-vs-custom-python.md)
(where custom code earns its keep), and
[issue-22-migration-stage-assessment.md](issue-22-migration-stage-assessment.md)
(pgcopydb auto-creates the publication/slot — phase 4 relies on this).
