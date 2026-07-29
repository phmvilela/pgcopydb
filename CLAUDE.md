# CLAUDE.md

Guidance for AI agents (and humans) working in this repo. Read this before
making changes. Keep it up to date when conventions change.

## What this project is

`pg_cyan` orchestrates a **side-by-side PostgreSQL migration to Azure** with
minimal downtime, across three stages: **pre-migration → migration →
post-migration**. Deliverables are a mix of Python (checks/orchestration) and
Terraform (Azure provisioning). See `README.md` for the user-facing view.

Current state: the **pre-migration** stages (`preflight`, `source-parameters`,
`source-pg-metrics`, `source-largeobjects`, `plan`), the phase-0 **pre-data
schema transfer** (`restore-predata`), the phase-2 **sharded copy**
(`copy-shard`), the **migration DAG orchestrator** (`orchestrate`), and the
**folded controller** (`run-migration`, #70) that holds the consistent snapshot
in-pod and drives the whole `deploy/k8s/` sharded pgcopydb DAG, plus the
Terraform **worker VM** / **AKS cluster** modules exist. Post-migration is not
built yet.

## Where work comes from

- The backlog lives in **GitHub issues** on `berenguel/pgcyan`
  (`gh issue list`). Drive work from there.

## Architecture & the authoritative pattern

The **code is the source of truth** for design. `pg_cyan/preflight.py` and its
`PgCyanPreflight` class define the canonical shape for a stage orchestrator.
Follow it; do not introduce a competing per-check "write one JSON file per
check" pattern even if older notes describe one.

Key conventions embodied by `PgCyanPreflight`:

- **One orchestrator class per stage.** Connection/state lives on the instance
  (`self.conn`, `self.checks_passed/failed`, `self.warnings`, `self.errors`).
- **Each check is a method** returning `bool` and appending to the shared
  counters/lists. Results are aggregated by `run_all_checks()`, which returns a
  single combined dict (`checks` + `summary`). Output is combined, not
  one-file-per-check.
- **Structured JSON logging** via `_log_event(event_type, status, message,
  details)`. `status` is one of `success` / `warning` / `failure`. Always
  include a `remediation` hint in `details` on failures.
- **Graceful degradation.** Checks never raise to the caller — on error they
  log a `failure` event, record it, and return `False`. Connection failures
  short-circuit `run_all_checks()` with `ready_for_migration: false`.
- **A warning is not a blocker.** Increment `checks_passed` for warnings; only
  real problems increment `checks_failed` and flip readiness.

### Adding a new pre-migration check

1. Add a `check_<name>(self, ...) -> bool` method to `PgCyanPreflight`.
2. Guard on `self.conn` first (log a `failure` + return `False` if missing).
3. Wrap DB work in `try/except PgError`; on error, `_log_event(...,
   "failure", ...)` with a `remediation`, append to `self.errors`, bump
   `checks_failed`, return `False`.
4. On the happy path, `_log_event(..., "success", ...)`, bump `checks_passed`,
   return `True`.
5. Register it inside `run_all_checks()` under `results["checks"][...]`.
6. Add a DB-free unit test (guard path at minimum). Extend the CLI only if the
   check needs a new option.

Use `psycopg2.sql` composition for any query with interpolated identifiers or
literals — never f-string user/config values into SQL.

## CLI

`pg_cyan/cli.py` is a thin **click** wrapper. `python -m pg_cyan.cli` is the
entry point (also the Docker `ENTRYPOINT`). Each stage is a subcommand;
`preflight`, `source-parameters`, `source-pg-metrics`,
`source-largeobjects`, `plan`, `restore-predata`, `copy-shard`, `orchestrate`,
and `run-migration` are implemented (the standalone `snapshot-hold` command was
retired in #70 -- its `PgCyanSnapshot` is now driven in-pod by the controller). Options default to `PG_SOURCE_*`
env vars so the same invocation works locally and in the container. Exit codes:
`0` ready/success, `1` not ready/failure, `2` interrupted — keep these stable.

`source-parameters` (`pg_cyan/parameters.py`, `PgCyanParameters`) is the second
orchestrator and the template for **artifact-producing** stages: instead of a
pass/fail summary it writes a JSON *inventory* to disk. It follows the same
orchestrator conventions (connection/state on the instance, `_log_event`,
graceful degradation — `export()` never raises). The file contract is the
important part and comes from its issue: a non-empty `pg-source-parameters.json`
on success, or `pg-source-parameters-errors.json` carrying an error message on
any failure (connection, query, empty result, or write). Both land in
`--output-dir` / `$PG_OUTPUT_DIR`. Keep this success-vs-error two-file shape for
future artifact stages.

`source-pg-metrics` (`pg_cyan/metrics.py`, `PgCyanMetrics`) is the third
orchestrator, another artifact-producing stage built on the same
success-vs-error two-file shape. It collects PostgreSQL version, object
counts (tables, partitions, views, indexes, sequences, functions, procedures,
triggers, extensions), database size, and per-table sizes. Table sizes are
reported as **separate heap and index figures** (`heap_bytes`/`index_bytes`,
via `pg_table_size()`/`pg_indexes_size()`), per table and as database-wide
`table_totals`, so the sharded-migration planner can size the network-bound
COPY (heap) apart from the index build (issue #37); the two sum to the old
conflated `pg_total_relation_size`. On success it writes a non-empty
`source-pg-metrics.json`; on any failure (connection, query, empty result,
or write) it writes `source-pg-metrics-errors.json` with the error message
(issue #3). Object counts rely on `pg_proc.prokind`, which requires
PostgreSQL 11+.

`source-largeobjects` (`pg_cyan/lobs.py`, `PgCyanLobs`) is the
fourth orchestrator, another artifact-producing stage on the same
success-vs-error two-file shape. It counts the rows in
`pg_largeobject_metadata` (the source database's PostgreSQL large objects,
copied by a separate `pg_dump` path than the per-table `COPY`) and writes the
count to a non-empty `source-pg-largeobjects.json`; on any failure
(connection, query, or write) it writes `source-pg-largeobjects-errors.json`
with the error message (issue #20). **A count of zero is a valid happy path**
— an empty database is not a failure — so `collect()` guards on the count
being `is not None` rather than truthiness (contrast `source-parameters`,
where an empty result *is* a failure).

`plan` (`pg_cyan/planner.py`, `PgCyanPlanner`) is the **migration planner**
(issue #38) and the first stage that is a *pure offline transform* rather than
a DB collector: it reads the already-collected artifacts from `--inputs-dir`
(`source-pg-metrics.json` + `source-pg-largeobjects.json` required;
`pg-source-parameters.json` / `constraints.json` optional) and never opens a DB
connection. It LPT bin-packs tables into disjoint copy shards balanced by
**heap bytes** and writes `plan.json` plus one `shard-<n>.filters` per shard
(pgcopydb `--filters` format with a disjoint `[include-only-table]` list —
disjointness is re-validated before any file is written). It still follows the
orchestrator conventions (`_log_event`, graceful degradation — `plan()` never
raises) and the success-vs-error two-file shape: `plan.json` on success,
`plan-errors.json` on any failure. A **LOB abort gate** runs first — if
`source-pg-largeobjects.json` reports `lob_count > 0`, `plan()` writes no plan
and returns failure (exit 1), because pgcopydb pgoutput CDC does not replicate
large objects. Fixed at N=4 shards for now; the FK graph is emitted as
`"deferred"` and cross-shard range-splitting of an oversized table is out of
scope (both deferred to follow-ups, per the resolved sub-questions in #38).
This is the phase `-1` node in `deploy/k8s/30-job-planner.yaml`; its output
populates `12-configmap-plan.yaml`.

`PgCyanSnapshot` (`pg_cyan/snapshot.py`) is the **snapshot holder** (issue #23).
Since #70 it is the snapshot **sub-component of the folded controller** (held
in-pod by `run-migration`), not a standalone `snapshot-hold` command / phase-1
Deployment (both retired). It still both talks to the source **and** stays
running (the controller keeps its `pgcopydb snapshot --follow` child alive for
the whole copy). In one coordinated step it creates the publication `FOR ALL TABLES`,
then drives `pgcopydb snapshot --follow --plugin pgoutput --slot-name <slot>`
(a thin wrapper -- **do not** hand-roll `CREATE_REPLICATION_SLOT` SQL) to create
the slot, export its **consistent snapshot**, write the snapshot id to the
shared `/work` volume, and **hold the exporting connection open** so phases 0/2
pin the same instant. pgcopydb's `snapshot` path does not manage the
publication, so the holder creates it itself (a single `CREATE PUBLICATION` via
`psycopg2.sql`, not the discouraged slot SQL). It does **not** create/advance a
replication origin. It still follows the orchestrator conventions (`_log_event`,
graceful degradation -- `establish()` never raises) and the success-vs-error
two-file shape, but with **two** artifacts: `source-pg-slot.json` (slot +
snapshot name + captured `consistent_point`) and `source-pg-publication.json`
(publication name), or matching `*-errors.json` on failure. Publication/slot/
mode default to the planner's (`PGCYAN_PUBLICATION`/`PGCYAN_SLOT`/
`PGCYAN_SNAPSHOT_MODE`) and can be overridden from `plan.json` via
`--plan-file`. Because the holder is **authoritative and non-resumable** (a
restart == a brand-new snapshot), `establish()` starts from a clean slate: it
`reset_work_dir()`s any prior run's pgcopydb catalog (a leftover catalog pushes
pgcopydb into `--resume` mode, which a consistent snapshot cannot use ->
`Option --resume requires option --not-consistent`) and stale output files (a
leftover `source-pg-slot.json` would trip the readiness probe early), then
`drop_stale_slot()`s any *inactive* leftover replication slot (a persistent slot
survives a clean prior exit, and a fresh consistent snapshot can only be exported
at slot creation). An **active** slot is a hard failure -- another holder or a
leaked connection owns it. Separately, pgcopydb keeps a **SQLite** catalog under
`--dir` and opens it in **WAL** mode, which needs locking/shared-memory that
**Azure Files (SMB) does not provide** (`PRAGMA journal_mode = WAL failed:
database is locked`). So the holder's `--dir` (`PGCYAN_WORK_DIR`, `work_dir`)
lives on a **node-local `emptyDir`** -- fine, since the holder is non-resumable.
The **snapshot-id handoff** (issue #58) no longer rides a shared volume:
`publish_snapshot_configmap()` publishes the exported id to the **`pgcyan-snapshot`
ConfigMap** via `kubectl` (mirroring how the planner publishes `plan.json`), and
the data shards read it via `configMapKeyRef`. So the holder needs **no shared
PVC at all** and runs under the **`pgcyan-orchestrator`** SA (for the ConfigMap
write; DB auth is via the `pgcyan-db-uris` Secret, SA-independent). The
data/index shards' own WAL-on-SMB problem is resolved the same-family way (issue
#58, **option 2b**): each shard/singleton pgcopydb pod gets its **own
ReadWriteOnce managed disk** for `--dir` (real POSIX locking, and — unlike an
`emptyDir` — resume state survives a pod restart), so they stay plain Jobs
(no StatefulSet) — see `deploy/k8s/20-pvc-workdir.yaml`. The lifecycle splits
into `establish()` (create + hold + write
outputs; returns a summary) and `hold()` (block until SIGTERM/SIGINT, then
terminate pgcopydb so the replication connection closes -- a graceful pod exit
leaves **no stale source connection**, per the ticket's PS). Since #70 this runs
**in-pod inside the controller** (no `50-*` Deployment, no `pgcyan-snapshot`
ConfigMap): the controller keeps the `pgcopydb snapshot --follow` child alive and
injects the id into the phase pods directly. Its integration coverage is now the
`controller` job of `.github/workflows/integration-tests.yml` (see the
`run-migration` / `PgCyanController` entry), not a standalone `snapshot-holder`
job.

`restore-predata` (`pg_cyan/predata.py`, `PgCyanPreData`) is the phase `0`
**pre-data schema transfer** (issue #24): it creates the *target* schema before
any data shard runs, so every shard finds its tables already present. It is a
thin wrapper over pgcopydb (like the snapshot holder), NOT a DB collector -- it opens
no psycopg2 connection, only drives subprocesses. It transfers the **pre-data
section only**: tables, sequences, functions, LOB metadata -- and deliberately
NOT secondary indexes, **primary-key/unique constraints**, foreign keys, or
triggers. This is a real pgcopydb/`pg_dump` fact worth remembering: PK and UNIQUE
constraints are backed by indexes and live in the **post-data** section
(`pgcopydb clone` builds them *after* the COPY, steps 6-10 of
`pgcopydb_clone.rst`), so a pre-data restore yields tables with **no** PKs. All
of those deferred objects are built by the post-data phase (#28), which is much
faster than maintaining them during COPY. Because `pgcopydb restore pre-data`
restores from a custom-format dump file in `--dir` and this pod's `--dir` starts
empty, the wrapper runs **two** pgcopydb steps: `dump schema --snapshot=<id>`
(pinned to the phase-1 holder's snapshot so the schema matches the instant the
shards copy) then `restore pre-data --no-owner --drop-if-exists` (pre-data
section only; `--no-owner` assumes target roles are pre-created by Terraform;
`--drop-if-exists` keeps retries idempotent). The snapshot id comes from the
`pgcyan-snapshot` ConfigMap the holder publishes, read via `configMapKeyRef` --
so **the holder (phase 1) runs BEFORE pre-data (phase 0)** in the orchestrator
DAG (the phase names keep their numbers; only the apply order changed). It still
follows the orchestrator conventions (`_log_event`, graceful degradation --
`run()` never raises) and the success-vs-error two-file shape:
`target-pg-schema-log.json` on success (carrying a success message + the
pgcopydb commands run), or `target-pg-schema-log-errors.json` on any failure.
`--not-consistent` waives snapshot pinning (the dump takes its own snapshot) for
a frozen/read-only source. This is the node in
`deploy/k8s/40-job-phase0-predata.yaml`; its integration test is the `pre-data`
job of `.github/workflows/integration-tests.yml`, which stands up the holder,
runs the phase-0 Job against a **real target**, and asserts the target ends up
with tables but zero secondary indexes / FKs / triggers.

`copy-shard` (`pg_cyan/datashard.py`, `PgCyanDataShard`) is the phase `2`
**sharded initial copy** (issue #25): one Job per copy shard (stamped from
`deploy/k8s/60-job-phase2-datashard.template.yaml`), all concurrent, each copying
its OWN disjoint set of tables (the planner's `shard-<n>.filters`
`[include-only-table]` list — disjointness is the whole no-overlap guarantee) so
no row is copied twice and none is missed. Like `restore-predata` it is a thin
pgcopydb wrapper (no psycopg2 for the copy itself), NOT a DB collector. The
central design fact (resolved in #25 against the ticket's literal wording, per
`notes/migration-phases-lessons-learned.md` item 6): it drives **`pgcopydb copy
table-data`, NOT `pgcopydb clone`**. `clone` runs pre-data *and* post-data DDL
around the COPY; phase 0 (#24) already did pre-data and phase 3 (#28) does
post-data, so the shards must be **DATA ONLY** — `copy table-data` copies the
HEAP and nothing else (no schema, no indexes, no vacuum), sidestepping the Azure
extension/ownership walls (item 2) and collisions with phases 0/3. Pinned to the
holder's `--snapshot` (from `$SNAPSHOT_ID`, injected by the controller — #70) so
every shard copies the same instant; resumable on its own RWO disk (`--resume`;
`--restart` is deliberately not passed — it wipes temp files and `copy table-data`
rejects `--restart=false`), so — unlike the non-resumable pre-data pod — it does
**not** reset its work dir. It follows the orchestrator conventions (`_log_event`,
graceful degradation — `run()` never raises) and the success-vs-error two-file
shape: `migration-copy-log.json` on success (a `completed` status, the slot /
publication / snapshot names, and per-table row counts), or
`migration-copy-log-errors.json` on failure. The **row counts are ground truth**:
after the COPY it runs `SELECT count(*)` on the **target** per table in the shard's
filter set (pgcopydb's progress JSON only exposes the *source* `reltuples`
estimate), gated by `--count-rows` / `$PGCYAN_COUNT_ROWS` (default on; disable for
very large tables where the seq-scan — indexes are phase 3 — is too costly).
Because the shard's work dir is a private RWO disk the orchestrator cannot mount,
it ALSO publishes the copy log to its OWN **`pgcyan-copylog-<shard>` ConfigMap**
(via `kubectl`, mirroring the snapshot holder), labelled `pgcyan.dev/copylog=shard`
— that durable, structured ConfigMap is how the per-table counts reach the
orchestrator (see below). The shard runs under the **`pgcyan-worker`** SA, which
for exactly this is granted a narrow `configmaps: [create, get, patch]` in
`01-rbac.yaml` (its only Kubernetes API access). Slot/publication are plan-level
(one per run), so the orchestrator passes them into the shard via
`SLOT_NAME`/`PUBLICATION_NAME` env (from `plan.json`'s top-level
`slot.name`/`publication.name`), NOT the per-shard entry.

`orchestrate` (`pg_cyan/orchestrator.py`, `PgCyanOrchestrator`) is the
**migration DAG orchestrator** (issue #40) -- the `deploy/k8s/` manifests are
the nodes, this is the edges. Design decision for the ticket's open question
(custom Python vs. Argo Workflows): **custom Python**, using the official
`kubernetes` client, because `01-rbac.yaml` already scopes a plain
ServiceAccount for exactly this (Job/Deployment/ConfigMap verbs, no CRDs), and
every other stage is already this same `PgCyan<Stage>` + `_log_event` pattern
-- see the rationale in the module docstring for the full argument. It walks
the phases in order -- planner(-1) → pre-data(0) → data
shards(2, ×N) → index(3a, ×M) → FK(3b) → follow-CDC(4) -- applying each
phase's manifest(s) (substituting the `${...}` placeholders in the
`*.template.yaml` files once per `plan.json` entry for the templated phases)
and **blocking on that phase's barrier**: Job phases wait for every Job to
report `Complete`, the follow-CDC Deployment waits for its ready-replica count
since it runs forever by design. **There is no snapshot-holder phase** -- under
the fold (#70) the `run-migration` controller holds the snapshot in-pod and
passes it into the orchestrator as `snapshot_id` (injected as `SNAPSHOT_ID` into
the `needs_snapshot` phases 0/2, replacing any manifest env), `owner_reference`
(its StatefulSet, stamped on every created workload so a `kubectl delete
statefulset` cascade-deletes the migration), and a `health_check` the barrier
loop polls so a dropped snapshot aborts the walk instead of "succeeding" against
a dead one. When those are unset (a standalone `orchestrate`), none of that
happens.
`run()` never raises and **fails fast** -- the first phase whose barrier does
not resolve to success stops the walk; later phases are never applied.
`plan.json` is read from the `pgcyan-plan` ConfigMap via the API right before
phase 2 needs it (not mounted as a volume, since that ConfigMap does not exist
until the phase-(-1) planner Job creates it). After the **phase-2 barrier**
clears, the orchestrator **aggregates the per-shard copy logs** (issue #25): it
lists the `pgcyan-copylog-<shard>` ConfigMaps each shard published (by the
`pgcyan.dev/copylog=shard` label), merges the disjoint per-table counts into one
combined `migration-copy-log.json`, and publishes it to the **`pgcyan-copylog`**
ConfigMap (labelled `copylog=combined` so it is not read back as a shard input).
This is best-effort — a passed COPY barrier is never flipped to failure by an
aggregation hiccup (the copy is the deliverable; the combined log is reporting).
(Running `orchestrate` itself in-cluster, and a `--stop-after` checkpoint knob,
are deferred to **#70**, which folds the snapshot holder into a StatefulSet
orchestrator.) Out of scope for this
ticket (assumed already applied): the namespace/RBAC/PVC/Secrets (00/01/10/20)
and the input-collector phase -2 (`25-job-collect-inputs.yaml`).

`run-migration` (`pg_cyan/controller.py`, `PgCyanController`) is the **folded
snapshot-holder + orchestrator** (issue #70), run as a **single-pod StatefulSet**
(`deploy/k8s/35-statefulset-controller.yaml`, under the `pgcyan-orchestrator` SA).
The old model had a *separate* snapshot-holder Deployment; a finite orchestrator
could never own it (owner-refs would GC the holder mid-copy), so it leaked -- and
a leaked holder is a **held replication slot = unbounded WAL growth on the
source**. The fold resolves that: the controller **holds the consistent snapshot
in its own process** and walks the DAG, so the snapshot's lifetime == the
controller's, which makes owning the children (cascade-delete on death) correct
rather than dangerous -- justified because **a lost snapshot is already fatal**
(the in-flight copy was pinned to an instant that no longer exists → full
re-copy). It **composes** rather than merges: it owns a `PgCyanSnapshot` (the
snapshot sub-component -- a real `pgcopydb snapshot --follow` subprocess, so a
bug in the walk can't drop the connection) and a `PgCyanOrchestrator` (the walk),
both still separately unit-tested. Ordering: it establishes the snapshot **first**
(planning is offline/fast, so holding from the start pins everything to one
instant), then runs the full walk with the id injected + workloads owned + a
`health_check` wired to the child's liveness. Why a **StatefulSet(1)**:
*at-most-one* (a `Deployment(1)` can transiently run two during a rollout -- the
two-holders race, #66) + stable identity; it is explicitly **not** reschedule
protection (a node eviction still kills the pod and the snapshot with it -- fine,
because that loss was always fatal). `run()` never raises and **always releases
the snapshot child** (`finally`), so a graceful pod exit leaves no stale source
connection. **REFUSE-TO-RESUME:** any restart loses the in-pod snapshot, so on
boot the controller **atomically claims the run** by *creating* the
`pgcyan-migration-state` ConfigMap; if it already exists (a prior process began
the run) the controller **refuses** and **parks** -- flips its readiness probe to
not-ready (it manages a `/pgcopydb/ready` file the probe `test -f`s), logs the
halt, and blocks (never exits into a `restartPolicy: Always` crash-loop) until a
human deletes the state ConfigMap + orphaned workloads. The same `park()` holds
the pod in an observable terminal state (Ready iff the migration succeeded) after
a run finishes. **`--stop-after <id>`** (env `PGCYAN_STOP_AFTER`) brings the
pipeline up through a phase then parks Ready -- a checkpoint (e.g. `2` = through
the data copy). The source-side replication-slot reaper (the one leaked resource
owner-ref cascade can't reach -- it needs a source DB connection) is **specified**
as `deploy/k8s/90-cronjob-slot-reaper.example.yaml` (suspended; relates to
cutover #29). The standalone `snapshot-hold` command + `50-deployment-snapshot-
holder.yaml` are **retired**, and the cluster integration test is a single
`migration` job in `.github/workflows/integration-tests.yml` that runs
`run-migration` with `PGCYAN_STOP_AFTER=2` -- the whole implemented DAG end-to-end
via the folded controller -- and asserts both the combined `pgcyan-copylog`
ConfigMap (#25's AC) and the data-only target schema shape. This has been **run
green on the live AKS cluster** (7 tables / ~4.8M rows across 4 disjoint shards);
getting there took three real fixups the DB-free tests structurally could not
catch -- a job-collision race (fixed by the single job), missing `jobs/status` /
`deployments/status` RBAC subresources (the least-privilege-SA gap), and an
invalid `--restart=false` on `copy table-data`. Bump `PGCYAN_STOP_AFTER` as
phases 3/4 land.

## Configuration & secrets

- Config is **environment-driven** (`PG_SOURCE_*`, `LOG_LEVEL`,
  `PREFLIGHT_TIMEOUT_SECONDS`). Add new config the same way and document it in
  the README table.
- **Never** commit secrets. No passwords in code, tests, or `*.tfvars`. For
  Azure, prefer `azure-identity` managed identity / `DefaultAzureCredential`
  over static credentials.

## Tests

- Framework: **pytest** (+ `pytest-cov`). Run: `python -m pytest`.
- Config and coverage settings live in `pyproject.toml`.
- Keep the default suite **DB-free**: exercise guard paths, JSON event shapes,
  and CLI wiring (stub `PgCyanPreflight`). Anything needing a live server goes
  in `tests/integration/`, marked `@pytest.mark.integration` (or module-level
  `pytestmark`) so `addopts = "... -m \"not integration\""` keeps it out of
  the default run.
- Add/extend a test for every new check or CLI change before considering it
  done.
- `tests/integration/test_pre_migration_integration.py` (issue #19, extended
  for issue #4) is the **one file** for manual integration tests of the
  pre-migration artifact stages -- add new stages as another test function
  here rather than a new integration test file. Each test runs the real
  orchestrator against a live server (connection settings from `PG_SOURCE_*`
  env vars, same as `collect_metrics_from_env` /
  `export_parameters_from_env`) and asserts only the *shape* of the output --
  every documented field is present -- never specific values. **No workflow
  currently drives this pytest file** (the old `integration-pre-migration.yml`
  was retired when the workflows were consolidated); run it manually with
  `PG_SOURCE_*` set, or rely on the cluster-side phase -2 collector Job that
  `.github/workflows/integration-tests.yml` exercises for CI coverage of a real
  source DB. Keep the file's conventions (env-var config, secrets only for
  credentials, field-presence-only assertions) for future stages.

## CI / workflows

Exactly **four** GitHub workflows, each with one clear job (issue #50 cleanup):

- `.github/workflows/unit-tests.yml` -- the DB-free pytest suite, on push/PR.
- `.github/workflows/build.yml` -- builds the image and (on push to main /
  manual dispatch) publishes it to the ACR tagged by the **commit short SHA**
  only (no `latest`, no version tag). No deploy step.
- `.github/workflows/integration-tests.yml` -- manual (`workflow_dispatch`)
  cluster-backed tests: a `planner` job (phase -2 -> -1) and a `controller` job
  (#70) that stands up the folded controller StatefulSet with
  `PGCYAN_STOP_AFTER=0`, so it holds the snapshot in-pod and walks planner ->
  pre-data (injecting the id, no `pgcyan-snapshot` ConfigMap), then asserts the
  real target ends up with tables but no secondary indexes / FKs / triggers
  (needs `PG_TARGET_*`). Teardown deletes the StatefulSet -> owner-ref cascade.
  Bumping `PGCYAN_STOP_AFTER` to `2` (through the data copy, asserting
  `pgcyan-copylog`) is the follow-up. The phase-2 wrapper + ConfigMap aggregation
  stay covered by the DB-free `tests/test_datashard.py` / `test_orchestrator.py`.
- `.github/workflows/terraform-aks.yml` -- `terraform fmt`/`validate` for the
  AKS module.

**Image reference convention:** the `deploy/k8s` manifests carry
`image: "${PGCYAN_IMAGE}"`, NOT a concrete image. It is filled at apply time:
`integration-tests.yml` substitutes `<acr>.azurecr.io/pgcyan:<short-sha>` with
`envsubst '${PGCYAN_IMAGE}'` (which leaves runtime `$(SNAPSHOT_ID)` /
`${RESTORE_JOBS}` refs untouched), and the orchestrator substitutes its OWN
running image (resolved from its pod spec via `_resolve_image`) so every phase
Job/Deployment it launches shares its exact build. Never commit a concrete
image or digest to the manifests or `kustomization.yaml`.

## Style

- Python 3.10+, type hints on public methods, docstrings in the existing style
  (Args/Returns). Match the surrounding code — don't reformat unrelated lines.
- Prefer standard library + the already-pinned deps; discuss before adding a
  new dependency.

## Terraform

Two independent root modules, each with its own **remote** state (separate blobs
in one shared Azure Blob backend; see below):

- `terraform/` — the migration **worker VM** (resource group, VNet, subnet,
  NSG, public IP, VM, cloud-init). Restrict `allowed_ssh_cidr`.
- `terraform/aks/` — the **AKS cluster** that runs the `deploy/k8s` sharded
  pgcopydb migration (issue #34). Enables **OIDC issuer + Workload Identity**,
  creates the two user-assigned managed identities + federated credentials for
  the `pgcyan-orchestrator`/`pgcyan-worker` ServiceAccounts, an ACR (AcrPull to
  the kubelet identity), Log Analytics/Container Insights, and Azure CNI
  Overlay networking. Post-apply, its outputs feed the
  `azure.workload.identity/client-id` SA annotations in `deploy/k8s/01-rbac.yaml`
  (see `terraform/aks/README.md`). Managed-identity → Postgres login must be
  granted DB-side out of band.

Both modules are meant to be idempotent — `terraform apply` should be safe to
re-run. State lives in a shared Azure Blob backend, one blob per module
(`worker-vm.tfstate` / `aks.tfstate`), bootstrapped once by
`terraform/bootstrap-backend.sh` (idempotent; storage account name derived from
the subscription id). Run it, then `terraform init -backend-config=backend.hcl`
in each module. This is what keeps an ephemeral dev container from orphaning
Azure resources by losing local state. Keep state/secrets out of the repo
(`*.tfstate`, `*.tfvars`, `backend.hcl` are gitignored; commit only the
`*.example` files).

## Before wiring up a new migration phase (#25–#29)

Read **`notes/migration-phases-lessons-learned.md`** first. It captures the
non-obvious failures hit getting phase 0 (`restore-predata`, #24) green on
AKS + Azure Postgres — non-root pod + managed-disk PVC permissions
(`fsGroup`), Azure extension allow-listing (`--skip-extensions`), the
image-pinned-by-commit-SHA CI flow (build then test, or retag), the
manifest-applied-live vs image-baked deploy layers, shared pgcopydb `--dir`
snapshot consistency, the `clone`-does-schema data-only check for #25, and
holder/ConfigMap CI-job serialization. Most of these recur verbatim in the
data-shard (#25), CDC (#26), and post-data (#28) phases.

## Known gaps / cleanups worth doing

- `pg_cyan/cloud/azure/` and `pg_cyan/postgres/` are empty skeletons — the
  Azure-side pre-migration checks (HA, geo-backup, networking, storage, SKU)
  and shared PG introspection helpers will land here.
- Migration and post-migration stages are unimplemented.
