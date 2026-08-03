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

## CLI & stages

`pg_cyan/cli.py` is a thin **click** wrapper; `python -m pg_cyan.cli` is the entry
point (also the Docker `ENTRYPOINT`). Each stage is a subcommand. Options default
to `PG_SOURCE_*` / `PGCYAN_*` env vars so the same invocation works locally and in
the container. **Exit codes: `0` success, `1` failure, `2` interrupted — keep
these stable.**

**The per-stage deep docs live in the module docstrings — those are the source of
truth** (the code is authoritative; see "Architecture" above). Each stage is a
`PgCyan<Stage>` class following the pattern there. This table is just the index —
read the linked module before changing a stage.

| Command | Module / class | Phase | What it is |
|---|---|---|---|
| `preflight` | `preflight.py` · `PgCyanPreflight` | pre | Source readiness checks; the **canonical pattern** (pass/fail summary). |
| `source-parameters` | `parameters.py` · `PgCyanParameters` | pre | Instance-parameter inventory; template for **artifact-producing** stages. |
| `source-pg-metrics` | `metrics.py` · `PgCyanMetrics` | pre | Object counts + **heap/index sizes** (feeds the planner's shard sizing). |
| `source-largeobjects` | `lobs.py` · `PgCyanLobs` | pre | Large-object count (a count of 0 is a valid happy path; gates CDC). |
| `plan` | `planner.py` · `PgCyanPlanner` | −1 | Offline transform: LPT bin-pack tables into N disjoint copy shards + `shard-<n>.filters`; **LOB abort gate**. |
| `restore-predata` | `predata.py` · `PgCyanPreData` | 0 | Pre-data schema to target (tables/sequences/functions — **no** indexes/PK/FK/triggers; those are post-data #28). |
| `copy-shard` | `datashard.py` · `PgCyanDataShard` | 2 | One disjoint shard's **data-only** copy (`pgcopydb copy table-data`); per-table row counts → `pgcyan-copylog-<shard>` ConfigMap. |
| `copy-sequences` | `sequences.py` · `PgCyanSequences` | 2s | **Custom SQL**: source `pg_sequences` → target `setval` — the COPY load leaves target identity/serial sequences at their start, so CDC/post-cutover inserts collide on id=1 (#78). |
| `build-indexes` | `postindex.py` · `PgCyanPostIndex` | 3a | One shard's secondary indexes **+ PK/unique constraints** — **custom SQL** (`pg_get_indexdef`/`pg_get_constraintdef` → target DDL); PK/unique was #24's descope. |
| `build-foreign-keys` | `postfk.py` · `PgCyanForeignKeys` | 3b | **Custom SQL** (not pgcopydb): source FKs → target `ADD … NOT VALID` then `VALIDATE`; single pass; writes `target-pg-postload-log.json`. |
| *(controller-owned)* | `follow.py` · `PgCyanFollow` | CDC | **Not a CLI stage.** After the walk, the controller releases its snapshot and *becomes* the CDC consumer — `pgcopydb follow --resume` on its OWN `--dir` (so follow reuses the slot; slot-creator + streamer must share a `--dir`). Gated by `PGCYAN_FOLLOW_CDC`. Option A, #26. |
| `orchestrate` | `orchestrator.py` · `PgCyanOrchestrator` | — | Walks the DAG, applies each phase manifest, blocks on its barrier; aggregates the combined `pgcyan-copylog`. |
| `run-migration` | `controller.py` · `PgCyanController` | — | **The folded controller (#70)**: holds the snapshot in-pod and drives the whole DAG. This is what runs in-cluster. With `--wait-for-start`/`PGCYAN_WAIT_FOR_START` (#85) it parks **pre-snapshot** and serves a control API (`GET /status`, `POST /start` on :9999), snapshotting + walking only when `POST /start` arrives — the control-plane (#84) entry point. |
| *(no command)* | `snapshot.py` · `PgCyanSnapshot` | 1 | Snapshot sub-component — establishes + holds the consistent slot/snapshot; driven in-pod by the controller (standalone `snapshot-hold` retired in #70). |

**Shared conventions** (embodied by every stage; details in each docstring):

- **Success-vs-error two-file output.** Artifact stages write a non-empty
  `<name>.json` on success or `<name>-errors.json` on any failure (connection,
  query, empty result, write) into `--output-dir` / `$PG_OUTPUT_DIR`. `run()` /
  `establish()` / `export()` **never raise** — they degrade and record.
- **Structured JSON logging** via `_log_event(event_type, status, message,
  details)`; `status` ∈ `success`/`warning`/`failure`, with a `remediation` hint
  on failures.
- **SQL identifiers/literals** go through `psycopg2.sql` composition — never
  f-string config into SQL.

**The migration DAG** (nodes = `deploy/k8s/` manifests, edges = the
orchestrator): `plan(-1)` → `pre-data(0)` → `data shards(2, ×N)` →
`sequences(2s)` → `index(3a, ×M)` → `FK(3b)`. The **controller** (`run-migration`, a single-pod
StatefulSet) holds the consistent snapshot in its own pod for the whole copy and
**injects `SNAPSHOT_ID`** into the phase pods (no `pgcyan-snapshot` ConfigMap),
**owns** every workload it creates (delete it → cascade), and **refuses to resume**
a lost-snapshot restart (parks not-ready). **CDC is not a DAG phase**: after the
walk the controller *itself* streams `pgcopydb follow` on its own `--dir` (Option
A, #26) — the slot-creator and streamer must share one pgcopydb `--dir`, so a
separate pod can't do it. See `notes/k8s-sharded-migration-topology.md` for the
topology and the module docstrings for each node.

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
- `.github/workflows/integration-tests.yml` -- manual (`workflow_dispatch`),
  cluster-backed. **One `migration` job** stands up the folded controller
  (`run-migration`, `PGCYAN_FOLLOW_CDC=true`) and walks the whole implemented DAG
  end-to-end (planner → pre-data → data shards → index → FK), then the controller
  streams CDC on its own `--dir` (#26 Option A). Asserts the combined
  `pgcyan-copylog` ConfigMap (#25), the post-data target shape (indexes + PK/unique
  + validated FKs, #28), and one active consumer on the slot (#26). Needs
  `PG_TARGET_*` + a source with tables (and FKs). **To run or debug it, use the
  `run-integration-test` skill** (`.claude/skills/`) -- dispatch/watch/artifact-log
  flow + known failure modes.
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

## Control plane (issue #84 book of work)

A separate, long-lived service that provisions and drives **per-migration**
sandboxes on demand — distinct from a single migration run. Two new trees:

- **`control-plane/api/`** — a standalone **FastAPI** service (its own
  `requirements.txt` + `Dockerfile`, image `pgcyan-control-plane`), NOT part of
  the `pg_cyan` package or the heavy migration image. `GET /health` (#86) +
  `POST /migrations` (#87, provisions a per-migration namespace with the
  controller parked in wait-to-start); registry/start-status-teardown are #88–#90.
  Its tests live under `control-plane/api/tests/` and are **not** collected by
  the repo-root `pytest` (run them from `control-plane/api`). **Built from the
  repo-root context** (like the migration image) so it can bake in `deploy/k8s`:
  `docker build -f control-plane/api/Dockerfile .`. The provisioner reads those
  baked manifests and only *retargets the namespace + fills `${PGCYAN_*}` env* at
  apply time — the migration YAML is never re-declared (no drift). The controller
  image it stamps in comes from the `PGCYAN_MIGRATION_IMAGE` env on the API pod.
- **`deploy/control-plane/`** — the fixed **`pgcyan-system`** namespace: SA +
  **cluster-scoped** RBAC (create/delete namespaces and the resources inside
  per-migration namespaces), an RWO SQLite-registry PVC (`/data`, #58 pattern),
  the API Deployment (single replica, `Recreate` — RWO volume) + Service. Same
  image-placeholder convention as `deploy/k8s`: `${PGCYAN_CONTROL_PLANE_IMAGE}`
  filled by `envsubst` at apply time. Applied **manually** for now; the deploy
  workflow is #91.

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
