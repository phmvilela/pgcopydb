# CLAUDE.md

Guidance for AI agents (and humans) working in this repo. Read this before
making changes. Keep it up to date when conventions change.

## What this project is

`pg_cyan` orchestrates a **side-by-side PostgreSQL migration to Azure** with
minimal downtime, across three stages: **pre-migration → migration →
post-migration**. Deliverables are a mix of Python (checks/orchestration) and
Terraform (Azure provisioning). See `README.md` for the user-facing view.

Current state: the **pre-migration** stages (`preflight`, `source-parameters`,
`source-pg-metrics`, `source-largeobjects`, `plan`), the **schema** step
(`schema`), the **copy** step (`copy`), the **migration DAG
orchestrator** (`orchestrate`), and the
**folded controller** (`run-migration`, #70) that holds the consistent snapshot
in-pod and drives the whole `deploy/k8s/` sharded pgcopydb DAG, plus the
Terraform **worker VM** / **AKS cluster** modules exist. Post-migration is not
built yet.

## Where work comes from

- The backlog lives in **GitHub issues** on `berenguel/pgcyan`
  (`gh issue list`). Drive work from there.

## Architecture & the authoritative pattern

The **code is the source of truth** for design. `pg_cyan/plan/preflight.py` and its
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

| Command | Module / class | Step | What it is |
|---|---|---|---|
| `preflight` | `preflight.py` · `PgCyanPreflight` | pre | Source readiness checks; the **canonical pattern** (pass/fail summary). |
| `source-parameters` | `parameters.py` · `PgCyanParameters` | pre | Instance-parameter inventory; template for **artifact-producing** stages. |
| `source-pg-metrics` | `metrics.py` · `PgCyanMetrics` | pre | Object counts + **heap/index sizes** (feeds the planner's shard sizing). |
| `source-largeobjects` | `lobs.py` · `PgCyanLobs` | pre | Large-object count (a count of 0 is a valid happy path; gates CDC). |
| `plan` | `planner.py` · `PgCyanPlanner` | plan | Offline transform: LPT bin-pack tables into N disjoint copy shards + `shard-<n>.filters`; **LOB abort gate**. |
| `schema` | `predata.py` · `PgCyanPreData` | schema | Pre-data schema to target (tables/sequences/functions — **no** indexes/PK/FK/triggers; those are post-data #28). |
| `copy` | `datashard.py` · `PgCyanDataShard` | copy | One disjoint shard's **data-only** copy (`pgcopydb copy table-data`); per-table row counts → `pgcyan-copylog-<shard>` ConfigMap. |
| `sequences` | `sequences.py` · `PgCyanSequences` | sequences | **Custom SQL**: source `pg_sequences` → target `setval` — the COPY load leaves target identity/serial sequences at their start, so CDC/post-cutover inserts collide on id=1 (#78). |
| `indexes` | `postindex.py` · `PgCyanPostIndex` | indexes | One shard's secondary indexes **+ PK/unique constraints** — **custom SQL** (`pg_get_indexdef`/`pg_get_constraintdef` → target DDL); PK/unique was #24's descope. |
| `foreign-keys` | `postfk.py` · `PgCyanForeignKeys` | foreign-keys | **Custom SQL** (not pgcopydb): source FKs → target `ADD … NOT VALID` then `VALIDATE`; single pass; writes `target-pg-postload-log.json`. |
| *(controller-owned)* | `follow.py` · `PgCyanFollow` | CDC | **Not a CLI stage.** After the walk, the controller releases its snapshot and *becomes* the CDC consumer — `pgcopydb follow --resume` on its OWN `--dir` (so follow reuses the slot; slot-creator + streamer must share a `--dir`). Gated by `PGCYAN_FOLLOW_CDC`. Option A, #26. |
| `orchestrate` | `orchestrator.py` · `PgCyanOrchestrator` | — | Walks the DAG, applies each step manifest, blocks on its barrier; aggregates the combined `pgcyan-copylog`. |
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
orchestrator): `plan` → `schema` → `copy (×N)` →
`sequences` → `indexes (×M)` → `foreign-keys`. The **controller** (`run-migration`, a single-pod
StatefulSet) holds the consistent snapshot in its own pod for the whole copy and
**injects `SNAPSHOT_ID`** into the step pods (no `pgcyan-snapshot` ConfigMap),
**owns** every workload it creates (delete it → cascade), and **refuses to resume**
a lost-snapshot restart (parks not-ready). **CDC is not a DAG step**: after the
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
  `PG_SOURCE_*` set, or rely on the cluster-side the collect step collector Job that
  `.github/workflows/integration-tests.yml` exercises for CI coverage of a real
  source DB. Keep the file's conventions (env-var config, secrets only for
  credentials, field-presence-only assertions) for future stages.

## CI / workflows

Exactly **five** GitHub workflows, each with one clear job (issue #50 cleanup, +#91):

- `.github/workflows/unit-tests.yml` -- the DB-free pytest suite, on push/PR.
- `.github/workflows/build.yml` -- builds the image and (on push to main /
  manual dispatch) publishes it to the ACR tagged by the **commit short SHA**
  only (no `latest`, no version tag). No deploy step.
- `.github/workflows/integration-tests.yml` -- manual (`workflow_dispatch`),
  cluster-backed. **One `migration` job** stands up the folded controller
  (`run-migration`, `PGCYAN_FOLLOW_CDC=true`) and walks the whole implemented DAG
  end-to-end (plan → schema → copy → indexes → foreign-keys), then the controller
  streams CDC on its own `--dir` (#26 Option A). Asserts the combined
  `pgcyan-copylog` ConfigMap (#25), the post-data target shape (indexes + PK/unique
  + validated FKs, #28), and one active consumer on the slot (#26). Needs
  `PG_TARGET_*` + a source with tables (and FKs). **To run or debug it, use the
  `run-integration-test` skill** (`.claude/skills/`) -- dispatch/watch/artifact-log
  flow + known failure modes.
- `.github/workflows/deploy-control-plane.yml` -- manual (`workflow_dispatch`),
  cluster-backed. Builds the **control-plane** API image (repo-root context,
  `control-plane/api/Dockerfile`) tagged by the commit short SHA, pushes it to the
  ACR, then applies `deploy/control-plane` to the fixed `pgcyan-system` namespace,
  filling `${PGCYAN_CONTROL_PLANE_IMAGE}` (this build) and `${PGCYAN_MIGRATION_IMAGE}`
  (the `pgcyan:<tag>` the API stamps into provisioned controllers, #87) via
  `envsubst`; then rollout + `GET /health`. The repeatable form of #86's manual
  apply (#91).
- `.github/workflows/terraform-aks.yml` -- `terraform fmt`/`validate` for the
  AKS module.

**Image reference convention:** the `deploy/k8s` manifests carry
`image: "${PGCYAN_IMAGE}"`, NOT a concrete image. It is filled at apply time:
`integration-tests.yml` substitutes `<acr>.azurecr.io/pgcyan:<short-sha>` with
`envsubst '${PGCYAN_IMAGE}'` (which leaves runtime `$(SNAPSHOT_ID)` /
`${RESTORE_JOBS}` refs untouched), and the orchestrator substitutes its OWN
running image (resolved from its pod spec via `_resolve_image`) so every step
Job/Deployment it launches shares its exact build. Never commit a concrete
image or digest to the manifests or `kustomization.yaml`.

## Control plane (issue #84 book of work)

A separate, long-lived service that provisions and drives **per-migration**
sandboxes on demand — distinct from a single migration run. Two new trees:

- **`control-plane/api/`** — a standalone **FastAPI** service (its own
  `requirements.txt` + `Dockerfile`, image `pgcyan-control-plane`), NOT part of
  the `pg_cyan` package or the heavy migration image. `GET /health` (#86);
  `POST /migrations` (#87, provisions a per-migration namespace with the
  controller parked in wait-to-start) + `GET /migrations` (#88, lists tracked
  migrations) + `POST/GET /migrations/{id}/start|status` (#89, proxy the
  controller's `/start` and aggregate its `/status`, reached over the migration
  namespace's headless `pgcyan-controller` Service via stdlib `urllib` —
  `app/controller_client.py`) + `POST /migrations/{id}/done` (#90, teardown:
  scale the controller to 0 for a graceful SIGTERM stop of CDC, wait, delete the
  namespace, mark the registry `done` — `app/teardown.py`; the *no-SIGKILL*
  guarantee rides on #81/#83's reconciled shutdown window). A **node-local SQLite
  registry** on the RWO PVC (`app/registry.py`) tracks migrations and enforces
  **one active control-plane migration at a time** (409) via a partial unique
  index — the DB, not app-level check-then-act, picks the race winner, and a
  terminal state frees the slot so a crash can't wedge it; a best-effort,
  injectable **source probe** (`app/source_probe.py`, psycopg2) rejects a source
  already carrying the pgcyan slot/publication (409). The registry stores only
  `host:port/db` labels — **never credentials** (those live in the k8s Secret).
  Its tests live under `control-plane/api/tests/` and are **not** collected by
  the repo-root `pytest` (run them from `control-plane/api`). **Built from the
  repo-root context** (like the migration image) so it can bake in `deploy/k8s`.
  Both images are split into a rarely-rebuilt base (`Dockerfile.base`: heavy deps
  — the migration one compiles pgcopydb, the control-plane one is python + pip
  deps) and a thin app `Dockerfile` that does `FROM` the hash-tagged base, so a
  normal commit skips the compile/pip install. The provisioner reads those
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
- **Comments:** follow the `comments` skill (`.claude/skills/`) — no historical
  or "changed X" notes, no issue/PR numbers, keep them plain and sparse, use
  "see also" links to related files.
- **Commits:** follow the `commit` skill (`.claude/skills/`) — ≤600 chars, no
  trailers, reference the main GitHub issue only.

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

## Before wiring up a new migration step (#25–#29)

Read **`notes/migration-steps-lessons-learned.md`** first. It captures the
non-obvious failures hit getting the schema step (`schema`, #24) green on
AKS + Azure Postgres — non-root pod + managed-disk PVC permissions
(`fsGroup`), Azure extension allow-listing (`--skip-extensions`), the
image-pinned-by-commit-SHA CI flow (build then test, or retag), the
manifest-applied-live vs image-baked deploy layers, shared pgcopydb `--dir`
snapshot consistency, the `clone`-does-schema data-only check for #25, and
holder/ConfigMap CI-job serialization. Most of these recur verbatim in the
data-shard (#25), CDC (#26), and post-data (#28) steps.

## Known gaps / cleanups worth doing

- `pg_cyan/postgres/` is still an empty skeleton for shared PG introspection
  helpers. `pg_cyan/cloud/azure/` is no longer empty — it holds
  `postgres_flexible_server.py` (#154: Flexible Server storage/SKU + the
  `storage_used` Monitor metric, behind `source-capacity-azure`) — but the
  other Azure-side pre-migration checks (HA, geo-backup, networking) are
  still unbuilt.
- Migration and post-migration stages are unimplemented.
