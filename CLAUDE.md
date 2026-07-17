# CLAUDE.md

Guidance for AI agents (and humans) working in this repo. Read this before
making changes. Keep it up to date when conventions change.

## What this project is

`pg_cyan` orchestrates a **side-by-side PostgreSQL migration to Azure** with
minimal downtime, across three stages: **pre-migration → migration →
post-migration**. Deliverables are a mix of Python (checks/orchestration) and
Terraform (Azure provisioning). See `README.md` for the user-facing view.

Current state: the **pre-migration preflight** checks, the **source parameter
inventory** (`source-parameters`), the **source metrics collector**
(`source-pg-metrics`), and the Terraform **worker VM** exist. Migration and
post-migration are not built yet.

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
`preflight`, `source-parameters`, and `source-pg-metrics` are implemented.
Options default to `PG_SOURCE_*` env vars so the same invocation works locally
and in the container. Exit codes: `0` ready/success, `1` not ready/failure,
`2` interrupted — keep these stable.

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
triggers, extensions), database size, and per-table sizes. On success it
writes a non-empty `source-pg-metrics.json`; on any failure (connection,
query, empty result, or write) it writes `source-pg-metrics-errors.json`
with the error message (issue #3). Object counts rely on
`pg_proc.prokind`, which requires PostgreSQL 11+.

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
  in a clearly separated integration suite, not the default run.
- Add/extend a test for every new check or CLI change before considering it
  done.

## Style

- Python 3.10+, type hints on public methods, docstrings in the existing style
  (Args/Returns). Match the surrounding code — don't reformat unrelated lines.
- Prefer standard library + the already-pinned deps; discuss before adding a
  new dependency.

## Terraform

Lives in `terraform/` (resource group, VNet, subnet, NSG, public IP, VM,
cloud-init). It provisions the migration **worker VM**. It is meant to be
idempotent — `terraform apply` should be safe to re-run. Restrict
`allowed_ssh_cidr` and keep state/secrets out of the repo.

## Known gaps / cleanups worth doing

- `pg_cyan/cloud/azure/` and `pg_cyan/postgres/` are empty skeletons — the
  Azure-side pre-migration checks (HA, geo-backup, networking, storage, SKU)
  and shared PG introspection helpers will land here.
- Migration and post-migration stages are unimplemented.
