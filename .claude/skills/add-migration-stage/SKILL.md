---
name: add-migration-stage
description: Use when adding a new pg_cyan stage or migration step — a new PgCyan<Stage> class, a CLI subcommand, and (for a migration step) a deploy/k8s manifest node. Covers the authoritative orchestrator pattern, the success-vs-error two-file output, structured logging, DB-free tests, and the RBAC/injection wiring a new step needs under the folded controller.
---

# Add a pg_cyan stage / migration step

`pg_cyan/preflight.py` (`PgCyanPreflight`) is the canonical shape; every stage is
a `PgCyan<Stage>` class. **Follow the existing code — the module docstrings are
the source of truth.** Pick the closest sibling to copy: a **collector** that
talks to a DB (`metrics.py`, `parameters.py`, `lobs.py`), a **pgcopydb wrapper**
that drives subprocesses (`predata.py`, `datashard.py`, `snapshot.py`), or an
**offline transform** (`planner.py`).

## The pattern (non-negotiable conventions)

- **One class per stage**, connection/state on the instance (`self.conn`,
  `self.errors`, counters). Each unit of work is a method returning `bool`,
  aggregated by a single `run()` / `run_all_checks()` / `export()` that returns
  one combined dict.
- **`run()` NEVER raises.** On any error it logs a `failure` event, records it,
  and returns a failure summary. Connection failures short-circuit.
- **Structured logging** via `_log_event(event_type, status, message, details)` —
  `status` ∈ `success`/`warning`/`failure`; put a `remediation` hint in `details`
  on failures. Copy the method verbatim from a sibling.
- **Success-vs-error two-file output** (artifact stages): write a non-empty
  `<name>.json` on success, or `<name>-errors.json` carrying the error message on
  ANY failure (connection, query, empty result, write), both into `--output-dir`
  / `$PG_OUTPUT_DIR`. Keep this shape.
- **`psycopg2.sql` composition** for any interpolated identifier/literal — never
  f-string config/user values into SQL.
- **A warning is not a blocker** — increment the pass counter for warnings; only
  real problems flip readiness / fail.

## the snapshot step. **New stage class** `pg_cyan/<stage>.py` — copy the closest sibling; keep
   `_log_event`, the two-file output, and the never-raise `run()`.
2. **CLI subcommand** in `pg_cyan/cli.py` — a thin click command mirroring an
   existing one. Every option gets an env fallback
   (`default=lambda: os.getenv("PGCYAN_...")`); convert click's empty-string
   defaults to `None` with `x or None` so the stage's guards fire. Exit `0`
   success / `1` failure / `2` `KeyboardInterrupt` — keep these stable.
3. **DB-free unit tests** `tests/test_<stage>.py` — the default suite is DB-free
   (`addopts` excludes `-m integration`). Stub the DB / subprocess / kubectl
   (see `test_datashard.py` for the subprocess+psycopg2 fakes, `test_predata.py`
   for the subprocess fake). Cover: the guard paths, the command/query build, the
   `run()` happy path, and each failure branch writes the error file. Add a CLI
   wiring test in `test_cli.py` (a stub class recording kwargs).
4. **Update the CLAUDE.md stage index** (`## CLI & stages`) — one table row. The
   deep prose stays in your module docstring, NOT in CLAUDE.md.

## If it's a MIGRATION STEP (a deploy/k8s node)

Read `notes/migration-steps-lessons-learned.md` first — the non-obvious AKS
failures recur across steps. Then:

- **Manifest** `deploy/k8s/NN-...yaml` (Job, or `*.template.yaml` if per-shard).
  Non-root pods that mount a managed-disk PVC need the `securityContext`
  (`runAsUser: 1000`, `fsGroup: 1000`, `fsGroupChangePolicy: OnRootMismatch`) —
  copy it. Image is `image: "${PGCYAN_IMAGE}"`, never a concrete tag. Add it to
  `kustomization.yaml`.
- **Snapshot** (if the step pins the copy): do NOT read a `pgcyan-snapshot`
  ConfigMap — the controller (#70) INJECTS `SNAPSHOT_ID` into the pod env. Mark
  the step `needs_snapshot: True` in the orchestrator's `STEPS`.
- **Orchestrator wiring** (`orchestrator.py`): add a `STEPS` entry (static
  `manifest` or templated `template`+`plan_key`+`substitutions`). Templated
  steps are stamped once per `plan.json` entry.
- **RBAC** (`01-rbac.yaml`): if the orchestrator makes a NEW kind of API call for
  this step, grant it. Reads of a `*/status` subresource need it explicitly
  (`jobs/status`, `deployments/status`) — a grant on the parent is NOT enough
  (lessons-learned #9). Worker pods that write a ConfigMap need the narrow
  `configmaps` grant (as the data shards do).
- **Integration**: the step runs inside the controller walk — bump
  `PGCYAN_STOP_AFTER` in `.github/workflows/integration-tests.yml` to include it,
  and add an assertion. Use the `run-integration-test` skill to run/debug it.

## Before you call it done

- `python -m pytest` green (DB-free).
- `python -m pg_cyan.cli <stage> --help` shows the command.
- New config documented in the README env-var table.
- The module docstring fully explains the stage (it's the source of truth); the
  CLAUDE.md index row points to it.
