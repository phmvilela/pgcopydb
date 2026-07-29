# pg_cyan

**PostgreSQL side-by-side migration orchestrator for Azure.**

`pg_cyan` supports migrating an existing PostgreSQL environment to a new
PostgreSQL environment in Azure with minimal downtime. It is organized around
three stages:

1. **Pre-migration** — readiness checks, inventory, and metrics for the source
   (versions, parameters, extensions, primary keys, object counts, database
   and table sizes, HA/geo-backup/networking/storage, etc.).
2. **Migration** — the cutover itself *(planned)*.
3. **Post-migration** — validation and cleanup *(planned)*.

Output is a mix of **Python** (checks, orchestration) and **Terraform**
(Azure resource provisioning), since the migration needs infrastructure it can
stand up and tear down.

> Status: early (v0.1.0). The pre-migration **preflight** checks and the Azure
> **worker VM** Terraform template are the first pieces in place.

---

## Repository layout

```
pg_cyan/
  __init__.py          Package metadata (__version__)
  cli.py               click CLI — `python -m pg_cyan.cli`
  preflight.py         PgCyanPreflight: pre-migration readiness checks
  parameters.py        PgCyanParameters: export source instance parameters
  metrics.py           PgCyanMetrics: collect source database metrics
  lobs.py              PgCyanLobs: count source large objects
  planner.py           PgCyanPlanner: bin-pack shards + emit the migration plan
  snapshot.py          PgCyanSnapshot: establish + hold the consistent snapshot
  orchestrator.py      PgCyanOrchestrator: walk the deploy/k8s migration DAG
  cloud/azure/         Azure SDK integrations (skeleton)
  postgres/            PostgreSQL connection/introspection utils (skeleton)
terraform/             Azure worker-VM provisioning (VNet, NSG, VM, cloud-init)
tests/                 pytest suite (DB-free unit tests)
Dockerfile             Non-root image: pgcopydb + kubectl + the CLI (default cmd)
requirements.txt       Pinned Python dependencies
```

## Requirements

- Python 3.10+
- PostgreSQL client tools (`psql`) for some workflows
- Azure CLI + Terraform ≥ 1.0 for provisioning
- Access to the **source** PostgreSQL server

## Setup

```bash
pip install -r requirements.txt
```

## Usage

The CLI is the entry point for every stage. Connection settings come from CLI
options, falling back to `PG_SOURCE_*` environment variables (handy in Docker).

```bash
# Show commands
python -m pg_cyan.cli --help

# Run pre-migration readiness checks against the source DB
export PG_SOURCE_HOST=your-source-host
export PG_SOURCE_USER=postgres
export PG_SOURCE_PASSWORD=...           # prefer env over --password
export PG_SOURCE_DATABASE=yourdb
python -m pg_cyan.cli preflight
```

`preflight` prints a JSON summary and sets an exit code:

| Code | Meaning                     |
|------|-----------------------------|
| 0    | Ready for migration         |
| 1    | Not ready / a check failed  |
| 2    | Interrupted                 |

### Export the source instance parameters

```bash
# Dump every instance-level parameter of the source to JSON
python -m pg_cyan.cli source-parameters
```

`source-parameters` connects to the source and writes an inventory of **all
instance-level configuration parameters** (`pg_settings`) — the values the
planner and preflight gates read (`wal_level`, `max_wal_senders`,
`max_replication_slots`, `track_commit_timestamp`, `max_slot_wal_keep_size`, …).

| Outcome | File written                          | Exit |
|---------|---------------------------------------|------|
| Success | `pg-source-parameters.json` (non-empty) | 0    |
| Failure | `pg-source-parameters-errors.json`      | 1    |
| Interrupted | *(none)*                          | 2    |

Files land in `--output-dir` (default `$PG_OUTPUT_DIR`, else the current
directory).

### Collect source database metrics

```bash
# Collect version, object counts, and size metrics from the source
python -m pg_cyan.cli source-pg-metrics
```

`source-pg-metrics` connects to the source and collects metrics useful for
planning the migration pipeline: PostgreSQL version, object counts (tables,
partitioned tables, partitions, views, materialized views, indexes,
sequences, functions, procedures, triggers, extensions), database size, and
per-table sizes. Object counts assume PostgreSQL 11+ (`pg_proc.prokind`).

| Outcome | File written                          | Exit |
|---------|---------------------------------------|------|
| Success | `source-pg-metrics.json` (non-empty)    | 0    |
| Failure | `source-pg-metrics-errors.json`         | 1    |
| Interrupted | *(none)*                          | 2    |

Files land in `--output-dir` (default `$PG_OUTPUT_DIR`, else the current
directory).

### Count source large objects

```bash
# Count the source database's large objects (pg_largeobject_metadata)
python -m pg_cyan.cli source-largeobjects
```

`source-largeobjects` connects to the source and counts the rows in
`pg_largeobject_metadata` — the database's PostgreSQL large objects, which the
migration copies via a separate `pg_dump` path than the per-table `COPY`. The
count is written to `lob_count`. A count of **zero** is a valid
success (an empty database is not a failure).

| Outcome | File written                              | Exit |
|---------|-------------------------------------------|------|
| Success | `source-pg-largeobjects.json` (non-empty) | 0    |
| Failure | `source-pg-largeobjects-errors.json`      | 1    |
| Interrupted | *(none)*                              | 2    |

Files land in `--output-dir` (default `$PG_OUTPUT_DIR`, else the current
directory).

### Plan the sharded migration

```bash
# Bin-pack the source into disjoint copy shards and emit the migration plan
python -m pg_cyan.cli plan --inputs-dir ./artifacts --output-dir ./plan
```

`plan` is a **pure offline transform** — it opens no database connection. It
reads the artifacts already produced by the pre-migration stages from
`--inputs-dir` (`source-pg-metrics.json` and `source-pg-largeobjects.json`
required; `pg-source-parameters.json` / `constraints.json` optional), LPT
bin-packs the tables into disjoint copy shards balanced by **heap bytes**, and
writes `plan.json` plus one `shard-<n>.filters` per shard (pgcopydb `--filters`
format with a disjoint `[include-only-table]` list). This is the phase `-1`
node in `deploy/k8s/`.

Before packing anything it runs a **large-object abort gate**: if the source
has any large objects (`lob_count > 0`), it exits non-zero, writes no plan, and
records why — pgcopydb's logical-replication CDC does not replicate large
objects. Shard count is fixed at 4 for now (clamped to the table count);
FK-graph collection and cross-shard range-splitting of an oversized table are
deferred follow-ups.

| Outcome | File(s) written                                | Exit |
|---------|------------------------------------------------|------|
| Success | `plan.json` + `shard-<n>.filters` (non-empty)  | 0    |
| Failure / LOB abort | `plan-errors.json` (no plan)       | 1    |
| Interrupted | *(none)*                                   | 2    |

Files land in `--output-dir` (default `$PG_OUTPUT_DIR`, else the current
directory).

### Hold the consistent snapshot

```bash
# Establish and HOLD the publication + slot + consistent snapshot on the source
python -m pg_cyan.cli snapshot-hold --work-dir /work/coordinator
```

`snapshot-hold` is the phase `1` **snapshot holder** — the safety-critical node
in `deploy/k8s/50-deployment-snapshot-holder.yaml`. In one coordinated step it
creates the publication `FOR ALL TABLES`, drives `pgcopydb snapshot --follow`
to create the `pgoutput` replication slot and export its **consistent
snapshot**, writes the snapshot id to the shared `/work` volume, and then
**holds the exporting connection open** so every pre-data (#24) and sharded-copy
(#25) pod pins the same instant. A PostgreSQL snapshot is only valid while its
transaction stays open, so this runs as a Deployment (replicas=1 + PDB): if it
dies the in-flight copy must `--restart` from a new snapshot. It does **not**
create or advance a replication origin.

On `SIGTERM`/`SIGINT` it terminates `pgcopydb`, which closes the replication
connection cleanly — a graceful exit leaves no stale source connection behind.

| Outcome | File(s) written                                            | Exit |
|---------|------------------------------------------------------------|------|
| Success | `source-pg-slot.json` + `source-pg-publication.json`, then holds until signalled | 0 |
| Failure | `source-pg-slot-errors.json` / `source-pg-publication-errors.json` | 1 |
| Interrupted | *(partial; child torn down)*                           | 2    |

Outputs land in `--output-dir` (default `$PG_OUTPUT_DIR`, else `--work-dir`), so
on the holder pod they sit next to the snapshot id on the shared `/work` volume.
The connection string comes from pgcopydb's native `PGCOPYDB_SOURCE_PGURI` when
set, otherwise the discrete `PG_SOURCE_*` variables.

### Configuration (environment variables)

| Variable                     | Default     | Purpose                          |
|------------------------------|-------------|----------------------------------|
| `PG_SOURCE_HOST`             | `localhost` | Source host                      |
| `PG_SOURCE_PORT`             | `5432`      | Source port                      |
| `PG_SOURCE_USER`             | `postgres`  | Source user                      |
| `PG_SOURCE_PASSWORD`         | *(empty)*   | Source password                  |
| `PG_SOURCE_DATABASE`         | `postgres`  | Source database                  |
| `PREFLIGHT_TIMEOUT_SECONDS`  | `30`        | Connection timeout               |
| `PG_OUTPUT_DIR`              | `.`         | Output dir for generated JSON    |
| `PG_INPUTS_DIR`              | `.`         | `plan`: dir holding source artifacts |
| `PGCYAN_SHARDS`              | `4`         | `plan`: number of copy shards    |
| `PGCYAN_SPLIT_TABLES_LARGER_THAN` | `50GB` | `plan`: pgcopydb within-job split threshold |
| `PGCYAN_SNAPSHOT_MODE`       | `coordinator-held` | `plan`: snapshot strategy |
| `PGCYAN_PUBLICATION`         | `pgcyan_pub` | `plan` / `snapshot-hold`: publication name |
| `PGCYAN_SLOT`                | `pgcyan_slot` | `plan` / `snapshot-hold`: replication slot name |
| `PGCYAN_RUN_ID`              | *(clock)*   | `plan`: opaque run identifier    |
| `PGCOPYDB_SOURCE_PGURI`      | *(unset)*   | `snapshot-hold`: source libpq URI (else `PG_SOURCE_*`) |
| `PGCYAN_PLUGIN`              | `pgoutput`  | `snapshot-hold`: logical decoding output plugin |
| `PGCYAN_WORK_DIR`            | `/work/coordinator` | `snapshot-hold`: pgcopydb `--dir`; snapshot id written here |
| `PGCYAN_PLAN_FILE`           | *(unset)*   | `snapshot-hold`: optional `plan.json` to source names/mode |
| `PGCOPYDB_TARGET_PGURI`      | *(unset)*   | `restore-predata` / `copy-shard`: target libpq URI |
| `SNAPSHOT_ID`               | *(unset)*   | `restore-predata` / `copy-shard`: holder's snapshot to pin to (from the `pgcyan-snapshot` ConfigMap) |
| `FILTER_FILE`               | *(unset)*   | `copy-shard`: this shard's `shard-<n>.filters` (under `$PGCYAN_PLAN_DIR`) |
| `PGCYAN_COUNT_ROWS`         | `true`      | `copy-shard`: count copied rows on the target (disable for very large tables; see #69) |
| `PGCYAN_COPYLOG_CONFIGMAP`  | *(derived)* | `copy-shard`: per-shard copy-log ConfigMap (defaults to `pgcyan-copylog-<shard>`) |
| `PGCYAN_STOP_AFTER`         | *(unset)*   | `run-migration` / `orchestrate`: bring the DAG up through this phase id (e.g. `2`), then park |
| `PGCYAN_READY_FILE`         | `/pgcopydb/ready` | `run-migration`: readiness-probe file; present == Ready, removed when parked/failed |
| `LOG_LEVEL`                  | `INFO`      | Log verbosity                    |

## Docker

```bash
docker build -t pg_cyan .
docker run --rm \
  -e PG_SOURCE_HOST=... -e PG_SOURCE_USER=... \
  -e PG_SOURCE_PASSWORD=... -e PG_SOURCE_DATABASE=... \
  pg_cyan preflight
```

## Provisioning the Azure worker VM

See [`terraform/README.md`](terraform/README.md). In short:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # edit values
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

## Tests

```bash
python -m pytest
```

The suite is intentionally **DB-free** (guard paths, JSON event shape, CLI
wiring). Tests requiring a live PostgreSQL server belong in a separate
integration suite (`tests/integration/`, marked `integration`, excluded from
the default run).

### Integration tests: pre-migration artifacts

`tests/integration/test_pre_migration_integration.py` runs `source-pg-metrics`
(issue #19), `source-parameters` (issue #4), and `source-largeobjects`
(issue #20) against a real PostgreSQL server and checks the *shape* of each
resulting JSON file — that every
documented field is present — without validating its values. Run it locally
against a source you point at via the usual `PG_SOURCE_*` env vars:

```bash
PG_SOURCE_HOST=... PG_SOURCE_USER=... PG_SOURCE_PASSWORD=... PG_SOURCE_DATABASE=... \
    python -m pytest tests/integration -m integration
```

It also runs manually from GitHub Actions via the **"Integration:
pre-migration artifacts"** workflow (`workflow_dispatch` only — it's never
triggered by push/PR). Connection details come from GitHub, split by
sensitivity: credentials are repo **Secrets**, everything else is a plain
repo **Variable**:

| Name                  | Kind      | Purpose                |
|------------------------|-----------|-------------------------|
| `PG_SOURCE_HOST`      | Variable  | Source host             |
| `PG_SOURCE_PORT`      | Variable  | Source port             |
| `PG_SOURCE_DATABASE`  | Variable  | Source database         |
| `PG_SOURCE_USER`      | Secret    | Source user             |
| `PG_SOURCE_PASSWORD`  | Secret    | Source password         |

The resulting `source-pg-metrics.json` and `pg-source-parameters.json` are
uploaded as a workflow artifact for human review.

## Development & work tracking

- Work is tracked as **GitHub issues** on
  [`berenguel/pgcyan`](https://github.com/berenguel/pgcyan/issues).
- See [`CLAUDE.md`](CLAUDE.md) for architecture, conventions, and the pattern
  for adding a new pre-migration check.
