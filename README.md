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
  cloud/azure/         Azure SDK integrations (skeleton)
  postgres/            PostgreSQL connection/introspection utils (skeleton)
terraform/             Azure worker-VM provisioning (VNet, NSG, VM, cloud-init)
tests/                 pytest suite (DB-free unit tests)
Dockerfile             Multi-stage, non-root image; entrypoint = the CLI
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
