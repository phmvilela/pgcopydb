# pg_cyan

**PostgreSQL side-by-side migration orchestrator for Azure.**

`pg_cyan` supports migrating an existing PostgreSQL environment to a new
PostgreSQL environment in Azure with minimal downtime. It is organized around
three stages:

1. **Pre-migration** — readiness checks and inventory of the source (versions,
   parameters, extensions, primary keys, HA/geo-backup/networking/storage, etc.).
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

### Configuration (environment variables)

| Variable                     | Default     | Purpose                          |
|------------------------------|-------------|----------------------------------|
| `PG_SOURCE_HOST`             | `localhost` | Source host                      |
| `PG_SOURCE_PORT`             | `5432`      | Source port                      |
| `PG_SOURCE_USER`             | `postgres`  | Source user                      |
| `PG_SOURCE_PASSWORD`         | *(empty)*   | Source password                  |
| `PG_SOURCE_DATABASE`         | `postgres`  | Source database                  |
| `PREFLIGHT_TIMEOUT_SECONDS`  | `30`        | Connection timeout               |
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
integration suite.

## Development & work tracking

- Work is tracked as **GitHub issues** on
  [`berenguel/pgcyan`](https://github.com/berenguel/pgcyan/issues).
- See [`CLAUDE.md`](CLAUDE.md) for architecture, conventions, and the pattern
  for adding a new pre-migration check.
