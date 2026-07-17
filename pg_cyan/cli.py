"""
pg_cyan command-line interface.

Thin click wrapper over the stage orchestrators (currently `PgCyanPreflight`).
Every command reads connection settings from CLI options first, falling back to
the ``PG_SOURCE_*`` environment variables so the same invocation works locally
and inside the Docker image (see the repo Dockerfile ENTRYPOINT).

Exit codes mirror the underlying orchestrators:
    0  ready / success
    1  not ready / failure
    2  interrupted
"""
import json
import os
import sys

import click

from pg_cyan import __version__
from pg_cyan.parameters import PgCyanParameters
from pg_cyan.preflight import PgCyanPreflight


@click.group()
@click.version_option(__version__, prog_name="pg_cyan")
def cli() -> None:
    """pg_cyan: PostgreSQL side-by-side migration orchestrator for Azure."""


@cli.command()
@click.option("--host", default=lambda: os.getenv("PG_SOURCE_HOST", "localhost"),
              show_default="$PG_SOURCE_HOST", help="Source PostgreSQL host.")
@click.option("--port", type=int, default=lambda: int(os.getenv("PG_SOURCE_PORT", "5432")),
              show_default="$PG_SOURCE_PORT", help="Source PostgreSQL port.")
@click.option("--user", default=lambda: os.getenv("PG_SOURCE_USER", "postgres"),
              show_default="$PG_SOURCE_USER", help="Source database user.")
@click.option("--password", default=lambda: os.getenv("PG_SOURCE_PASSWORD", ""),
              help="Source database password (prefer $PG_SOURCE_PASSWORD).")
@click.option("--database", default=lambda: os.getenv("PG_SOURCE_DATABASE", "postgres"),
              show_default="$PG_SOURCE_DATABASE", help="Source database name.")
@click.option("--timeout", type=int,
              default=lambda: int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
              show_default="$PREFLIGHT_TIMEOUT_SECONDS", help="Connection timeout (seconds).")
def preflight(host: str, port: int, user: str, password: str,
              database: str, timeout: int) -> None:
    """Run pre-migration readiness checks against the source database."""
    checker = PgCyanPreflight(
        host=host, port=port, user=user, password=password,
        database=database, timeout=timeout,
    )
    try:
        results = checker.run_all_checks()
        click.echo(json.dumps(results, indent=2, default=str))
        checker.close()
        sys.exit(0 if results["summary"]["ready_for_migration"] else 1)
    except KeyboardInterrupt:
        checker.close()
        sys.exit(2)


@cli.command(name="source-parameters")
@click.option("--host", default=lambda: os.getenv("PG_SOURCE_HOST", "localhost"),
              show_default="$PG_SOURCE_HOST", help="Source PostgreSQL host.")
@click.option("--port", type=int, default=lambda: int(os.getenv("PG_SOURCE_PORT", "5432")),
              show_default="$PG_SOURCE_PORT", help="Source PostgreSQL port.")
@click.option("--user", default=lambda: os.getenv("PG_SOURCE_USER", "postgres"),
              show_default="$PG_SOURCE_USER", help="Source database user.")
@click.option("--password", default=lambda: os.getenv("PG_SOURCE_PASSWORD", ""),
              help="Source database password (prefer $PG_SOURCE_PASSWORD).")
@click.option("--database", default=lambda: os.getenv("PG_SOURCE_DATABASE", "postgres"),
              show_default="$PG_SOURCE_DATABASE", help="Source database name.")
@click.option("--timeout", type=int,
              default=lambda: int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
              show_default="$PREFLIGHT_TIMEOUT_SECONDS", help="Connection timeout (seconds).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR",
              help="Directory for pg-source-parameters[-errors].json.")
def source_parameters(host: str, port: int, user: str, password: str,
                      database: str, timeout: int, output_dir: str) -> None:
    """Export all instance-level parameters of the source database to JSON.

    Writes ``pg-source-parameters.json`` on success, or
    ``pg-source-parameters-errors.json`` with an error message on failure.
    """
    exporter = PgCyanParameters(
        host=host, port=port, user=user, password=password,
        database=database, timeout=timeout, output_dir=output_dir,
    )
    try:
        summary = exporter.export()
        exporter.close()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        exporter.close()
        sys.exit(2)


if __name__ == "__main__":
    cli()
