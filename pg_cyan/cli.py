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
from pg_cyan.lobs import PgCyanLobs
from pg_cyan.metrics import PgCyanMetrics
from pg_cyan.orchestrator import PgCyanOrchestrator
from pg_cyan.parameters import PgCyanParameters
from pg_cyan.planner import PgCyanPlanner
from pg_cyan.preflight import PgCyanPreflight
from pg_cyan.snapshot import PgCyanSnapshot


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


@cli.command(name="source-pg-metrics")
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
              help="Directory for source-pg-metrics[-errors].json.")
def source_pg_metrics(host: str, port: int, user: str, password: str,
                      database: str, timeout: int, output_dir: str) -> None:
    """Collect source database metrics useful for migration planning.

    Writes ``source-pg-metrics.json`` on success, or
    ``source-pg-metrics-errors.json`` with an error message on failure.
    """
    collector = PgCyanMetrics(
        host=host, port=port, user=user, password=password,
        database=database, timeout=timeout, output_dir=output_dir,
    )
    try:
        summary = collector.collect()
        collector.close()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        collector.close()
        sys.exit(2)


@cli.command(name="source-largeobjects")
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
              help="Directory for source-pg-largeobjects[-errors].json.")
def source_largeobjects(host: str, port: int, user: str, password: str,
                        database: str, timeout: int, output_dir: str) -> None:
    """Count the source database's large objects and write the count to JSON.

    Writes ``source-pg-largeobjects.json`` on success, or
    ``source-pg-largeobjects-errors.json`` with an error message on failure.
    """
    collector = PgCyanLobs(
        host=host, port=port, user=user, password=password,
        database=database, timeout=timeout, output_dir=output_dir,
    )
    try:
        summary = collector.collect()
        collector.close()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        collector.close()
        sys.exit(2)


@cli.command(name="plan")
@click.option("--inputs-dir", default=lambda: os.getenv("PG_INPUTS_DIR", "."),
              show_default="$PG_INPUTS_DIR",
              help="Directory holding the collected source artifacts.")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR",
              help="Directory for plan.json / shard-<n>.filters (or plan-errors.json).")
@click.option("--shards", type=int,
              default=lambda: int(os.getenv("PGCYAN_SHARDS", str(PgCyanPlanner.DEFAULT_SHARDS))),
              show_default="$PGCYAN_SHARDS",
              help="Number of copy shards (clamped to the table count).")
@click.option("--split-tables-larger-than",
              default=lambda: os.getenv("PGCYAN_SPLIT_TABLES_LARGER_THAN",
                                        PgCyanPlanner.DEFAULT_SPLIT_THRESHOLD),
              show_default="$PGCYAN_SPLIT_TABLES_LARGER_THAN",
              help="pgcopydb within-job split threshold recorded per shard.")
@click.option("--snapshot-mode",
              default=lambda: os.getenv("PGCYAN_SNAPSHOT_MODE", PgCyanPlanner.DEFAULT_SNAPSHOT_MODE),
              show_default="$PGCYAN_SNAPSHOT_MODE",
              help="Snapshot strategy recorded in the plan.")
@click.option("--publication-name",
              default=lambda: os.getenv("PGCYAN_PUBLICATION", PgCyanPlanner.DEFAULT_PUBLICATION),
              show_default="$PGCYAN_PUBLICATION",
              help="Logical-replication publication name for downstream phases.")
@click.option("--slot-name",
              default=lambda: os.getenv("PGCYAN_SLOT", PgCyanPlanner.DEFAULT_SLOT),
              show_default="$PGCYAN_SLOT",
              help="Replication slot name for downstream phases.")
@click.option("--run-id", default=lambda: os.getenv("PGCYAN_RUN_ID"),
              show_default="$PGCYAN_RUN_ID",
              help="Opaque run identifier; generated from the clock if omitted.")
def plan(inputs_dir: str, output_dir: str, shards: int, split_tables_larger_than: str,
         snapshot_mode: str, publication_name: str, slot_name: str,
         run_id: str) -> None:
    """Bin-pack the source into disjoint copy shards and emit the migration plan.

    Reads the collected source artifacts from ``--inputs-dir`` and writes
    ``plan.json`` plus one ``shard-<n>.filters`` per shard on success, or
    ``plan-errors.json`` on failure. Aborts non-zero, writing no plan, when the
    source has large objects (pgcopydb CDC does not replicate them).
    """
    planner = PgCyanPlanner(
        inputs_dir=inputs_dir, output_dir=output_dir, shards=shards,
        split_tables_larger_than=split_tables_larger_than,
        snapshot_mode=snapshot_mode, publication_name=publication_name,
        slot_name=slot_name, run_id=run_id,
    )
    try:
        summary = planner.plan()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="orchestrate")
@click.option("--namespace",
              default=lambda: os.getenv("PGCYAN_NAMESPACE", PgCyanOrchestrator.DEFAULT_NAMESPACE),
              show_default="$PGCYAN_NAMESPACE", help="Namespace the migration workloads run in.")
@click.option("--manifests-dir",
              default=lambda: os.getenv("PGCYAN_MANIFESTS_DIR", PgCyanOrchestrator.DEFAULT_MANIFESTS_DIR),
              show_default="$PGCYAN_MANIFESTS_DIR",
              help="Directory containing the deploy/k8s phase manifests.")
@click.option("--plan-configmap",
              default=lambda: os.getenv("PGCYAN_PLAN_CONFIGMAP", PgCyanOrchestrator.DEFAULT_PLAN_CONFIGMAP),
              show_default="$PGCYAN_PLAN_CONFIGMAP",
              help="ConfigMap the planner Job publishes plan.json/shard filters to.")
@click.option("--poll-interval", type=int,
              default=lambda: int(os.getenv("PGCYAN_POLL_INTERVAL", str(PgCyanOrchestrator.DEFAULT_POLL_INTERVAL))),
              show_default="$PGCYAN_POLL_INTERVAL", help="Seconds between Job/Deployment status polls.")
@click.option("--job-timeout", type=int,
              default=lambda: int(os.getenv("PGCYAN_JOB_TIMEOUT", str(PgCyanOrchestrator.DEFAULT_JOB_TIMEOUT))),
              show_default="$PGCYAN_JOB_TIMEOUT",
              help="Seconds to wait for a phase barrier before failing it.")
@click.option("--in-cluster/--kubeconfig", default=True,
              help="Load in-cluster ServiceAccount credentials (default) or a local kubeconfig for dev/testing.")
def orchestrate(namespace: str, manifests_dir: str, plan_configmap: str,
                 poll_interval: int, job_timeout: int, in_cluster: bool) -> None:
    """Walk the migration DAG: apply each phase in order and block on its barrier.

    Applies planner(-1) -> pre-data(0) -> snapshot-holder(1) -> data shards(2)
    -> index(3a) -> FK(3b) -> follow-CDC(4), stopping (without applying later
    phases) at the first phase whose Jobs/Deployments do not all succeed/become
    ready.
    """
    orchestrator = PgCyanOrchestrator(
        namespace=namespace, manifests_dir=manifests_dir, plan_configmap=plan_configmap,
        poll_interval=poll_interval, job_timeout=job_timeout, in_cluster=in_cluster,
    )
    try:
        summary = orchestrator.run()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="snapshot-hold")
@click.option("--source-pguri", default=lambda: os.getenv("PGCOPYDB_SOURCE_PGURI"),
              show_default="$PGCOPYDB_SOURCE_PGURI",
              help="Source libpq URI (pgcopydb-native); falls back to --host/--port/... if unset.")
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
@click.option("--publication-name",
              default=lambda: os.getenv("PGCYAN_PUBLICATION", PgCyanSnapshot.DEFAULT_PUBLICATION),
              show_default="$PGCYAN_PUBLICATION", help="Publication to create FOR ALL TABLES.")
@click.option("--slot-name",
              default=lambda: os.getenv("PGCYAN_SLOT", PgCyanSnapshot.DEFAULT_SLOT),
              show_default="$PGCYAN_SLOT", help="Logical replication slot to create.")
@click.option("--snapshot-mode",
              default=lambda: os.getenv("PGCYAN_SNAPSHOT_MODE", PgCyanSnapshot.DEFAULT_SNAPSHOT_MODE),
              show_default="$PGCYAN_SNAPSHOT_MODE", help="Recorded snapshot mode (held vs frozen-source).")
@click.option("--plugin",
              default=lambda: os.getenv("PGCYAN_PLUGIN", PgCyanSnapshot.DEFAULT_PLUGIN),
              show_default="$PGCYAN_PLUGIN", help="Logical decoding output plugin.")
@click.option("--work-dir",
              default=lambda: os.getenv("PGCYAN_WORK_DIR", PgCyanSnapshot.DEFAULT_WORK_DIR),
              show_default="$PGCYAN_WORK_DIR", help="pgcopydb --dir; the snapshot id is written here.")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR"),
              show_default="$PG_OUTPUT_DIR (defaults to --work-dir)",
              help="Directory for source-pg-slot.json / source-pg-publication.json.")
@click.option("--plan-file", default=lambda: os.getenv("PGCYAN_PLAN_FILE"),
              show_default="$PGCYAN_PLAN_FILE",
              help="Optional plan.json to source publication/slot/mode from.")
@click.option("--pgcopydb-bin",
              default=lambda: os.getenv("PGCOPYDB_BIN", PgCyanSnapshot.DEFAULT_PGCOPYDB_BIN),
              show_default="$PGCOPYDB_BIN", help="pgcopydb executable name/path.")
def snapshot_hold(source_pguri: str, host: str, port: int, user: str, password: str,
                  database: str, timeout: int, publication_name: str, slot_name: str,
                  snapshot_mode: str, plugin: str, work_dir: str, output_dir: str,
                  plan_file: str, pgcopydb_bin: str) -> None:
    """Establish and HOLD the source's consistent publication + slot + snapshot.

    Creates the publication FOR ALL TABLES, creates the pgoutput slot and exports
    its consistent snapshot (writing the id to the shared /work volume), then
    blocks -- keeping the exporting connection open -- until SIGTERM/SIGINT, at
    which point it terminates pgcopydb so the source connection closes cleanly.
    Writes ``source-pg-slot.json`` + ``source-pg-publication.json`` on success,
    or matching ``*-errors.json`` on failure.
    """
    holder = PgCyanSnapshot(
        source_pguri=source_pguri or None,
        host=host, port=port, user=user, password=password,
        database=database, timeout=timeout,
        publication_name=publication_name, slot_name=slot_name,
        snapshot_mode=snapshot_mode, plugin=plugin, work_dir=work_dir,
        output_dir=output_dir or None, plan_file=plan_file or None,
        pgcopydb_bin=pgcopydb_bin,
    )
    try:
        summary = holder.establish()
        click.echo(json.dumps(summary, indent=2, default=str))
        if summary["status"] != "success":
            holder.close()
            sys.exit(1)
        sys.exit(0 if holder.hold() == 0 else 1)
    except KeyboardInterrupt:
        holder.close()
        sys.exit(2)


if __name__ == "__main__":
    cli()
