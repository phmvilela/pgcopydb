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
from pg_cyan.datashard import PgCyanDataShard
from pg_cyan.lobs import PgCyanLobs
from pg_cyan.metrics import PgCyanMetrics
from pg_cyan.orchestrator import PgCyanOrchestrator
from pg_cyan.parameters import PgCyanParameters
from pg_cyan.planner import PgCyanPlanner
from pg_cyan.predata import PgCyanPreData
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

    Applies planner(-1) -> snapshot-holder(1) -> pre-data(0) -> data shards(2)
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
@click.option("--snapshot-configmap", default=lambda: os.getenv("PGCYAN_SNAPSHOT_CONFIGMAP"),
              show_default="$PGCYAN_SNAPSHOT_CONFIGMAP",
              help="ConfigMap to publish the snapshot id into for the shards (blank = skip, local runs).")
@click.option("--configmap-namespace", default=lambda: os.getenv("PGCYAN_NAMESPACE"),
              show_default="$PGCYAN_NAMESPACE (else kubectl's namespace)",
              help="Namespace for the snapshot ConfigMap.")
@click.option("--kubectl-bin", default=lambda: os.getenv("KUBECTL_BIN", "kubectl"),
              show_default="$KUBECTL_BIN", help="kubectl executable used to publish the snapshot id.")
def snapshot_hold(source_pguri: str, host: str, port: int, user: str, password: str,
                  database: str, timeout: int, publication_name: str, slot_name: str,
                  snapshot_mode: str, plugin: str, work_dir: str, output_dir: str,
                  plan_file: str, pgcopydb_bin: str, snapshot_configmap: str,
                  configmap_namespace: str, kubectl_bin: str) -> None:
    """Establish and HOLD the source's consistent publication + slot + snapshot.

    Creates the publication FOR ALL TABLES, creates the pgoutput slot and exports
    its consistent snapshot, publishes the id into the ``pgcyan-snapshot``
    ConfigMap for the shards (issue #58), then blocks -- keeping the exporting
    connection open -- until SIGTERM/SIGINT, at which point it terminates
    pgcopydb so the source connection closes cleanly. Writes
    ``source-pg-slot.json`` + ``source-pg-publication.json`` on success, or
    matching ``*-errors.json`` on failure.
    """
    holder = PgCyanSnapshot(
        source_pguri=source_pguri or None,
        host=host, port=port, user=user, password=password,
        database=database, timeout=timeout,
        publication_name=publication_name, slot_name=slot_name,
        snapshot_mode=snapshot_mode, plugin=plugin, work_dir=work_dir,
        output_dir=output_dir or None, plan_file=plan_file or None,
        pgcopydb_bin=pgcopydb_bin,
        snapshot_configmap=snapshot_configmap or None,
        configmap_namespace=configmap_namespace or None,
        kubectl_bin=kubectl_bin,
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


@cli.command(name="restore-predata")
@click.option("--source-pguri", default=lambda: os.getenv("PGCOPYDB_SOURCE_PGURI"),
              show_default="$PGCOPYDB_SOURCE_PGURI",
              help="Source libpq URI (pgcopydb-native); passed to the dump step.")
@click.option("--target-pguri", default=lambda: os.getenv("PGCOPYDB_TARGET_PGURI"),
              show_default="$PGCOPYDB_TARGET_PGURI",
              help="Target libpq URI (pgcopydb-native); passed to the restore step.")
@click.option("--snapshot", "snapshot_id", default=lambda: os.getenv("SNAPSHOT_ID"),
              show_default="$SNAPSHOT_ID",
              help="Exported snapshot id from the phase-1 holder to pin the dump to.")
@click.option("--work-dir",
              default=lambda: os.getenv("PGCYAN_WORK_DIR", PgCyanPreData.DEFAULT_WORK_DIR),
              show_default="$PGCYAN_WORK_DIR", help="pgcopydb --dir; the schema dump lands here.")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR"),
              show_default="$PG_OUTPUT_DIR (defaults to --work-dir)",
              help="Directory for target-pg-schema-log[-errors].json.")
@click.option("--no-owner/--owner", default=True,
              help="Pass --no-owner to the restore (target roles pre-created by Terraform).")
@click.option("--drop-if-exists/--no-drop-if-exists", default=True,
              help="Pass --drop-if-exists so retries over a half-restored target are idempotent.")
@click.option("--skip-extensions", is_flag=True, default=False,
              help="Skip extensions in both the dump and the restore.")
@click.option("--not-consistent", is_flag=True, default=False,
              help="Do not pin to the holder's snapshot; let the dump take its own.")
@click.option("--restore-jobs", type=int,
              default=lambda: int(os.getenv("PGCYAN_RESTORE_JOBS", str(PgCyanPreData.DEFAULT_RESTORE_JOBS))),
              show_default="$PGCYAN_RESTORE_JOBS", help="pg_restore parallelism for the pre-data section.")
@click.option("--pgcopydb-bin",
              default=lambda: os.getenv("PGCOPYDB_BIN", PgCyanPreData.DEFAULT_PGCOPYDB_BIN),
              show_default="$PGCOPYDB_BIN", help="pgcopydb executable name/path.")
def restore_predata(source_pguri: str, target_pguri: str, snapshot_id: str,
                    work_dir: str, output_dir: str, no_owner: bool,
                    drop_if_exists: bool, skip_extensions: bool, not_consistent: bool,
                    restore_jobs: int, pgcopydb_bin: str) -> None:
    """Transfer the source's pre-data schema (tables only) to the target.

    A thin wrapper that dumps the source schema pinned to the phase-1 holder's
    snapshot (``pgcopydb dump schema --snapshot``) and restores ONLY the
    pre-data section to the target (``pgcopydb restore pre-data``) -- tables,
    sequences and functions, but deliberately NOT secondary indexes,
    primary-key/unique constraints, foreign keys, or triggers (those are the
    post-data phase, #28, so the initial COPY is not slowed by index
    maintenance). Writes ``target-pg-schema-log.json`` on success, or
    ``target-pg-schema-log-errors.json`` on failure.
    """
    worker = PgCyanPreData(
        source_pguri=source_pguri or None,
        target_pguri=target_pguri or None,
        snapshot_id=snapshot_id or None,
        work_dir=work_dir, output_dir=output_dir or None,
        no_owner=no_owner, drop_if_exists=drop_if_exists,
        skip_extensions=skip_extensions, not_consistent=not_consistent,
        restore_jobs=restore_jobs, pgcopydb_bin=pgcopydb_bin,
    )
    try:
        summary = worker.run()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="copy-shard")
@click.option("--source-pguri", default=lambda: os.getenv("PGCOPYDB_SOURCE_PGURI"),
              show_default="$PGCOPYDB_SOURCE_PGURI",
              help="Source libpq URI (pgcopydb-native); passed to the copy step.")
@click.option("--target-pguri", default=lambda: os.getenv("PGCOPYDB_TARGET_PGURI"),
              show_default="$PGCOPYDB_TARGET_PGURI",
              help="Target libpq URI (pgcopydb-native); also used for the per-table count(*).")
@click.option("--snapshot", "snapshot_id", default=lambda: os.getenv("SNAPSHOT_ID"),
              show_default="$SNAPSHOT_ID",
              help="Exported snapshot id from the phase-1 holder to pin the copy to.")
@click.option("--filter-file", default=lambda: os.getenv("FILTER_FILE"),
              show_default="$FILTER_FILE",
              help="Shard filter file (e.g. shard-0.filters), resolved under $PGCYAN_PLAN_DIR.")
@click.option("--shard-name", default=lambda: os.getenv("SHARD_NAME"),
              show_default="$SHARD_NAME", help="This shard's name (e.g. shard-0), recorded in the log.")
@click.option("--slot-name", default=lambda: os.getenv("SLOT_NAME") or os.getenv("PGCYAN_SLOT"),
              show_default="$SLOT_NAME / $PGCYAN_SLOT", help="Replication slot name, recorded in the log.")
@click.option("--publication-name",
              default=lambda: os.getenv("PUBLICATION_NAME") or os.getenv("PGCYAN_PUBLICATION"),
              show_default="$PUBLICATION_NAME / $PGCYAN_PUBLICATION", help="Publication name, recorded in the log.")
@click.option("--table-jobs", type=int,
              default=lambda: int(os.getenv("TABLE_JOBS", str(PgCyanDataShard.DEFAULT_TABLE_JOBS))),
              show_default="$TABLE_JOBS", help="pgcopydb --table-jobs (intra-shard COPY parallelism).")
@click.option("--split-tables-larger-than",
              default=lambda: os.getenv("SPLIT_TABLES_LARGER_THAN", PgCyanDataShard.DEFAULT_SPLIT_THRESHOLD),
              show_default="$SPLIT_TABLES_LARGER_THAN", help="pgcopydb --split-tables-larger-than threshold.")
@click.option("--count-rows/--no-count-rows",
              default=lambda: os.getenv("PGCYAN_COUNT_ROWS", "true").strip().lower() in ("1", "true", "yes", "on"),
              help="Count rows on the target after the copy (verification; disable for very large tables).")
def copy_shard(source_pguri: str, target_pguri: str, snapshot_id: str, filter_file: str,
               shard_name: str, slot_name: str, publication_name: str,
               table_jobs: int, split_tables_larger_than: str, count_rows: bool) -> None:
    """Copy one disjoint shard's table DATA to the target, pinned to the snapshot.

    A thin wrapper over ``pgcopydb copy table-data`` (DATA ONLY -- phase 0 already
    created the schema, phase 3 builds the indexes/constraints), restricted to
    this shard's ``[include-only-table]`` filter set and pinned to the phase-1
    holder's ``--snapshot`` so every shard copies the same instant. Resumable on
    its own RWO work disk (``--resume --restart=false``). After the copy it counts
    the rows landed on the target per table and writes ``migration-copy-log.json``
    (slot / publication / snapshot names + per-table row counts), or
    ``migration-copy-log-errors.json`` on failure. The log is also published to a
    per-shard ``pgcyan-copylog-<shard>`` ConfigMap for the orchestrator to
    aggregate across shards. Paths/ConfigMap/namespace are env-driven
    ($PGCYAN_PLAN_DIR, $PGCYAN_WORK_DIR, $PG_OUTPUT_DIR, $PGCYAN_COPYLOG_CONFIGMAP,
    $PGCYAN_NAMESPACE) -- the manifest sets them.
    """
    worker = PgCyanDataShard(
        source_pguri=source_pguri or None,
        target_pguri=target_pguri or None,
        snapshot_id=snapshot_id or None,
        filter_file=filter_file or None,
        plan_dir=os.getenv("PGCYAN_PLAN_DIR", PgCyanDataShard.DEFAULT_PLAN_DIR),
        shard_name=shard_name or None,
        slot_name=slot_name or None, publication_name=publication_name or None,
        work_dir=os.getenv("PGCYAN_WORK_DIR", PgCyanDataShard.DEFAULT_WORK_DIR),
        output_dir=os.getenv("PG_OUTPUT_DIR") or None,
        table_jobs=table_jobs, split_tables_larger_than=split_tables_larger_than,
        count_rows=count_rows,
        # Env-driven (the manifest sets these); not CLI flags to keep the surface lean.
        copylog_configmap=os.getenv("PGCYAN_COPYLOG_CONFIGMAP") or None,
        namespace=os.getenv("PGCYAN_NAMESPACE") or None,
        kubectl_bin=os.getenv("KUBECTL_BIN", PgCyanDataShard.DEFAULT_KUBECTL_BIN),
    )
    try:
        summary = worker.run()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        sys.exit(2)


if __name__ == "__main__":
    cli()
