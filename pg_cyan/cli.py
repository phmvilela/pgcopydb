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
from pg_cyan.controller import PgCyanController
from pg_cyan.copy.datashard import PgCyanDataShard
from pg_cyan.copy.postfk import PgCyanForeignKeys
from pg_cyan.copy.sequences import PgCyanSequences
from pg_cyan.copy.postindex import PgCyanPostIndex
from pg_cyan.copy.lobs import PgCyanLobs
from pg_cyan.collect.metrics import PgCyanMetrics
from pg_cyan.orchestrator import PgCyanOrchestrator
from pg_cyan.plan.parameters import PgCyanParameters
from pg_cyan.plan.planner import PgCyanPlanner
from pg_cyan.copy.predata import PgCyanPreData
from pg_cyan.plan.preflight import PgCyanPreflight
from pg_cyan.collect.capinputs import PgCyanCapacityInputs
from pg_cyan.collect.netprobe import PgCyanNetworkProbe
from pg_cyan.collect.sourcecap import PgCyanSourceCapacity
from pg_cyan.collect.sourcecap_azure import PgCyanSourceCapacityAzure
from pg_cyan.collect.targetcap import PgCyanTargetCapacity
from pg_cyan.collect.clustercap import PgCyanClusterCapacity
from pg_cyan.collect.copyloghistory import PgCyanCopylogHistory
from pg_cyan.postgres.conn import _source_default, _target_default


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
@click.option("--host", default=_source_default("host"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_HOST", help="Source PostgreSQL host.")
@click.option("--port", type=int, default=_source_default("port"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_PORT", help="Source PostgreSQL port.")
@click.option("--user", default=_source_default("user"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_USER", help="Source database user.")
@click.option("--password", default=_source_default("password"),
              help="Source database password (prefer $PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_PASSWORD).")
@click.option("--database", default=_source_default("database"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_DATABASE", help="Source database name.")
@click.option("--sslmode", default=_source_default("sslmode"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PGSSLMODE",
              help="libpq sslmode (e.g. require); omitted if unset.")
@click.option("--timeout", type=int,
              default=lambda: int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
              show_default="$PREFLIGHT_TIMEOUT_SECONDS", help="Connection timeout (seconds).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR",
              help="Directory for source-pg-metrics[-errors].json.")
def source_pg_metrics(host: str, port: int, user: str, password: str,
                      database: str, sslmode: str, timeout: int, output_dir: str) -> None:
    """Collect source database metrics useful for migration planning.

    Writes ``source-pg-metrics.json`` on success, or
    ``source-pg-metrics-errors.json`` with an error message on failure.
    """
    collector = PgCyanMetrics(
        host=host, port=port, user=user, password=password,
        database=database, timeout=timeout, output_dir=output_dir,
        sslmode=sslmode or None,
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
@click.option("--host", default=_source_default("host"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_HOST", help="Source PostgreSQL host.")
@click.option("--port", type=int, default=_source_default("port"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_PORT", help="Source PostgreSQL port.")
@click.option("--user", default=_source_default("user"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_USER", help="Source database user.")
@click.option("--password", default=_source_default("password"),
              help="Source database password (prefer $PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_PASSWORD).")
@click.option("--database", default=_source_default("database"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PG_SOURCE_DATABASE", help="Source database name.")
@click.option("--sslmode", default=_source_default("sslmode"),
              show_default="$PGCOPYDB_SOURCE_PGURI or $PGSSLMODE",
              help="libpq sslmode (e.g. require); omitted if unset.")
@click.option("--timeout", type=int,
              default=lambda: int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
              show_default="$PREFLIGHT_TIMEOUT_SECONDS", help="Connection timeout (seconds).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR",
              help="Directory for source-pg-largeobjects[-errors].json.")
def source_largeobjects(host: str, port: int, user: str, password: str,
                        database: str, sslmode: str, timeout: int, output_dir: str) -> None:
    """Count the source database's large objects and write the count to JSON.

    Writes ``source-pg-largeobjects.json`` on success, or
    ``source-pg-largeobjects-errors.json`` with an error message on failure.
    """
    collector = PgCyanLobs(
        host=host, port=port, user=user, password=password,
        database=database, timeout=timeout, output_dir=output_dir,
        sslmode=sslmode or None,
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
              help="Logical-replication publication name for downstream steps.")
@click.option("--slot-name",
              default=lambda: os.getenv("PGCYAN_SLOT", PgCyanPlanner.DEFAULT_SLOT),
              show_default="$PGCYAN_SLOT",
              help="Replication slot name for downstream steps.")
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


@cli.command(name="network-probe")
@click.option("--host", default=_source_default("host"),
              show_default="$PGCOPYDB_SOURCE_PGURI / $PG_SOURCE_HOST", help="Source host.")
@click.option("--port", type=int, default=_source_default("port"),
              show_default="from URI / $PG_SOURCE_PORT", help="Source port.")
@click.option("--user", default=_source_default("user"),
              show_default="from URI / $PG_SOURCE_USER", help="Source user.")
@click.option("--password", default=_source_default("password"),
              show_default="from URI / $PG_SOURCE_PASSWORD", help="Source password.")
@click.option("--database", default=_source_default("database"),
              show_default="from URI / $PG_SOURCE_DATABASE", help="Source database.")
@click.option("--sslmode", default=_source_default("sslmode"),
              show_default="from URI / $PGSSLMODE", help="libpq sslmode.")
@click.option("--timeout", type=int,
              default=lambda: int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
              show_default="$PREFLIGHT_TIMEOUT_SECONDS", help="Per-connect timeout (seconds).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR", help="Directory network.json is written to.")
@click.option("--streams", type=int,
              default=lambda: int(os.getenv("PGCYAN_NET_STREAMS", "4")),
              show_default="$PGCYAN_NET_STREAMS", help="Parallel streams for the bandwidth probe.")
@click.option("--stream-bytes", type=int,
              default=lambda: int(os.getenv("PGCYAN_NET_STREAM_BYTES", str(16 * 1024 * 1024))),
              show_default="$PGCYAN_NET_STREAM_BYTES", help="Approx bytes generated per stream.")
@click.option("--rtt-samples", type=int,
              default=lambda: int(os.getenv("PGCYAN_NET_RTT_SAMPLES", "5")),
              show_default="$PGCYAN_NET_RTT_SAMPLES", help="TCP connects; the min is the RTT.")
@click.option("--warmup-fraction", type=float,
              default=lambda: float(os.getenv("PGCYAN_NET_WARMUP_FRACTION", "0.2")),
              show_default="$PGCYAN_NET_WARMUP_FRACTION",
              help="Fraction of each transfer discarded as warm-up before rating it.")
def network_probe(host: str, port: int, user: str, password: str, database: str,
                  sslmode: str, timeout: int, output_dir: str, streams: int,
                  stream_bytes: int, rtt_samples: int, warmup_fraction: float) -> None:
    """Measure the source<->cluster path and emit the network.json capacity artifact.

    Measures RTT (TCP connect) and effective throughput (timed COPY, single +
    parallel streams, steady-state sampled past an initial warm-up window), then
    writes ``network.json`` for the capacity-aware planner. When parallel streams
    don't scale near-linearly, records a diagnostic ``aggregate_scaling`` block
    (not consumed by the planner) suggesting whether the source or the network is
    the likely limiter. An **optional** collector: a probe failure leaves any
    declared ``network.json`` in place and still exits 0, so it never breaks the walk.
    """
    prober = PgCyanNetworkProbe(
        host=host, port=port, user=user, password=password, database=database,
        sslmode=sslmode, timeout=timeout, output_dir=output_dir,
        streams=streams, stream_bytes=stream_bytes, rtt_samples=rtt_samples,
        warmup_fraction=warmup_fraction,
    )
    try:
        summary = prober.probe()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="source-capacity")
@click.option("--host", default=_source_default("host"),
              show_default="$PGCOPYDB_SOURCE_PGURI / $PG_SOURCE_HOST", help="Source host.")
@click.option("--port", type=int, default=_source_default("port"),
              show_default="from URI / $PG_SOURCE_PORT", help="Source port.")
@click.option("--user", default=_source_default("user"),
              show_default="from URI / $PG_SOURCE_USER", help="Source user.")
@click.option("--password", default=_source_default("password"),
              show_default="from URI / $PG_SOURCE_PASSWORD", help="Source password.")
@click.option("--database", default=_source_default("database"),
              show_default="from URI / $PG_SOURCE_DATABASE", help="Source database.")
@click.option("--sslmode", default=_source_default("sslmode"),
              show_default="from URI / $PGSSLMODE", help="libpq sslmode.")
@click.option("--timeout", type=int,
              default=lambda: int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
              show_default="$PREFLIGHT_TIMEOUT_SECONDS", help="Connect timeout (seconds).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR", help="Directory source-capacity.json is written to.")
@click.option("--wal-sample-seconds", type=float,
              default=lambda: float(os.getenv("PGCYAN_WAL_SAMPLE_SECONDS", "2")),
              show_default="$PGCYAN_WAL_SAMPLE_SECONDS",
              help="Interval over which the WAL generation rate is sampled.")
def source_capacity(host: str, port: int, user: str, password: str, database: str,
                    sslmode: str, timeout: int, output_dir: str,
                    wal_sample_seconds: float) -> None:
    """Fill the catalog fields of source-capacity.json (connections + WAL rate).

    Measures ``max_connections``/``current_connections`` and the live WAL
    generation rate, then **merges** them into any existing ``source-capacity.json``
    (preserving declared/cloud ``read_throughput_bytes_per_sec`` /
    ``wal_disk_free_bytes``). An **optional** collector: a failure preserves the
    existing artifact and still exits 0, so it never breaks the walk.
    """
    collector = PgCyanSourceCapacity(
        host=host, port=port, user=user, password=password, database=database,
        sslmode=sslmode, timeout=timeout, output_dir=output_dir,
        wal_sample_seconds=wal_sample_seconds,
    )
    try:
        summary = collector.collect()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="source-capacity-azure")
@click.option("--resource-id", default=lambda: os.getenv("PGCYAN_SOURCE_AZURE_RESOURCE_ID"),
              show_default="$PGCYAN_SOURCE_AZURE_RESOURCE_ID",
              help="The source server's Azure resource id (Flexible Server).")
@click.option("--timeout", type=int,
              default=lambda: int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
              show_default="$PREFLIGHT_TIMEOUT_SECONDS", help="Azure API connect timeout (seconds).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR", help="Directory source-capacity.json is written to.")
def source_capacity_azure(resource_id: str, timeout: int, output_dir: str) -> None:
    """Fill the Azure-cloud fields of source-capacity.json (read ceiling + WAL disk free).

    Reads the source Flexible Server's provisioned storage/SKU and the
    ``storage_used`` Monitor metric, then **merges**
    ``read_throughput_bytes_per_sec`` / ``wal_disk_free_bytes`` into any existing
    ``source-capacity.json`` (preserving declared/catalog fields). Needs
    ``PGCYAN_SOURCE_AZURE_RESOURCE_ID`` and an Azure credential (Workload
    Identity). An **optional** collector: a missing resource id or any Azure API
    failure preserves the existing artifact and still exits 0.
    """
    collector = PgCyanSourceCapacityAzure(
        resource_id=resource_id, output_dir=output_dir, timeout=timeout,
    )
    try:
        summary = collector.collect()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="target-capacity")
@click.option("--host", default=_target_default("host"),
              show_default="$PGCOPYDB_TARGET_PGURI / $PG_TARGET_HOST", help="Target host.")
@click.option("--port", type=int, default=_target_default("port"),
              show_default="from URI / $PG_TARGET_PORT", help="Target port.")
@click.option("--user", default=_target_default("user"),
              show_default="from URI / $PG_TARGET_USER", help="Target user.")
@click.option("--password", default=_target_default("password"),
              show_default="from URI / $PG_TARGET_PASSWORD", help="Target password.")
@click.option("--database", default=_target_default("database"),
              show_default="from URI / $PG_TARGET_DATABASE", help="Target database.")
@click.option("--sslmode", default=_target_default("sslmode"),
              show_default="from URI / $PGSSLMODE", help="libpq sslmode.")
@click.option("--timeout", type=int,
              default=lambda: int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
              show_default="$PREFLIGHT_TIMEOUT_SECONDS", help="Connect timeout (seconds).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR", help="Directory target-capacity.json is written to.")
def target_capacity(host: str, port: int, user: str, password: str, database: str,
                    sslmode: str, timeout: int, output_dir: str) -> None:
    """Fill the catalog fields of target-capacity.json (connection headroom).

    Measures the TARGET's ``max_connections``/``current_connections`` and
    **merges** them into any existing ``target-capacity.json`` (preserving a
    declared/cloud ``write_throughput_bytes_per_sec``). An **optional** collector:
    a failure preserves the existing artifact and still exits 0.
    """
    collector = PgCyanTargetCapacity(
        host=host, port=port, user=user, password=password, database=database,
        sslmode=sslmode, timeout=timeout, output_dir=output_dir,
    )
    try:
        summary = collector.collect()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="copylog-history")
@click.option("--copylog", "copylog_path",
              default=lambda: os.getenv("PGCYAN_COPYLOG_PATH", "."),
              show_default="$PGCYAN_COPYLOG_PATH",
              help="A migration-copy-log.json file, or a directory of them.")
@click.option("--metrics", "metrics_path",
              default=lambda: os.getenv("PGCYAN_METRICS_PATH"),
              show_default="$PGCYAN_METRICS_PATH",
              help="Optional source-pg-metrics.json to enrich observations with byte volume.")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR", help="Directory copylog-history.json is written to.")
@click.option("--max-runs", type=int,
              default=lambda: int(os.getenv("PGCYAN_HISTORY_MAX_RUNS",
                                            str(PgCyanCopylogHistory.DEFAULT_MAX_RUNS))),
              show_default="$PGCYAN_HISTORY_MAX_RUNS", help="Retention window (most recent runs).")
@click.option("--run-id", default=lambda: os.getenv("PGCYAN_RUN_ID"),
              show_default="$PGCYAN_RUN_ID", help="Opaque id for this run's history entry.")
def copylog_history(copylog_path: str, metrics_path: str, output_dir: str,
                    max_runs: int, run_id: str) -> None:
    """Append a completed run's copy observations to copylog-history.json.

    Reads the copy step's per-shard logs (``migration-copy-log.json``, which carry
    ``duration_seconds`` + ``table_jobs``), normalizes each shard to a
    single-stream-equivalent observation, and appends a windowed run entry to
    ``copylog-history.json`` for the next run's planner to calibrate from. Pure
    transform; a run with no usable durations leaves the history unchanged.
    """
    from pg_cyan.collect.copyloghistory import produce_copylog_history_from_env
    try:
        code = produce_copylog_history_from_env(
            copylog_path, metrics_path, output_dir, max_runs, run_id)
        sys.exit(code)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="cluster-capacity")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR", help="Directory cluster-capacity.json is written to.")
def cluster_capacity(output_dir: str) -> None:
    """Aggregate node allocatable capacity into cluster-capacity.json.

    Lists the cluster's Ready, schedulable nodes (in-cluster) and sums their
    allocatable CPU + pod capacity for the planner's ``k8s_cpu`` term and pod cap,
    **merging** into any existing artifact. An **optional** collector: no
    in-cluster config / node-list RBAC / API error preserves the existing artifact
    and still exits 0, so it never breaks the walk.
    """
    collector = PgCyanClusterCapacity(output_dir=output_dir)
    try:
        summary = collector.collect()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="capacity-inputs")
@click.option("--inputs-dir", default=lambda: os.getenv("PG_INPUTS_DIR", "."),
              show_default="$PG_INPUTS_DIR",
              help="Directory holding the declared capacity bundle.")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR", "."),
              show_default="$PG_OUTPUT_DIR",
              help="Directory the validated capacity artifacts are written to.")
@click.option("--declared-filename",
              default=lambda: os.getenv("PGCYAN_CAPACITY_DECLARED",
                                        PgCyanCapacityInputs.DECLARED_FILENAME),
              show_default="$PGCYAN_CAPACITY_DECLARED",
              help="The operator-declared bundle filename to read.")
def capacity_inputs(inputs_dir: str, output_dir: str, declared_filename: str) -> None:
    """Explode an operator-declared capacity bundle into validated planner artifacts.

    Reads the declared bundle from ``--inputs-dir`` and writes each valid
    sub-object (source/target capacity, network, cluster capacity, copylog
    history, constraints) as the canonical artifact file the planner reads, plus a
    ``capacity-inputs.json`` manifest. Invalid sub-objects are skipped so the
    planner degrades that term rather than trusting bad input; a missing bundle is
    a no-op. Exits non-zero only when nothing usable could be produced.
    """
    provider = PgCyanCapacityInputs(
        inputs_dir=inputs_dir, output_dir=output_dir,
        declared_filename=declared_filename,
    )
    try:
        summary = provider.provide()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] in ("success", "warning") else 1)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="orchestrate")
@click.option("--namespace",
              default=lambda: os.getenv("PGCYAN_NAMESPACE", PgCyanOrchestrator.DEFAULT_NAMESPACE),
              show_default="$PGCYAN_NAMESPACE", help="Namespace the migration workloads run in.")
@click.option("--manifests-dir",
              default=lambda: os.getenv("PGCYAN_MANIFESTS_DIR", PgCyanOrchestrator.DEFAULT_MANIFESTS_DIR),
              show_default="$PGCYAN_MANIFESTS_DIR",
              help="Directory containing the deploy/k8s step manifests.")
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
              help="Seconds to wait for a step barrier before failing it.")
@click.option("--in-cluster/--kubeconfig", default=True,
              help="Load in-cluster ServiceAccount credentials (default) or a local kubeconfig for dev/testing.")
def orchestrate(namespace: str, manifests_dir: str, plan_configmap: str,
                 poll_interval: int, job_timeout: int, in_cluster: bool) -> None:
    """Walk the migration DAG: apply each step in order and block on its barrier.

    Applies plan -> schema -> copy -> indexes -> foreign-keys,
    stopping (without applying later steps) at the first step whose Jobs/
    Deployments do not all succeed/become ready. (CDC is not a DAG step -- the
    controller streams it after the walk.) Standalone: the snapshot is NOT held
    (that is what `run-migration` adds); pass SNAPSHOT_ID some other way if a
    step needs it.
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


@cli.command(name="schema")
@click.option("--source-pguri", default=lambda: os.getenv("PGCOPYDB_SOURCE_PGURI"),
              show_default="$PGCOPYDB_SOURCE_PGURI",
              help="Source libpq URI (pgcopydb-native); passed to the dump step.")
@click.option("--target-pguri", default=lambda: os.getenv("PGCOPYDB_TARGET_PGURI"),
              show_default="$PGCOPYDB_TARGET_PGURI",
              help="Target libpq URI (pgcopydb-native); passed to the restore step.")
@click.option("--snapshot", "snapshot_id", default=lambda: os.getenv("SNAPSHOT_ID"),
              show_default="$SNAPSHOT_ID",
              help="Exported snapshot id from the snapshot holder to pin the dump to.")
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
def schema(source_pguri: str, target_pguri: str, snapshot_id: str,
                    work_dir: str, output_dir: str, no_owner: bool,
                    drop_if_exists: bool, skip_extensions: bool, not_consistent: bool,
                    restore_jobs: int, pgcopydb_bin: str) -> None:
    """Transfer the source's pre-data schema (tables only) to the target.

    A thin wrapper that dumps the source schema pinned to the snapshot holder's
    snapshot (``pgcopydb dump schema --snapshot``) and restores ONLY the
    pre-data section to the target (``pgcopydb restore pre-data``) -- tables,
    sequences and functions, but deliberately NOT secondary indexes,
    primary-key/unique constraints, foreign keys, or triggers (those are the
    post-data step, so the initial COPY is not slowed by index
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


@cli.command(name="copy")
@click.option("--source-pguri", default=lambda: os.getenv("PGCOPYDB_SOURCE_PGURI"),
              show_default="$PGCOPYDB_SOURCE_PGURI",
              help="Source libpq URI (pgcopydb-native); passed to the copy step.")
@click.option("--target-pguri", default=lambda: os.getenv("PGCOPYDB_TARGET_PGURI"),
              show_default="$PGCOPYDB_TARGET_PGURI",
              help="Target libpq URI (pgcopydb-native); also used for the per-table count(*).")
@click.option("--snapshot", "snapshot_id", default=lambda: os.getenv("SNAPSHOT_ID"),
              show_default="$SNAPSHOT_ID",
              help="Exported snapshot id from the snapshot holder to pin the copy to.")
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
def copy(source_pguri: str, target_pguri: str, snapshot_id: str, filter_file: str,
               shard_name: str, slot_name: str, publication_name: str,
               table_jobs: int, split_tables_larger_than: str, count_rows: bool) -> None:
    """Copy one disjoint shard's table DATA to the target, pinned to the snapshot.

    A thin wrapper over ``pgcopydb copy table-data`` (DATA ONLY -- the schema step already
    created the schema, the post-data steps builds the indexes/constraints), restricted to
    this shard's ``[include-only-table]`` filter set and pinned to the snapshot
    holder's ``--snapshot`` so every shard copies the same instant. Resumable on
    its own RWO work disk (``--resume``). After the copy it counts
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


@cli.command(name="indexes")
@click.option("--source-pguri", default=lambda: os.getenv("PGCOPYDB_SOURCE_PGURI"),
              show_default="$PGCOPYDB_SOURCE_PGURI", help="Source libpq URI (index/constraint defs read from here).")
@click.option("--target-pguri", default=lambda: os.getenv("PGCOPYDB_TARGET_PGURI"),
              show_default="$PGCOPYDB_TARGET_PGURI", help="Target libpq URI (DDL runs here).")
@click.option("--filter-file", default=lambda: os.getenv("FILTER_FILE"),
              show_default="$FILTER_FILE", help="This shard's filter file (under $PGCYAN_PLAN_DIR).")
@click.option("--shard-name", default=lambda: os.getenv("SHARD_NAME"),
              show_default="$SHARD_NAME", help="This index shard's name (e.g. idx-0), recorded in the log.")
def indexes(source_pguri: str, target_pguri: str,
                  filter_file: str, shard_name: str) -> None:
    """Build one shard's indexes + PK/unique constraints on the target (the indexes step).

    Custom SQL (like foreign-keys, not pgcopydb): introspect this shard's
    tables' secondary indexes (pg_get_indexdef) and PK/UNIQUE constraints
    (pg_get_constraintdef) from the source and apply them to the target -- CREATE
    INDEX then ADD CONSTRAINT. The PK/unique piece is deferred here from pre-data.
    Foreign keys are the separate single pass (``foreign-keys``). Writes
    ``target-pg-index-log.json`` (per shard) or ``-errors.json``.
    """
    worker = PgCyanPostIndex(
        source_pguri=source_pguri or None, target_pguri=target_pguri or None,
        filter_file=filter_file or None,
        plan_dir=os.getenv("PGCYAN_PLAN_DIR", PgCyanPostIndex.DEFAULT_PLAN_DIR),
        shard_name=shard_name or None,
        output_dir=os.getenv("PG_OUTPUT_DIR") or None,
    )
    try:
        summary = worker.run()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="sequences")
@click.option("--source-pguri", default=lambda: os.getenv("PGCOPYDB_SOURCE_PGURI"),
              show_default="$PGCOPYDB_SOURCE_PGURI", help="Source libpq URI (sequence values read from here).")
@click.option("--target-pguri", default=lambda: os.getenv("PGCOPYDB_TARGET_PGURI"),
              show_default="$PGCOPYDB_TARGET_PGURI", help="Target libpq URI (sequences setval'd here).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR"),
              show_default="$PG_OUTPUT_DIR", help="Directory for target-pg-sequences-log[-errors].json.")
def sequences(source_pguri: str, target_pguri: str, output_dir: str) -> None:
    """Sync the target's sequences to the source after the copy (the sequences step).

    Custom SQL (not pgcopydb): read each source sequence's value and ``setval`` it on
    the target, so target identity/serial sequences continue past the copied rows.
    The COPY-based data load leaves them at their start, which would make CDC
    and post-cutover inserts collide on id=1. Runs once, after the data copy, before
    CDC. Writes ``target-pg-sequences-log.json``.
    """
    worker = PgCyanSequences(
        source_pguri=source_pguri or None, target_pguri=target_pguri or None,
        output_dir=output_dir or None,
    )
    try:
        summary = worker.run()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="foreign-keys")
@click.option("--source-pguri", default=lambda: os.getenv("PGCOPYDB_SOURCE_PGURI"),
              show_default="$PGCOPYDB_SOURCE_PGURI", help="Source libpq URI (introspected for FK defs).")
@click.option("--target-pguri", default=lambda: os.getenv("PGCOPYDB_TARGET_PGURI"),
              show_default="$PGCOPYDB_TARGET_PGURI", help="Target libpq URI (FKs applied + verified here).")
@click.option("--output-dir", default=lambda: os.getenv("PG_OUTPUT_DIR"),
              show_default="$PG_OUTPUT_DIR", help="Directory for target-pg-postload-log[-errors].json.")
def foreign_keys(source_pguri: str, target_pguri: str, output_dir: str) -> None:
    """Apply the source's foreign keys to the target (the foreign-keys step).

    Custom SQL (not pgcopydb): introspect the source's FK definitions and apply
    each to the target as ``ADD CONSTRAINT ... NOT VALID`` (brief lock) then
    ``VALIDATE CONSTRAINT`` (scans under a weak lock) -- the minimal-downtime
    pattern pgcopydb's inline-validating restore cannot produce. Runs as one
    coordinated pass after every index shard, and writes the authoritative
    ``target-pg-postload-log.json`` verifying the target's indexes + validated FKs.
    """
    worker = PgCyanForeignKeys(
        source_pguri=source_pguri or None, target_pguri=target_pguri or None,
        output_dir=output_dir or None,
    )
    try:
        summary = worker.run()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary["status"] == "success" else 1)
    except KeyboardInterrupt:
        sys.exit(2)


@cli.command(name="run-migration")
@click.option("--namespace",
              default=lambda: os.getenv("PGCYAN_NAMESPACE", PgCyanOrchestrator.DEFAULT_NAMESPACE),
              show_default="$PGCYAN_NAMESPACE", help="Namespace the migration workloads run in.")
@click.option("--in-cluster/--kubeconfig", default=True,
              help="Load in-cluster ServiceAccount credentials (default) or a local kubeconfig.")
@click.option("--stop-after", default=lambda: os.getenv("PGCYAN_STOP_AFTER") or None,
              type=click.Choice([p["id"] for p in PgCyanOrchestrator.STEPS]),
              show_default="$PGCYAN_STOP_AFTER (empty/unset = the whole DAG)",
              help="Bring the pipeline up through this step id, then park Ready (e.g. copy = through the data copy).")
@click.option("--follow-cdc/--no-follow-cdc",
              default=lambda: os.getenv("PGCYAN_FOLLOW_CDC", "").strip().lower() in ("1", "true", "yes"),
              show_default="$PGCYAN_FOLLOW_CDC (unset = off)",
              help="After a full walk, become the CDC consumer (`pgcopydb follow`).")
@click.option("--wait-for-start/--no-wait-for-start",
              default=lambda: os.getenv("PGCYAN_WAIT_FOR_START", "").strip().lower() in ("1", "true", "yes"),
              show_default="$PGCYAN_WAIT_FOR_START (unset = off)",
              help="Park pre-snapshot and serve a control API; take the snapshot and walk only on POST /start")
def run_migration(namespace: str, in_cluster: bool, stop_after: str,
                  follow_cdc: bool, wait_for_start: bool) -> None:
    """Hold the snapshot in-pod and walk the whole migration DAG.

    The folded controller: it establishes and HOLDS the consistent snapshot in
    its own pod (no separate holder Deployment, no `pgcyan-snapshot` ConfigMap),
    walks plan -> schema -> copy -> indexes -> foreign-keys injecting the snapshot
    id into the pods and owning every workload it creates, then -- with
    ``--follow-cdc`` -- releases the snapshot and becomes the CDC consumer itself
    (`pgcopydb follow` on its own --dir, Option A). Single-pod StatefulSet;
    config is env-driven (the manifest sets it).

    With ``--wait-for-start`` it does NOT snapshot on launch: it serves
    a small control API (`GET /status`, `POST /start`) and parks pre-snapshot,
    establishing the snapshot + walking only when `POST /start` arrives.
    """
    from pg_cyan.copy.snapshot import _holder_from_env
    holder = _holder_from_env()
    controller = PgCyanController(holder=holder, namespace=namespace,
                                  in_cluster=in_cluster, stop_after=stop_after or None,
                                  follow_cdc=follow_cdc,
                                  api_port=int(os.getenv("PGCYAN_API_PORT",
                                                         str(PgCyanController.DEFAULT_API_PORT))))
    try:
        if wait_for_start:
            summary = controller.serve_and_run()   # serves the API + parks pre-snapshot
        else:
            summary = controller.run()
            # Park in the terminal state (Ready iff succeeded) rather than exiting --
            # a StatefulSet pod would otherwise be restarted and crash-loop.
            controller.park()
        click.echo(json.dumps(summary, indent=2, default=str))
        sys.exit(0 if summary.get("status") == "success" else 1)
    except KeyboardInterrupt:
        sys.exit(2)


if __name__ == "__main__":
    cli()
