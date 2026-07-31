"""
Sharded initial copy pinned to the snapshot (issue #25) -- phase ``2``.

This is the phase ``2`` node in ``deploy/k8s/`` (``60-job-phase2-datashard.template.yaml``):
one Job per copy shard, all running concurrently, each copying its OWN disjoint
set of tables (authored by the planner #38 as ``shard-<n>.filters``) so no row is
copied twice and none is missed. Scale-out is *more shard Jobs*, not fatter pods
-- COPY is network-bound (source -> pod -> target).

DATA ONLY -- ``pgcopydb copy table-data``, NOT ``clone``. Phase 0 (#24) already
created the target schema (pre-data section: tables, sequences, functions) and
phase 3 (#28) builds the secondary indexes / PK-unique / FK constraints
afterwards (post-data section). ``pgcopydb clone`` would re-run BOTH schema
sections around the COPY (see ``notes/migration-phases-lessons-learned.md`` item
6), colliding with phases 0/3 and re-tripping the Azure extension/ownership walls
(item 2). ``pgcopydb copy table-data`` copies the HEAP and nothing else -- no
schema, no indexes, no vacuum -- which is exactly this phase's job.

CONSISTENCY: every shard is pinned to the SAME ``--snapshot=<id>`` -- the
consistent point the controller (#70, which folded in the phase-1 holder #23)
holds in-pod and injects into each phase pod as ``$SNAPSHOT_ID`` -- so all shards
copy the same instant. (#70 superseded the earlier ``pgcyan-snapshot`` ConfigMap
handoff, #58.)

RESUME: each shard has its OWN ReadWriteOnce managed disk mounted at ``--dir``
(``/work``); ``--resume`` means a killed/backed-off shard pod re-attaches the
same disk and continues at per-table / per-part granularity, never redoing a
completed table. So -- unlike the (non-resumable) pre-data pod -- this stage does
**not** wipe its work dir on start. (``--restart`` is deliberately NOT passed: it
is a boolean flag that WIPES prior temp files, and ``copy table-data`` rejects
``--restart=false`` outright.)

Output (written to ``--output-dir`` / ``$PG_OUTPUT_DIR``, defaulting to the work
dir), following the same success-vs-error two-file shape as the other stages
(see ``CLAUDE.md``):
  * ``migration-copy-log.json``        -- on success: a ``completed`` copy status,
                                          the slot / publication / snapshot names,
                                          and the per-table row counts this shard
                                          copied.
  * ``migration-copy-log-errors.json`` -- on any failure, with the error message(s).

The per-table row counts are the GROUND TRUTH: after the COPY completes this
stage runs ``SELECT count(*)`` on the **target** for each table in the shard's
filter set (pgcopydb's own progress JSON only exposes the *source* ``reltuples``
estimate, not rows actually landed). Counting is gated by ``--count-rows`` /
``$PGCYAN_COUNT_ROWS`` -- default on, because verifying the copy is the whole
point of the log, but disableable for very large tables where a full seq-scan
(indexes are not built until phase 3) is too costly. Sourcing the count from
pgcopydb's own accounting instead is tracked in issue #69.

The per-shard log is ALSO published into its OWN ``pgcyan-copylog-<shard>``
ConfigMap (via ``kubectl``, mirroring how the snapshot holder publishes its
snapshot id): the shard's work dir is a private RWO disk the orchestrator cannot
mount, so the ConfigMap is the durable, structured handoff the orchestrator (#40)
reads to aggregate the combined ``migration-copy-log.json`` across all shards.
The shard runs under the ``pgcyan-worker`` SA, which is granted a narrow
``configmaps: [create, get, patch]`` for exactly this (see ``01-rbac.yaml``).

Follows the ``PgCyanPreData`` / ``PgCyanSnapshot`` wrapper pattern: a thin driver
over pgcopydb subprocesses plus a small psycopg2 read for the counts, structured
JSON logging via ``_log_event``, and graceful degradation -- ``run()`` never
raises.
"""
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import Error as PgError
from psycopg2 import sql

# Structured JSON logging (mirrors the other stages).
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)


class PgCyanDataShard:
    """Copies one disjoint shard's table data to the target, pinned to the snapshot."""

    #: Written on the happy path (non-empty, carries a "completed" copy status).
    LOG_OUTPUT_FILENAME = "migration-copy-log.json"
    #: Written on any failure, with the error message(s).
    LOG_ERROR_FILENAME = "migration-copy-log-errors.json"
    #: The single key the per-shard copy log is published under in its ConfigMap
    #: (and the key the orchestrator reads when aggregating).
    COPY_LOG_CONFIGMAP_KEY = "migration-copy-log.json"
    #: Label the orchestrator selects on to find the per-shard copy-log ConfigMaps.
    COPY_LOG_LABEL = "pgcyan.dev/copylog"

    #: Kept in lock-step with the other stages / the snapshot holder.
    DEFAULT_WORK_DIR = "/work"
    #: Where the planner's shard-<n>.filters files are mounted (ConfigMap).
    DEFAULT_PLAN_DIR = "/plan"
    #: pgcopydb executable; not configurable -- add a knob only if a use case needs it.
    PGCOPYDB_BIN = "pgcopydb"
    #: kubectl executable used to publish the per-shard copy-log ConfigMap.
    DEFAULT_KUBECTL_BIN = "kubectl"
    #: pgcopydb intra-shard COPY parallelism default (the planner overrides per shard).
    DEFAULT_TABLE_JOBS = 4
    #: pgcopydb within-job split threshold default (the planner overrides per shard).
    DEFAULT_SPLIT_THRESHOLD = "50GB"

    #: The COPY can run for a long time on a big shard; give it a generous ceiling
    #: rather than let a hung source connection wedge the Job forever. k8s
    #: backoffLimit + --resume handle a killed pod picking up where it left off.
    COPY_TIMEOUT = 6 * 60 * 60      # 6h
    #: Per-table count(*) timeout (statement_timeout on the counting connection).
    COUNT_TIMEOUT = 30 * 60         # 30m

    def __init__(
        self,
        source_pguri: Optional[str] = None,
        target_pguri: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        filter_file: Optional[str] = None,
        plan_dir: str = DEFAULT_PLAN_DIR,
        shard_name: Optional[str] = None,
        slot_name: Optional[str] = None,
        publication_name: Optional[str] = None,
        work_dir: str = DEFAULT_WORK_DIR,
        output_dir: Optional[str] = None,
        table_jobs: int = DEFAULT_TABLE_JOBS,
        split_tables_larger_than: str = DEFAULT_SPLIT_THRESHOLD,
        count_rows: bool = True,
        copylog_configmap: Optional[str] = None,
        namespace: Optional[str] = None,
        kubectl_bin: str = DEFAULT_KUBECTL_BIN,
    ) -> None:
        """
        Args:
            source_pguri: Source libpq URI (pgcopydb reads ``PGCOPYDB_SOURCE_PGURI``
                natively; passed as ``--source`` when set).
            target_pguri: Target libpq URI (pgcopydb reads ``PGCOPYDB_TARGET_PGURI``
                natively; passed as ``--target`` when set, and used for the
                per-table count(*)).
            snapshot_id: The exported snapshot id from the phase-1 holder to pin
                the copy to. Required -- shards must all copy the same instant.
            filter_file: The shard's filter file name (e.g. ``shard-0.filters``),
                resolved under ``plan_dir``; its ``[include-only-table]`` list
                restricts the copy and drives the per-table count.
            plan_dir: Directory the shard filter files are mounted in.
            shard_name: This shard's name (e.g. ``shard-0``), recorded in the log
                and used to derive the per-shard ConfigMap name.
            slot_name: Replication slot name, recorded in the log (from the plan).
            publication_name: Publication name, recorded in the log (from the plan).
            work_dir: pgcopydb ``--dir`` (this shard's own RWO disk).
            output_dir: Directory for the log files (defaults to ``work_dir``).
            table_jobs: ``--table-jobs`` -- intra-shard COPY parallelism.
            split_tables_larger_than: ``--split-tables-larger-than`` threshold.
            count_rows: Count rows on the target after the copy (verification).
            copylog_configmap: ConfigMap to publish the per-shard copy log into
                for the orchestrator to aggregate. Defaults to
                ``pgcyan-copylog-<shard_name>``. Blank disables publishing (local runs).
            namespace: Namespace for the ConfigMap (blank -> kubectl's own namespace).
            kubectl_bin: kubectl executable used to publish the ConfigMap.
        """
        self.source_pguri = source_pguri
        self.target_pguri = target_pguri
        self.snapshot_id = snapshot_id
        self.filter_file = filter_file
        self.plan_dir = plan_dir
        self.shard_name = shard_name
        self.slot_name = slot_name
        self.publication_name = publication_name
        self.work_dir = work_dir
        self.output_dir = output_dir if output_dir is not None else work_dir
        self.table_jobs = table_jobs
        self.split_tables_larger_than = split_tables_larger_than
        self.count_rows = count_rows
        # Default the per-shard ConfigMap name off the shard name so the manifest
        # does not have to spell it out; the orchestrator finds it by label anyway.
        if copylog_configmap is None and shard_name:
            copylog_configmap = f"pgcyan-copylog-{shard_name}"
        self.copylog_configmap = copylog_configmap
        self.namespace = namespace
        self.kubectl_bin = kubectl_bin

        self.errors: List[str] = []

    # ------------------------------------------------------------- logging

    def _log_event(
        self, event_type: str, status: str, message: str,
        details: Optional[Dict] = None
    ) -> None:
        """Emit a single structured JSON log line (mirrors the other stages)."""
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "data-shard",
            "shard": self.shard_name,
            "event_type": event_type,
            "status": status,
            "message": message,
        }
        if details:
            event["details"] = details
        logger.info(json.dumps(event, default=str))

    # --------------------------------------------------------- filter parsing

    def filter_path(self) -> Optional[str]:
        """Absolute path to this shard's filter file, or ``None`` if unset."""
        if not self.filter_file:
            return None
        # An absolute filter_file is used as-is; otherwise resolve under plan_dir.
        if os.path.isabs(self.filter_file):
            return self.filter_file
        return os.path.join(self.plan_dir, self.filter_file)

    def parse_filter_tables(self) -> List[Tuple[str, str]]:
        """
        Parse the shard's ``[include-only-table]`` list into ``(schema, table)``.

        The planner writes one ``schema.table`` per line under the
        ``[include-only-table]`` section (``planner._filters_body``). We read only
        that section and split each entry on the FIRST dot -- matching the
        planner's own ``f"{schema}.{name}"`` composition. Never raises; a missing
        or unreadable file records an error and returns an empty list.
        """
        path = self.filter_path()
        if not path:
            return []
        try:
            with open(path, encoding="utf-8") as fh:
                raw = fh.read()
        except OSError as e:
            self._log_event(
                "parse_filters", "failure", f"Could not read filter file {path}",
                {"path": path, "error": str(e)},
            )
            self.errors.append(f"Could not read filter file {path}: {str(e)}")
            return []

        tables: List[Tuple[str, str]] = []
        in_section = False
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("[") and stripped.endswith("]"):
                in_section = stripped.lower() == "[include-only-table]"
                continue
            if in_section and "." in stripped:
                schema, table = stripped.split(".", 1)
                tables.append((schema, table))
        return tables

    # ------------------------------------------------------- command build

    def build_copy_command(self) -> List[str]:
        """
        Build the ``pgcopydb copy table-data`` invocation for this shard.

        DATA ONLY: ``copy table-data`` copies the heap and nothing else -- no
        schema (phase 0), no indexes/constraints (phase 3), no vacuum. Restricted
        to the shard's disjoint tables via ``--filters`` and pinned to the
        holder's snapshot via ``--snapshot``. ``--resume`` makes a restarted pod
        continue on its RWO disk rather than start over; ``--restart`` is
        deliberately OMITTED -- it is a boolean flag (``copy table-data`` rejects
        ``--restart=false`` outright) that WIPES prior temp files, the opposite of
        what a resumable shard wants, and it is off by default anyway.
        """
        cmd = [self.PGCOPYDB_BIN, "copy", "table-data", "--dir", self.work_dir]
        # pgcopydb reads PGCOPYDB_SOURCE_PGURI / _TARGET_PGURI from the env; pass
        # them explicitly only when given, so both paths agree.
        if self.source_pguri:
            cmd += ["--source", self.source_pguri]
        if self.target_pguri:
            cmd += ["--target", self.target_pguri]
        filter_path = self.filter_path()
        if filter_path:
            cmd += ["--filters", filter_path]
        if self.table_jobs:
            cmd += ["--table-jobs", str(self.table_jobs)]
        if self.split_tables_larger_than:
            cmd += ["--split-tables-larger-than", str(self.split_tables_larger_than)]
        cmd.append("--skip-vacuum")
        cmd.append("--resume")
        if self.snapshot_id:
            cmd += ["--snapshot", self.snapshot_id]
        return cmd

    def _run_copy(self) -> bool:
        """
        Run the ``pgcopydb copy table-data`` step, capturing combined output.

        Logs a ``success``/``failure`` event and, on failure, records the error
        (with the tail of the captured output) for the ``*-errors.json`` file.
        Returns ``True`` iff the process exited 0.
        """
        cmd = self.build_copy_command()
        self._log_event("copy", "success", "Running pgcopydb copy table-data",
                        {"command": " ".join(cmd)})
        try:
            result = subprocess.run(  # noqa: S603 -- fixed argv, no shell
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=self.COPY_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            self._log_event(
                "copy", "failure", f"Copy timed out after {self.COPY_TIMEOUT}s",
                {"command": " ".join(cmd),
                 "remediation": "The shard is resumable (--resume); a fresh pod continues on its RWO disk."},
            )
            self.errors.append(f"Copy timed out after {self.COPY_TIMEOUT}s")
            return False
        except (OSError, ValueError) as e:
            self._log_event(
                "copy", "failure", "Failed to launch pgcopydb",
                {"command": " ".join(cmd), "error": str(e),
                 "remediation": "Confirm the pgcopydb binary is on PATH in the image"},
            )
            self.errors.append(f"Failed to launch pgcopydb: {str(e)}")
            return False

        output = (result.stdout or "").strip()
        if output:
            logger.info(output)
        if result.returncode != 0:
            tail = output[-2000:] if output else ""
            self._log_event(
                "copy", "failure", f"Copy exited non-zero (rc={result.returncode})",
                {"returncode": result.returncode, "output_tail": tail,
                 "remediation": "Read the pgcopydb FATAL/ERROR lines above; check the snapshot is still valid (holder up), the source/target URIs, and target write privileges. rc=12 -> work-dir permissions; rc=7 -> catalog/snapshot mismatch (see notes/migration-phases-lessons-learned.md)."},
            )
            self.errors.append(f"Copy failed (rc={result.returncode}): {tail}")
            return False
        self._log_event("copy", "success", "Copy completed", {"returncode": 0})
        return True

    # ----------------------------------------------------------- row counts

    def count_target_rows(
        self, tables: List[Tuple[str, str]]
    ) -> Dict[str, Optional[int]]:
        """
        Count the rows landed on the target for each ``(schema, table)``.

        Ground truth for "rows copied": ``SELECT count(*)`` per table on the
        TARGET, over one short-lived connection. Never raises -- a connection
        failure records an error and returns ``None`` for every table; a
        per-table error records that table's count as ``None`` and continues, so
        one bad table does not lose the rest. ``statement_timeout`` bounds each
        count (indexes are not built until phase 3, so these are seq-scans).
        """
        counts: Dict[str, Optional[int]] = {
            f"{schema}.{table}": None for schema, table in tables
        }
        if not tables:
            return counts
        if not self.target_pguri:
            self.errors.append("Cannot count target rows: no target URI supplied")
            return counts

        conn = None
        try:
            conn = psycopg2.connect(self.target_pguri)
            conn.autocommit = True
        except PgError as e:
            self._log_event(
                "count_rows", "failure", "Could not connect to the target to count rows",
                {"error": str(e),
                 "remediation": "The COPY still succeeded; counts are a verification add-on. Check the target URI / network, or pass --no-count-rows."},
            )
            self.errors.append(f"Could not connect to target for row counts: {str(e)}")
            return counts

        try:
            with conn.cursor() as cur:
                try:
                    # SET takes a literal, not a bind parameter -- inline the
                    # (integer) millisecond value via sql.Literal.
                    cur.execute(
                        sql.SQL("SET statement_timeout = {}").format(
                            sql.Literal(int(self.COUNT_TIMEOUT * 1000))
                        )
                    )
                except PgError:
                    pass  # non-fatal; proceed without a per-statement ceiling
                for schema, table in tables:
                    key = f"{schema}.{table}"
                    try:
                        cur.execute(
                            sql.SQL("SELECT count(*) FROM {}.{}").format(
                                sql.Identifier(schema), sql.Identifier(table)
                            )
                        )
                        row = cur.fetchone()
                        counts[key] = int(row[0]) if row and row[0] is not None else 0
                    except PgError as e:
                        self._log_event(
                            "count_rows", "warning", f"Could not count rows for {key}",
                            {"table": key, "error": str(e)},
                        )
                        self.errors.append(f"Could not count rows for {key}: {str(e)}")
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass
        return counts

    # ------------------------------------------------------------- output

    def _build_success(self, tables: Dict[str, Optional[int]]) -> Dict:
        """Assemble the ``migration-copy-log.json`` payload."""
        counted = [v for v in tables.values() if v is not None]
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "data-shard",
            "status": "completed",
            "shard": self.shard_name,
            "slot": self.slot_name,
            "publication": self.publication_name,
            "snapshot": self.snapshot_id,
            "counted": self.count_rows,
            "tables": tables,
            "table_count": len(tables),
            "row_count_total": sum(counted) if counted else 0,
            "command": " ".join(self.build_copy_command()),
        }

    def _build_error(self, message: str) -> Dict:
        """Assemble a ``migration-copy-log-errors.json`` payload."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "data-shard",
            "status": "failure",
            "shard": self.shard_name,
            "message": message,
            "errors": self.errors or ["Unknown error"],
            "remediation": "The data-copy phase is a hard barrier -- the orchestrator (#40) blocks phase 3 until every shard succeeds. Fix the cause and let k8s retry the Job (the shard resumes on its RWO disk).",
        }

    def _write_json(self, filename: str, payload: Dict) -> Optional[str]:
        """Write ``payload`` as pretty JSON to ``output_dir/filename``."""
        path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, indent=2, default=str) + "\n")
            return path
        except OSError as e:
            self._log_event(
                "write_output", "failure", f"Failed to write {filename}",
                {"path": path, "error": str(e)},
            )
            self.errors.append(f"Failed to write {filename}: {str(e)}")
            return None

    def publish_copylog_configmap(self, log_path: str) -> bool:
        """
        Publish the per-shard copy log into its ``pgcyan-copylog-<shard>`` ConfigMap.

        The shard's ``migration-copy-log.json`` lives on a private RWO disk the
        orchestrator cannot mount, so -- mirroring how the snapshot holder
        publishes its snapshot id -- the shard writes it to a labelled ConfigMap
        that the orchestrator lists + reads to aggregate the combined copy log.
        Uses the create-then-apply idiom so a resumed shard overwrites rather than
        errors, and labels it (``pgcyan.dev/copylog=shard``) for the orchestrator's
        selector.

        A no-op success when no ConfigMap is configured (local runs without a
        cluster). Returns ``False`` only on a real publish failure. Never raises.
        """
        if not self.copylog_configmap:
            self._log_event(
                "publish_copylog", "success",
                "No copy-log ConfigMap configured; skipping publish (local run)",
                {"shard": self.shard_name},
            )
            return True

        ns_args = ["--namespace", self.namespace] if self.namespace else []
        render_cmd = [
            self.kubectl_bin, "create", "configmap", self.copylog_configmap,
            f"--from-file={self.COPY_LOG_CONFIGMAP_KEY}={log_path}",
            "--dry-run=client", "-o", "yaml", *ns_args,
        ]
        # Stamp the selector label (and the shard name) onto the rendered object
        # offline, so create|label|apply is a single idempotent publish.
        label_cmd = [
            self.kubectl_bin, "label", "--local", "-f", "-", "--overwrite",
            "-o", "yaml",
            f"{self.COPY_LOG_LABEL}=shard",
            f"pgcyan.dev/shard={self.shard_name or ''}",
        ]
        apply_cmd = [self.kubectl_bin, "apply", "-f", "-", *ns_args]
        try:
            rendered = subprocess.run(  # noqa: S603 -- fixed argv, no shell
                render_cmd, check=True, capture_output=True, text=True
            ).stdout
            labelled = subprocess.run(  # noqa: S603 -- fixed argv, no shell
                label_cmd, input=rendered, check=True, capture_output=True, text=True
            ).stdout
            subprocess.run(  # noqa: S603 -- fixed argv, no shell
                apply_cmd, input=labelled, check=True, capture_output=True, text=True
            )
            self._log_event(
                "publish_copylog", "success",
                "Published the per-shard copy log to its ConfigMap",
                {"configmap": self.copylog_configmap, "key": self.COPY_LOG_CONFIGMAP_KEY},
            )
            return True
        except (OSError, subprocess.CalledProcessError) as e:
            stderr = getattr(e, "stderr", "") or ""
            self._log_event(
                "publish_copylog", "failure",
                "Failed to publish the per-shard copy log ConfigMap",
                {"configmap": self.copylog_configmap, "error": str(e), "stderr": stderr,
                 "remediation": "The orchestrator aggregates from this ConfigMap; ensure the pgcyan-worker SA can create/patch configmaps (01-rbac.yaml) and kubectl is on PATH."},
            )
            self.errors.append(f"Failed to publish copy-log ConfigMap: {str(e)}")
            return False

    # --------------------------------------------------------------- run

    def run(self) -> Dict:
        """
        Copy this shard's table data to the target, count the landed rows, write
        the copy log, and publish it to the per-shard ConfigMap. Never raises.
        Returns a summary dict:
            {
              "status": "success" | "failure",
              "log_file": <path or None>,
              "log_error_file": <path or None>,
              "shard": <name or None>,
              "snapshot": <id or None>,
              "row_count_total": <int>,
              "table_count": <int>,
              "published": <bool>,
              "errors": [...],
            }
        """
        summary = {
            "status": "failure",
            "log_file": None,
            "log_error_file": None,
            "shard": self.shard_name,
            "snapshot": self.snapshot_id,
            "row_count_total": 0,
            "table_count": 0,
            "published": False,
            "errors": self.errors,
        }

        # Consistency guard: every shard MUST pin the holder's snapshot, or this
        # shard could copy a different instant than the rest.
        if not self.snapshot_id:
            self._log_event(
                "run", "failure", "No snapshot id supplied",
                {"remediation": "Pass --snapshot (the controller injects $SNAPSHOT_ID into each phase pod, #70)"},
            )
            self.errors.append(
                "No snapshot id: pass --snapshot=<id> (the holder's snapshot)"
            )
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Missing snapshot id for a consistent shard copy"),
            )
            return summary

        filter_path = self.filter_path()
        if not filter_path or not os.path.isfile(filter_path):
            self._log_event(
                "run", "failure", "Shard filter file missing",
                {"filter_file": self.filter_file, "resolved": filter_path,
                 "remediation": "The planner (#38) publishes shard-<n>.filters into the pgcyan-plan ConfigMap; ensure it is mounted at --plan-dir and FILTER_FILE names it."},
            )
            self.errors.append(
                f"Shard filter file missing: {filter_path or self.filter_file!r}"
            )
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Missing shard filter file"),
            )
            return summary

        # Run the data-only COPY.
        if not self._run_copy():
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("pgcopydb copy table-data failed for this shard"),
            )
            return summary

        # Ground-truth per-table row counts on the target (verification add-on).
        tables = self.parse_filter_tables()
        table_keys = [f"{s}.{t}" for s, t in tables]
        counts: Dict[str, Optional[int]]
        if self.count_rows:
            counts = self.count_target_rows(tables)
        else:
            self._log_event(
                "count_rows", "success", "Row counting disabled (--no-count-rows)",
                {"table_count": len(tables)},
            )
            counts = {key: None for key in table_keys}

        payload = self._build_success(counts)
        summary["row_count_total"] = payload["row_count_total"]
        summary["table_count"] = payload["table_count"]

        summary["log_file"] = self._write_json(self.LOG_OUTPUT_FILENAME, payload)
        if not summary["log_file"]:
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Copy succeeded but writing the copy log failed"),
            )
            return summary

        # Publish the per-shard log for the orchestrator to aggregate. A publish
        # failure does NOT fail the shard -- the copy (the deliverable) succeeded
        # and the log is on disk; the orchestrator simply cannot see this shard's
        # counts. Surface it as a warning via the summary flag + recorded error.
        summary["published"] = self.publish_copylog_configmap(summary["log_file"])

        summary["status"] = "success"
        self._log_event(
            "run", "success", "Shard copy complete",
            {"log_file": summary["log_file"], "shard": self.shard_name,
             "row_count_total": summary["row_count_total"],
             "table_count": summary["table_count"], "snapshot": self.snapshot_id,
             "published": summary["published"]},
        )
        return summary


def copy_shard_from_env() -> int:
    """Convenience entry point mirroring the other stages' ``*_from_env``."""
    worker = _datashard_from_env()
    summary = worker.run()
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary["status"] == "success" else 1


def _env_flag(name: str, default: bool) -> bool:
    """Parse a boolean-ish env var, falling back to ``default`` when unset."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _datashard_from_env() -> "PgCyanDataShard":
    """Build a ``PgCyanDataShard`` from the environment (container defaults)."""
    return PgCyanDataShard(
        source_pguri=os.getenv("PGCOPYDB_SOURCE_PGURI") or None,
        target_pguri=os.getenv("PGCOPYDB_TARGET_PGURI") or None,
        snapshot_id=os.getenv("SNAPSHOT_ID") or None,
        filter_file=os.getenv("FILTER_FILE") or None,
        plan_dir=os.getenv("PGCYAN_PLAN_DIR", PgCyanDataShard.DEFAULT_PLAN_DIR),
        shard_name=os.getenv("SHARD_NAME") or None,
        slot_name=os.getenv("SLOT_NAME") or os.getenv("PGCYAN_SLOT") or None,
        publication_name=os.getenv("PUBLICATION_NAME") or os.getenv("PGCYAN_PUBLICATION") or None,
        work_dir=os.getenv("PGCYAN_WORK_DIR", PgCyanDataShard.DEFAULT_WORK_DIR),
        output_dir=os.getenv("PG_OUTPUT_DIR") or None,
        table_jobs=int(os.getenv("TABLE_JOBS", str(PgCyanDataShard.DEFAULT_TABLE_JOBS))),
        split_tables_larger_than=os.getenv(
            "SPLIT_TABLES_LARGER_THAN", PgCyanDataShard.DEFAULT_SPLIT_THRESHOLD
        ),
        count_rows=_env_flag("PGCYAN_COUNT_ROWS", True),
        copylog_configmap=os.getenv("PGCYAN_COPYLOG_CONFIGMAP") or None,
        namespace=os.getenv("PGCYAN_NAMESPACE") or None,
        kubectl_bin=os.getenv("KUBECTL_BIN", PgCyanDataShard.DEFAULT_KUBECTL_BIN),
    )


if __name__ == "__main__":
    sys.exit(copy_shard_from_env())
