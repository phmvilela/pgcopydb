"""
Pre-data schema transfer for the sharded pgcopydb topology.

This is the step ``0`` node in ``deploy/k8s/`` (``40-job-phase0-predata.yaml``):
a single coordinator Job that creates the *target* schema **before** any data
shard runs, so every shard finds its tables already present. It transfers the
**pre-data section only** -- tables, sequences, functions, LOB metadata -- and
deliberately NOT secondary indexes, primary-key/unique constraints, foreign
keys, or triggers. Those all live in Postgres' **post-data** section (per
``pg_dump``/``pgcopydb``: primary keys and unique constraints are backed by
indexes and are created *after* the data lands, see ``pgcopydb`` clone steps
6-10) and are built by the post-data step, which is dramatically faster
than maintaining them during the COPY.

Rather than hand-building the DDL transfer, this is a thin wrapper over pgcopydb
(mirroring the snapshot holder in ``snapshot.py``). ``pgcopydb restore pre-data``
restores from a custom-format dump file in ``--dir``, so there is no such file in
this pod's own (empty) work dir -- the wrapper runs two steps:

  1. ``pgcopydb dump schema --snapshot=<id> --dir=<work>`` -- pins the schema
     dump to the **exact snapshot exported by the snapshot holder** so the
     schema matches the instant the shards copy, then
  2. ``pgcopydb restore pre-data --dir=<work> --target=<uri>`` -- applies ONLY
     the pre-data section to the target.

The snapshot id arrives as ``$SNAPSHOT_ID``, injected into this Job's env at
apply time by the controller -- which holds the consistent snapshot in-pod and
orders this step after it (see ``pg_cyan/orchestrator.py``). There is no
separate holder Job and no ``configMapKeyRef``. Consistency can be waived with
``--not-consistent`` (the dump then takes its own snapshot) only if the source
schema is stable for the migration window.

Idempotent retries: the work dir is reset to a clean slate at the start of every
run (the RWO ``--dir`` disk persists across pod restarts, and a leftover dump
would make ``dump schema`` abort), and ``restore pre-data`` runs with
``--drop-if-exists`` so a retry over a half-restored target drops and recreates
cleanly.

ASSUMPTION: target roles are pre-created (Terraform), so ``--no-owner`` applies.
Drop it (``--owner``) if the target must own objects as the source did.

Output (written to ``--output-dir`` / ``$PG_OUTPUT_DIR``, defaulting to the work
dir), following the same success-vs-error two-file shape as the other stages
(see ``CLAUDE.md``):
  * ``target-pg-schema-log.json``        -- on success, a summary + success
                                            message + the pgcopydb commands run.
  * ``target-pg-schema-log-errors.json`` -- on any failure, with the error
                                            message(s).

Follows the ``PgCyanSnapshot`` / ``PgCyanMetrics`` orchestrator pattern:
structured JSON logging via ``_log_event`` and graceful degradation --
``run()`` never raises.
"""
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Optional

# Structured JSON logging (mirrors the other stages).
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)


class PgCyanPreData:
    """Transfers the source's pre-data schema (tables only) to the target."""

    #: Written on the happy path (non-empty, carries a success message).
    LOG_OUTPUT_FILENAME = "target-pg-schema-log.json"
    #: Written on any failure, with the error message(s).
    LOG_ERROR_FILENAME = "target-pg-schema-log-errors.json"

    #: Kept in lock-step with the other stages / the snapshot holder.
    DEFAULT_WORK_DIR = "/work"
    DEFAULT_PGCOPYDB_BIN = "pgcopydb"
    #: pg_restore parallelism for the (small) pre-data section.
    DEFAULT_RESTORE_JOBS = 2

    #: How long each pgcopydb step may run before it is abandoned. Pre-data is
    #: schema DDL only -- tiny -- but a hung source connection should not wedge
    #: the Job forever.
    DEFAULT_STEP_TIMEOUT = 900

    def __init__(
        self,
        source_pguri: Optional[str] = None,
        target_pguri: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        work_dir: str = DEFAULT_WORK_DIR,
        output_dir: Optional[str] = None,
        no_owner: bool = True,
        drop_if_exists: bool = True,
        skip_extensions: bool = False,
        not_consistent: bool = False,
        restore_jobs: int = DEFAULT_RESTORE_JOBS,
        pgcopydb_bin: str = DEFAULT_PGCOPYDB_BIN,
        step_timeout: int = DEFAULT_STEP_TIMEOUT,
    ) -> None:
        """
        Args:
            source_pguri: Source libpq URI (pgcopydb reads ``PGCOPYDB_SOURCE_PGURI``
                natively; passed as ``--source`` when set).
            target_pguri: Target libpq URI (``--target`` for the restore step;
                pgcopydb also reads ``PGCOPYDB_TARGET_PGURI`` natively).
            snapshot_id: The exported snapshot id from the snapshot holder to pin
                the dump to. Required unless ``not_consistent`` is set.
            work_dir: pgcopydb ``--dir`` (the dump lands here).
            output_dir: Directory for the log files (defaults to ``work_dir``).
            no_owner: Pass ``--no-owner`` to the restore (target roles pre-created).
            drop_if_exists: Pass ``--drop-if-exists`` so retries are idempotent.
            skip_extensions: Pass ``--skip-extensions`` to both steps.
            not_consistent: Skip snapshot pinning; the dump takes its own snapshot.
            restore_jobs: ``--restore-jobs`` for the pre-data restore.
            pgcopydb_bin: pgcopydb executable name/path.
            step_timeout: Per-step subprocess timeout in seconds.
        """
        self.source_pguri = source_pguri
        self.target_pguri = target_pguri
        self.snapshot_id = snapshot_id
        self.work_dir = work_dir
        self.output_dir = output_dir if output_dir is not None else work_dir
        self.no_owner = no_owner
        self.drop_if_exists = drop_if_exists
        self.skip_extensions = skip_extensions
        self.not_consistent = not_consistent
        self.restore_jobs = restore_jobs
        self.pgcopydb_bin = pgcopydb_bin
        self.step_timeout = step_timeout

        self.errors: List[str] = []

    # ------------------------------------------------------------- logging

    def _log_event(
        self, event_type: str, status: str, message: str,
        details: Optional[Dict] = None
    ) -> None:
        """Emit a single structured JSON log line (mirrors the other stages)."""
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "pre-data",
            "event_type": event_type,
            "status": status,
            "message": message,
        }
        if details:
            event["details"] = details
        logger.info(json.dumps(event, default=str))

    # --------------------------------------------------------- work dir

    def reset_work_dir(self) -> bool:
        """
        Clear the work dir so every run starts from a clean slate.

        The pre-data pod's ``--dir`` is a ReadWriteOnce managed disk that
        PERSISTS across pod restarts. A leftover dump from a prior
        (failed) run would make ``pgcopydb dump schema`` abort, and a stale
        catalog would confuse the restore -- so wipe it. Pre-data is cheap to
        redo, so a clean run is preferable to resuming a partial one.
        """
        try:
            if os.path.isdir(self.work_dir):
                for entry in os.listdir(self.work_dir):
                    path = os.path.join(self.work_dir, entry)
                    if os.path.isdir(path) and not os.path.islink(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.remove(path)
            os.makedirs(self.work_dir, exist_ok=True)
            self._log_event(
                "reset_work_dir", "success",
                "Cleared the work dir for a fresh pre-data run",
                {"work_dir": self.work_dir},
            )
            return True
        except OSError as e:
            self._log_event(
                "reset_work_dir", "failure",
                f"Could not reset work dir {self.work_dir}",
                {"work_dir": self.work_dir, "error": str(e)},
            )
            self.errors.append(f"Work dir reset failed: {str(e)}")
            return False

    # ------------------------------------------------------- command build

    def build_dump_command(self) -> List[str]:
        """
        Build the ``pgcopydb dump schema`` invocation.

        Dumps the source schema (both pre-data and post-data sections) as custom
        files under ``--dir``; the subsequent ``restore pre-data`` applies only
        the pre-data section. Pinned to the holder's snapshot via ``--snapshot``
        unless running ``--not-consistent`` (in which case pgcopydb takes its own
        snapshot for the dump).
        """
        cmd = [self.pgcopydb_bin, "dump", "schema", "--dir", self.work_dir]
        # pgcopydb also reads PGCOPYDB_SOURCE_PGURI from the env; pass --source
        # explicitly only when we were given the URI, so both paths agree.
        if self.source_pguri:
            cmd += ["--source", self.source_pguri]
        if self.skip_extensions:
            cmd.append("--skip-extensions")
        if not self.not_consistent and self.snapshot_id:
            cmd += ["--snapshot", self.snapshot_id]
        return cmd

    def build_restore_command(self) -> List[str]:
        """
        Build the ``pgcopydb restore pre-data`` invocation.

        Restores ONLY the pre-data section (tables/sequences/functions -- no
        secondary indexes, PK/unique constraints, FKs, or triggers) from the
        local dump to the target. ``--drop-if-exists`` keeps retries idempotent;
        ``--no-owner`` assumes target roles are pre-created (Terraform).

        The dump and restore share the same ``--dir``, and pgcopydb stamps the
        snapshot id into that shared catalog (``<dir>/schema/source.db``). It then
        refuses to reuse the catalog unless the restore's snapshot matches
        ("Catalogs ... have been setup for snapshot X and current snapshot is
        ''"), so we pass the SAME ``--snapshot`` here; with ``--not-consistent``
        we instead tell pgcopydb to ignore the snapshot bookkeeping. (Restore
        reads the local dump and never connects to the source, so this is a
        consistency-check match, not a new source snapshot.)
        """
        cmd = [self.pgcopydb_bin, "restore", "pre-data", "--dir", self.work_dir]
        if self.target_pguri:
            cmd += ["--target", self.target_pguri]
        if self.no_owner:
            cmd.append("--no-owner")
        if self.drop_if_exists:
            cmd.append("--drop-if-exists")
        if self.skip_extensions:
            cmd.append("--skip-extensions")
        if self.restore_jobs:
            cmd += ["--restore-jobs", str(self.restore_jobs)]
        if self.not_consistent:
            cmd.append("--not-consistent")
        elif self.snapshot_id:
            cmd += ["--snapshot", self.snapshot_id]
        return cmd

    def _run_step(self, name: str, cmd: List[str]) -> bool:
        """
        Run one pgcopydb step, streaming nothing but capturing combined output.

        Logs a ``success``/``failure`` event and, on failure, records the error
        (with the tail of the captured output) for the ``*-errors.json`` file.
        Returns ``True`` iff the process exited 0.
        """
        self._log_event(name, "success", f"Running {name}", {"command": " ".join(cmd)})
        try:
            result = subprocess.run(  # noqa: S603 -- fixed argv, no shell
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=self.step_timeout,
            )
        except subprocess.TimeoutExpired:
            self._log_event(
                name, "failure", f"{name} timed out after {self.step_timeout}s",
                {"command": " ".join(cmd)},
            )
            self.errors.append(f"{name} timed out after {self.step_timeout}s")
            return False
        except (OSError, ValueError) as e:
            self._log_event(
                name, "failure", f"Failed to launch {name}",
                {"command": " ".join(cmd), "error": str(e),
                 "remediation": "Confirm the pgcopydb binary is on PATH in the image"},
            )
            self.errors.append(f"Failed to launch {name}: {str(e)}")
            return False

        output = (result.stdout or "").strip()
        if output:
            # Surface pgcopydb's own log lines for debugging.
            logger.info(output)
        if result.returncode != 0:
            tail = output[-2000:] if output else ""
            self._log_event(
                name, "failure",
                f"{name} exited non-zero (rc={result.returncode})",
                {"returncode": result.returncode, "output_tail": tail,
                 "remediation": "Check the pgcopydb output above; verify snapshot validity, source/target URIs, and target privileges"},
            )
            self.errors.append(f"{name} failed (rc={result.returncode}): {tail}")
            return False
        self._log_event(name, "success", f"{name} completed", {"returncode": 0})
        return True

    # ------------------------------------------------------------- output

    def _build_success(self) -> Dict:
        """Assemble the ``target-pg-schema-log.json`` payload."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "pre-data",
            "status": "success",
            "message": (
                "Pre-data schema (tables, sequences, functions) transferred to "
                "the target; secondary indexes, primary-key/unique constraints, "
                "foreign keys and triggers are deliberately omitted (post-data)."
            ),
            "snapshot": self.snapshot_id,
            "not_consistent": self.not_consistent,
            "no_owner": self.no_owner,
            "section": "pre-data",
            "commands": {
                "dump": " ".join(self.build_dump_command()),
                "restore": " ".join(self.build_restore_command()),
            },
        }

    def _build_error(self, message: str) -> Dict:
        """Assemble a ``target-pg-schema-log-errors.json`` payload."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "pre-data",
            "status": "failure",
            "message": message,
            "errors": self.errors or ["Unknown error"],
            "remediation": "Check the messages above; the pre-data step is a hard barrier -- no data shard may start until it succeeds",
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

    # --------------------------------------------------------------- run

    def run(self) -> Dict:
        """
        Reset the work dir, dump the schema, restore its pre-data section, and
        write the log file. Never raises. Returns a summary dict:
            {
              "status": "success" | "failure",
              "log_file": <path or None>,
              "log_error_file": <path or None>,
              "snapshot": <id or None>,
              "errors": [...],
            }
        """
        summary = {
            "status": "failure",
            "log_file": None,
            "log_error_file": None,
            "snapshot": self.snapshot_id,
            "errors": self.errors,
        }

        # Consistency guard: without --not-consistent we MUST have the holder's
        # snapshot id, or the shards and this schema could diverge in time.
        if not self.not_consistent and not self.snapshot_id:
            self._log_event(
                "run", "failure",
                "No snapshot id supplied and --not-consistent not set",
                {"remediation": "Pass --snapshot (the controller injects $SNAPSHOT_ID into each step pod) or --not-consistent"},
            )
            self.errors.append(
                "No snapshot id: pass --snapshot=<id> (holder's snapshot) or --not-consistent"
            )
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Missing snapshot id for a consistent pre-data transfer"),
            )
            return summary

        if not self.target_pguri:
            self._log_event(
                "run", "failure", "No target URI supplied",
                {"remediation": "Set PGCOPYDB_TARGET_PGURI (or pass --target-pguri)"},
            )
            self.errors.append("No target URI: set PGCOPYDB_TARGET_PGURI or pass --target-pguri")
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Missing target database URI"),
            )
            return summary

        if not self.reset_work_dir():
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Failed to reset the work dir"),
            )
            return summary

        # 1) Dump the source schema (pinned to the holder's snapshot).
        if not self._run_step("dump-schema", self.build_dump_command()):
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Failed to dump the source schema"),
            )
            return summary

        # 2) Restore ONLY the pre-data section to the target.
        if not self._run_step("restore-pre-data", self.build_restore_command()):
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Failed to restore the pre-data section to the target"),
            )
            return summary

        summary["log_file"] = self._write_json(
            self.LOG_OUTPUT_FILENAME, self._build_success()
        )
        if not summary["log_file"]:
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME,
                self._build_error("Pre-data transfer succeeded but writing the log file failed"),
            )
            return summary

        summary["status"] = "success"
        self._log_event(
            "run", "success", "Pre-data schema transfer complete",
            {"log_file": summary["log_file"], "snapshot": self.snapshot_id},
        )
        return summary


def restore_predata_from_env() -> int:
    """Convenience entry point mirroring the other stages' ``*_from_env``."""
    worker = _predata_from_env()
    summary = worker.run()
    print(json.dumps(summary, indent=2, default=str))
    return 0 if summary["status"] == "success" else 1


def _predata_from_env() -> "PgCyanPreData":
    """Build a ``PgCyanPreData`` from the environment (container defaults)."""
    return PgCyanPreData(
        source_pguri=os.getenv("PGCOPYDB_SOURCE_PGURI") or None,
        target_pguri=os.getenv("PGCOPYDB_TARGET_PGURI") or None,
        snapshot_id=os.getenv("SNAPSHOT_ID") or None,
        work_dir=os.getenv("PGCYAN_WORK_DIR", PgCyanPreData.DEFAULT_WORK_DIR),
        output_dir=os.getenv("PG_OUTPUT_DIR") or None,
        not_consistent=os.getenv("PGCYAN_NOT_CONSISTENT", "").lower() in ("1", "true", "yes"),
    )


if __name__ == "__main__":
    sys.exit(restore_predata_from_env())
