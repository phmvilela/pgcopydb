"""
Snapshot holder for the sharded pgcopydb topology (issue #23).

This is the phase ``1`` node in ``deploy/k8s/`` (``50-deployment-snapshot-
holder.yaml``): the single most safety-critical object in the migration. It
establishes -- and then **holds** -- the consistent logical-replication setup on
the *source* database in one coordinated step, so every downstream copy phase
pins the exact same instant:

  1. create the publication ``FOR ALL TABLES`` (idempotent),
  2. create the ``pgoutput`` logical replication slot and export its
     **consistent snapshot**,
  3. keep the exporting connection open so the snapshot stays valid for the
     pre-data (#24) and sharded copy (#25) phases.

WHY a long-lived holder: a PostgreSQL exported snapshot is only valid while the
transaction that exported it stays open. The moment that connection closes,
``SET TRANSACTION SNAPSHOT`` in the shards fails and each shard would otherwise
fall back to its OWN point in time -> silent cross-shard inconsistency. Because
the shards are shared-nothing separate pods, none of them can own the snapshot
on behalf of the others; this pod does. It runs as a Deployment (replicas=1 +
PDB): if it dies the snapshot is gone and the in-flight initial copy must
``--restart`` from a new snapshot.

Prefer driving pgcopydb (a thin wrapper) over hand-rolled ``CREATE_REPLICATION
_SLOT`` SQL: the slot + snapshot are created and held by ``pgcopydb snapshot
--follow --plugin pgoutput --slot-name <slot>`` as a child process, which also
writes the snapshot id to ``<work-dir>/snapshot`` on the shared RWX ``/work``
volume (``20-pvc-workdir.yaml``) and closes the replication connection cleanly
when signalled -- so a graceful pod exit leaves no stale connection behind.
the holder creates its own publication ``FOR ALL TABLES``. Separately, pgcopydb's
``snapshot --follow --plugin pgoutput`` auto-manages a publication of its OWN,
named after the slot (``snapshot`` has no ``--publication`` flag) and created
with a non-idempotent ``CREATE PUBLICATION``; ``drop_stale_publication`` clears
any leftover of that name from a prior partial run so pgcopydb can recreate it.
The holder's own publication is a single ``CREATE PUBLICATION``, not the discouraged
hand-rolled slot SQL) before starting the child.

This node does NOT create or advance any replication origin -- that is the
follow-CDC phase's job.

Outputs (written to ``--output-dir`` / ``$PG_OUTPUT_DIR``, defaulting to the
work dir so both land on the shared ``/work`` volume):
  * ``source-pg-publication.json`` -- publication name (+ FOR ALL TABLES).
  * ``source-pg-slot.json``        -- slot name, snapshot name, plugin, and the
                                      captured ``consistent_point`` LSN.
On any failure a matching ``*-errors.json`` is written instead (the same
success-vs-error two-file shape as the other stages; see ``CLAUDE.md``).

Follows the ``PgCyanMetrics`` / ``PgCyanParameters`` orchestrator pattern:
structured JSON logging via ``_log_event`` and graceful degradation --
``establish()`` never raises. ``hold()`` blocks until SIGTERM/SIGINT, then
terminates the pgcopydb child and closes connections.
"""
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from psycopg2 import sql
from psycopg2 import Error as PgError

# Structured JSON logging (mirrors the other stages).
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)


class PgCyanSnapshot:
    """Establishes and holds the source's consistent publication + slot + snapshot."""

    #: Written on the happy path (non-empty).
    SLOT_OUTPUT_FILENAME = "source-pg-slot.json"
    PUBLICATION_OUTPUT_FILENAME = "source-pg-publication.json"
    #: Written when the respective step fails, with an error message.
    SLOT_ERROR_FILENAME = "source-pg-slot-errors.json"
    PUBLICATION_ERROR_FILENAME = "source-pg-publication-errors.json"

    #: Basename pgcopydb writes the exported snapshot id to, under ``work_dir``.
    SNAPSHOT_FILENAME = "snapshot"

    #: Defaults kept in lock-step with ``PgCyanPlanner`` so the holder and the
    #: plan agree on names/mode when a plan is not passed explicitly.
    DEFAULT_PUBLICATION = "pgcyan_pub"
    DEFAULT_SLOT = "pgcyan_slot"
    DEFAULT_SNAPSHOT_MODE = "coordinator-held"
    DEFAULT_PLUGIN = "pgoutput"
    DEFAULT_WORK_DIR = "/work/coordinator"
    DEFAULT_PGCOPYDB_BIN = "pgcopydb"

    #: How long to wait for the child to export the snapshot before giving up.
    DEFAULT_SNAPSHOT_TIMEOUT = 120
    #: How long to give the child to close its connection on shutdown.
    DEFAULT_SHUTDOWN_TIMEOUT = 30

    def __init__(
        self,
        source_pguri: Optional[str] = None,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "",
        database: str = "postgres",
        timeout: int = 30,
        publication_name: str = DEFAULT_PUBLICATION,
        slot_name: str = DEFAULT_SLOT,
        snapshot_mode: str = DEFAULT_SNAPSHOT_MODE,
        plugin: str = DEFAULT_PLUGIN,
        work_dir: str = DEFAULT_WORK_DIR,
        output_dir: Optional[str] = None,
        plan_file: Optional[str] = None,
        pgcopydb_bin: str = DEFAULT_PGCOPYDB_BIN,
        snapshot_timeout: int = DEFAULT_SNAPSHOT_TIMEOUT,
        shutdown_timeout: int = DEFAULT_SHUTDOWN_TIMEOUT,
    ):
        """
        Initialize the snapshot holder.

        Args:
            source_pguri: libpq connection URI for the source (pgcopydb's native
                ``PGCOPYDB_SOURCE_PGURI``). When set it is used verbatim for the
                publication/consistent-point queries and passed to the child via
                ``--source``; otherwise the discrete ``host``/``port``/... are used.
            host, port, user, password, database, timeout: discrete connection
                settings used when ``source_pguri`` is not provided.
            publication_name: publication to create ``FOR ALL TABLES``.
            slot_name: logical replication slot to create.
            snapshot_mode: recorded in the outputs (held vs frozen-source).
            plugin: logical decoding output plugin (``pgoutput``).
            work_dir: pgcopydb ``--dir``; the snapshot id is written here.
            output_dir: where the JSON outputs land; defaults to ``work_dir``.
            plan_file: optional ``plan.json`` to source publication/slot/mode from.
            pgcopydb_bin: pgcopydb executable name/path.
            snapshot_timeout: seconds to wait for the child to export the snapshot.
            shutdown_timeout: seconds to let the child close its connection.
        """
        self.source_pguri = source_pguri
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.snapshot_mode = snapshot_mode
        self.plugin = plugin
        self.work_dir = work_dir
        self.output_dir = output_dir if output_dir is not None else work_dir
        self.plan_file = plan_file
        self.pgcopydb_bin = pgcopydb_bin
        self.snapshot_timeout = snapshot_timeout
        self.shutdown_timeout = shutdown_timeout

        self.conn = None
        self.proc: Optional[subprocess.Popen] = None
        self.errors: List[str] = []
        self._stop_requested = False

    # --------------------------------------------------------------- logging

    def _log_event(
        self,
        event_type: str,
        status: str,
        message: str,
        details: Optional[Dict] = None,
    ) -> None:
        """Log a structured JSON event (mirrors the other stages)."""
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "snapshot-holder",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }
        logger.info(json.dumps(event))

    # ------------------------------------------------------------------ plan

    def load_plan_overrides(self) -> None:
        """
        Override publication/slot/mode from ``plan.json`` when one is supplied.

        The planner (#38) is the authority for these names and the snapshot
        mode; when the orchestrator hands the holder the plan, prefer its values
        over the option/env defaults. Missing/unreadable plan is a silent no-op
        (the defaults still apply) -- never raises.
        """
        if not self.plan_file:
            return
        try:
            with open(self.plan_file, encoding="utf-8") as fh:
                plan = json.load(fh)
        except (OSError, ValueError) as e:
            self._log_event(
                "load_plan",
                "warning",
                f"Could not read plan file {self.plan_file}; using option/env defaults",
                {"path": self.plan_file, "error": str(e)},
            )
            return

        publication = (plan.get("publication") or {}).get("name")
        slot = (plan.get("slot") or {}).get("name")
        mode = (plan.get("snapshot") or {}).get("mode")
        if publication:
            self.publication_name = publication
        if slot:
            self.slot_name = slot
        if mode:
            self.snapshot_mode = mode
        self._log_event(
            "load_plan",
            "success",
            "Applied publication/slot/mode from plan.json",
            {
                "publication": self.publication_name,
                "slot": self.slot_name,
                "snapshot_mode": self.snapshot_mode,
            },
        )

    @property
    def not_consistent(self) -> bool:
        """Frozen-source mode (each shard takes its own snapshot); recorded, not held."""
        return self.snapshot_mode != self.DEFAULT_SNAPSHOT_MODE

    @property
    def snapshot_path(self) -> str:
        """
        Absolute path pgcopydb writes the exported snapshot id to.

        This is under ``work_dir`` -- pgcopydb's private ``--dir``. In the pod
        that is a node-local ``emptyDir`` (see below), so the id is then
        *published* to the shared volume via ``published_snapshot_path`` for the
        downstream shards to read.
        """
        return os.path.join(self.work_dir, self.SNAPSHOT_FILENAME)

    @property
    def published_snapshot_path(self) -> str:
        """
        Shared-volume path the snapshot id is published to for the shards.

        pgcopydb keeps a **SQLite** catalog under ``--dir`` and opens it in WAL
        mode, which needs file locking + shared memory that **Azure Files (SMB)
        does not provide** -- on the shared RWX PVC pgcopydb aborts with
        ``PRAGMA journal_mode = WAL failed: database is locked``. So ``work_dir``
        (pgcopydb's ``--dir``) lives on a node-local ``emptyDir`` while the
        handoff artifacts land in ``output_dir`` on the shared ``/work`` volume.
        The shards read ``<output_dir>/snapshot``; when the two dirs differ we
        copy the id there after pgcopydb exports it.
        """
        return os.path.join(self.output_dir, self.SNAPSHOT_FILENAME)

    # ------------------------------------------------------------- work dir

    def reset_work_dir(self) -> None:
        """
        Wipe any state a PRIOR holder left behind before launching.

        pgcopydb's ``--dir`` (``work_dir``) is a node-local ``emptyDir`` in the
        pod, but the shared ``output_dir`` on the ``/work`` PVC persists across
        runs. A leftover pgcopydb catalog would push pgcopydb into *resume* mode
        and -- because a consistent snapshot cannot be resumed -- abort with
        ``Option --resume requires option --not-consistent`` (issue #23). The
        holder is authoritative and non-resumable by design (a restart == a
        brand-new snapshot), so it must always start from a clean dir.

        This also removes any stale ``source-pg-slot.json`` / published snapshot
        id from a prior run in ``output_dir``, which would otherwise satisfy the
        readiness probe before *this* run has established its own snapshot. Never
        raises -- a dir we cannot fully clear is a loud warning, not a hard
        failure (pgcopydb may still succeed).
        """
        try:
            if os.path.isdir(self.work_dir):
                for entry in os.listdir(self.work_dir):
                    path = os.path.join(self.work_dir, entry)
                    if os.path.isdir(path) and not os.path.islink(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        try:
                            os.remove(path)
                        except OSError:
                            pass
            os.makedirs(self.work_dir, exist_ok=True)
            # If outputs land elsewhere, still clear stale artifacts there so a
            # prior run's slot file cannot prematurely satisfy the readiness probe.
            if os.path.abspath(self.output_dir) != os.path.abspath(self.work_dir):
                for name in (
                    self.SLOT_OUTPUT_FILENAME, self.PUBLICATION_OUTPUT_FILENAME,
                    self.SLOT_ERROR_FILENAME, self.PUBLICATION_ERROR_FILENAME,
                    self.SNAPSHOT_FILENAME,
                ):
                    try:
                        os.remove(os.path.join(self.output_dir, name))
                    except OSError:
                        pass
            self._log_event(
                "reset_work_dir", "success",
                "Cleared any prior holder state from the work dir",
                {"work_dir": self.work_dir},
            )
        except OSError as e:
            self._log_event(
                "reset_work_dir", "warning",
                "Could not fully clear the work dir; pgcopydb may enter resume mode",
                {"work_dir": self.work_dir, "error": str(e),
                 "remediation": "Ensure the /work PVC is writable and no prior holder still has the dir open"},
            )

    # ------------------------------------------------------------ connection

    def connect(self) -> bool:
        """
        Open the source connection used for the publication + consistent-point.

        This connection is short-lived: it is closed as soon as the outputs are
        written, so the pgcopydb child is the *only* holder of a long-lived
        connection (and it closes cleanly on shutdown -> no stale connections).
        Never raises.
        """
        try:
            if self.source_pguri:
                self.conn = psycopg2.connect(
                    self.source_pguri, connect_timeout=self.timeout
                )
                target = self.source_pguri
            else:
                self.conn = psycopg2.connect(
                    host=self.host, port=self.port, user=self.user,
                    password=self.password, database=self.database,
                    connect_timeout=self.timeout,
                )
                target = f"{self.host}:{self.port}/{self.database}"
            # DDL (CREATE PUBLICATION) is simplest with autocommit on.
            self.conn.autocommit = True
            self._log_event("connection", "success", f"Connected to source {target}")
            return True
        except PgError as e:
            self._log_event(
                "connection",
                "failure",
                "Failed to connect to the source database",
                {"error": str(e),
                 "remediation": "Check PGCOPYDB_SOURCE_PGURI / PG_SOURCE_* and network connectivity"},
            )
            self.errors.append(f"Connection failed: {str(e)}")
            return False
        except Exception as e:  # noqa: BLE001 -- degrade, never raise
            self._log_event(
                "connection", "failure", "Unexpected error during connection",
                {"error": str(e)},
            )
            self.errors.append(f"Unexpected connection error: {str(e)}")
            return False

    # ----------------------------------------------------------- publication

    def create_publication(self) -> bool:
        """
        Create the publication ``FOR ALL TABLES`` (idempotent).

        This is the holder's own publication (``publication_name``); it is
        distinct from the slot-named publication pgcopydb auto-manages for its
        pgoutput slot (see ``drop_stale_publication`` /
        ``build_pgcopydb_command``). Re-running against an existing publication of
        the same name is a success (the migration is resumable).
        Identifiers are composed
        with ``psycopg2.sql`` -- never f-stringed into SQL. Returns ``False`` on
        any DB error (a ``source-pg-publication-errors.json`` is written by the
        caller).
        """
        if not self.conn:
            self.errors.append("Cannot create publication: no source connection")
            return False
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (self.publication_name,),
                )
                if cur.fetchone():
                    self._log_event(
                        "create_publication",
                        "success",
                        f"Publication {self.publication_name} already exists; reusing it",
                        {"publication": self.publication_name},
                    )
                    return True
                cur.execute(
                    sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(
                        sql.Identifier(self.publication_name)
                    )
                )
            self._log_event(
                "create_publication",
                "success",
                f"Created publication {self.publication_name} FOR ALL TABLES",
                {"publication": self.publication_name},
            )
            return True
        except PgError as e:
            self._log_event(
                "create_publication",
                "failure",
                f"Failed to create publication {self.publication_name}",
                {"publication": self.publication_name, "error": str(e),
                 "remediation": "The role needs privileges to CREATE PUBLICATION FOR ALL TABLES (superuser/azure_pg_admin)"},
            )
            self.errors.append(f"Publication creation failed: {str(e)}")
            return False

    # ------------------------------------------------------------------ slot

    def drop_stale_slot(self) -> bool:
        """
        Drop a leftover replication slot of the same name before creating a fresh one.

        A logical slot is persistent: a previously successful holder leaves
        ``slot_name`` on the source even after it exits cleanly. pgcopydb's
        ``snapshot --follow`` issues ``CREATE_REPLICATION_SLOT``, which would
        fail ("slot already exists") -- and, more importantly, a slot's
        *consistent snapshot* is only exported at creation time, so reusing an
        old slot cannot yield a fresh consistent point. The holder must
        therefore start from no slot. An *active* slot means another holder (or
        a leaked connection) still owns it: dropping is impossible and
        proceeding is unsafe, so that is a hard failure. Returns ``False`` only
        on a real error; a missing slot is the happy path. Never raises.
        """
        if not self.conn:
            self.errors.append("Cannot check for a stale slot: no source connection")
            return False
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot_name,),
                )
                row = cur.fetchone()
                if not row:
                    return True  # nothing to clean up -- the slot name is free
                active, active_pid = row[0], row[1]
                if active:
                    self._log_event(
                        "drop_stale_slot", "failure",
                        f"Replication slot {self.slot_name} is still ACTIVE (pid {active_pid})",
                        {"slot": self.slot_name, "active_pid": active_pid,
                         "remediation": "Another snapshot holder or a leaked connection owns the slot; fully tear down the prior holder (kubectl delete deploy --wait) before retrying"},
                    )
                    self.errors.append(
                        f"Replication slot {self.slot_name} is active (pid {active_pid}); refusing to drop"
                    )
                    return False
                cur.execute("SELECT pg_drop_replication_slot(%s)", (self.slot_name,))
            self._log_event(
                "drop_stale_slot", "success",
                f"Dropped stale replication slot {self.slot_name} from a prior run",
                {"slot": self.slot_name},
            )
            return True
        except PgError as e:
            self._log_event(
                "drop_stale_slot", "failure",
                f"Failed to drop the stale replication slot {self.slot_name}",
                {"slot": self.slot_name, "error": str(e),
                 "remediation": "Drop it manually once inactive: SELECT pg_drop_replication_slot('<slot>');"},
            )
            self.errors.append(f"Failed to drop stale slot: {str(e)}")
            return False

    def drop_stale_publication(self) -> bool:
        """
        Drop pgcopydb's auto-managed, slot-named publication before pgcopydb runs.

        With ``--plugin pgoutput`` and no ``--publication`` flag (``snapshot`` has
        none), pgcopydb derives the publication name from the slot name and
        creates it with a plain, **non-idempotent** ``CREATE PUBLICATION``
        (pgcopydb ``cli_common.c`` / ``snapshot.c``). A leftover of that name from
        a prior partial run makes pgcopydb abort with ``Failed to create
        publication "<slot>"``. ``DROP PUBLICATION IF EXISTS`` clears it so the
        auto-create succeeds on every run -- this drops the *slot-named*
        publication, NOT the holder's own ``publication_name``. Never raises;
        returns ``False`` only on a real DB error (a missing publication is the
        happy path -- ``IF EXISTS`` makes it a no-op).
        """
        if not self.conn:
            self.errors.append("Cannot drop the stale publication: no source connection")
            return False
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP PUBLICATION IF EXISTS {}").format(
                        sql.Identifier(self.slot_name)
                    )
                )
            self._log_event(
                "drop_stale_publication", "success",
                f"Cleared any leftover pgcopydb-managed publication {self.slot_name}",
                {"publication": self.slot_name},
            )
            return True
        except PgError as e:
            self._log_event(
                "drop_stale_publication", "failure",
                f"Failed to drop the stale publication {self.slot_name}",
                {"publication": self.slot_name, "error": str(e),
                 "remediation": "Drop it manually: DROP PUBLICATION IF EXISTS \"<slot>\";"},
            )
            self.errors.append(f"Failed to drop stale publication: {str(e)}")
            return False

    # --------------------------------------------------------------- snapshot

    def build_pgcopydb_command(self) -> List[str]:
        """
        Build the ``pgcopydb snapshot --follow`` invocation.

        ``--follow`` makes pgcopydb create the slot via the replication protocol
        (``CREATE_REPLICATION_SLOT``) and export its consistent snapshot, rather
        than a plain ``pg_export_snapshot()`` -- the WAL is reserved so the slot
        can decode from exactly the snapshot's point. It then holds the
        connection open until signalled.

        NB: ``pgcopydb snapshot`` has **no** ``--publication`` option (only
        ``clone``/``follow`` do). With ``--plugin pgoutput`` and no publication
        given, pgcopydb auto-manages a publication whose name it derives from the
        SLOT name (``cli_common.c``: ``publicationName = slotName``) and creates
        it with a plain, non-idempotent ``CREATE PUBLICATION`` (``snapshot.c`` ->
        ``pgsql_create_publication``). A leftover publication of that name from a
        prior partial run makes that CREATE fail (``Failed to create publication
        "<slot>"``); ``drop_stale_publication`` clears it first.
        """
        cmd = [
            self.pgcopydb_bin, "snapshot",
            "--follow",
            "--plugin", self.plugin,
            "--slot-name", self.slot_name,
            "--dir", self.work_dir,
        ]
        # pgcopydb also reads PGCOPYDB_SOURCE_PGURI from the env; pass --source
        # explicitly only when we were given the URI, so both paths agree.
        if self.source_pguri:
            cmd += ["--source", self.source_pguri]
        return cmd

    def start_snapshot(self) -> bool:
        """Launch the pgcopydb child that creates + holds the slot/snapshot."""
        try:
            os.makedirs(self.work_dir, exist_ok=True)
        except OSError as e:
            self._log_event(
                "start_snapshot", "failure",
                f"Could not create work dir {self.work_dir}",
                {"path": self.work_dir, "error": str(e)},
            )
            self.errors.append(f"Work dir creation failed: {str(e)}")
            return False

        # A stale snapshot file from a prior pod would make wait_for_snapshot
        # return immediately with a dead identifier -- remove it first.
        try:
            if os.path.exists(self.snapshot_path):
                os.remove(self.snapshot_path)
        except OSError:
            pass

        cmd = self.build_pgcopydb_command()
        self._log_event(
            "start_snapshot", "success",
            "Starting pgcopydb snapshot holder",
            {"command": " ".join(cmd)},
        )
        try:
            self.proc = subprocess.Popen(cmd)  # noqa: S603 -- fixed argv, no shell
            return True
        except (OSError, ValueError) as e:
            self._log_event(
                "start_snapshot", "failure",
                "Failed to launch pgcopydb",
                {"command": " ".join(cmd), "error": str(e),
                 "remediation": "Confirm the pgcopydb binary is on PATH in the image"},
            )
            self.errors.append(f"Failed to launch pgcopydb: {str(e)}")
            return False

    def wait_for_snapshot(self) -> Optional[str]:
        """
        Block until the child writes the snapshot id, and return it.

        Polls for a non-empty ``<work-dir>/snapshot`` while the child is alive.
        Returns the snapshot identifier, or ``None`` if the child died first or
        the timeout elapsed.
        """
        deadline = time.monotonic() + self.snapshot_timeout
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                self._log_event(
                    "wait_for_snapshot", "failure",
                    "pgcopydb exited before exporting a snapshot",
                    {"returncode": self.proc.returncode,
                     "remediation": "Check the pgcopydb logs above; verify slot/publication privileges and that the slot name is free"},
                )
                self.errors.append(
                    f"pgcopydb exited early (rc={self.proc.returncode}) before exporting a snapshot"
                )
                return None
            snapshot_id = self._read_snapshot_file()
            if snapshot_id:
                self._log_event(
                    "wait_for_snapshot", "success",
                    "Snapshot exported and held",
                    {"snapshot_name": snapshot_id, "path": self.snapshot_path},
                )
                return snapshot_id
            time.sleep(1)

        self._log_event(
            "wait_for_snapshot", "failure",
            f"Timed out after {self.snapshot_timeout}s waiting for the snapshot",
            {"path": self.snapshot_path},
        )
        self.errors.append(f"Timed out waiting for snapshot after {self.snapshot_timeout}s")
        return None

    def _read_snapshot_file(self) -> Optional[str]:
        """Return the snapshot id from ``<work-dir>/snapshot``, or ``None``."""
        try:
            with open(self.snapshot_path, encoding="utf-8") as fh:
                content = fh.read().strip()
            return content or None
        except OSError:
            return None

    def publish_snapshot_id(self, snapshot_id: str) -> Optional[str]:
        """
        Copy the exported snapshot id onto the shared volume for the shards.

        pgcopydb writes the id to its private ``--dir`` (``snapshot_path``),
        which is a node-local ``emptyDir`` so the SQLite/WAL catalog stays off
        Azure Files (SMB). The downstream shards read ``published_snapshot_path``
        on the shared ``/work`` volume, so once the id exists we mirror it there.
        When ``output_dir`` and ``work_dir`` coincide the file is already in
        place. Returns the shared path, or ``None`` if the copy failed (a failure
        means the shards could not learn the snapshot, so the caller aborts).
        """
        dest = self.published_snapshot_path
        if os.path.abspath(dest) == os.path.abspath(self.snapshot_path):
            return dest  # pgcopydb already wrote it where the shards will look
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(dest, "w", encoding="utf-8") as fh:
                fh.write(snapshot_id + "\n")
            self._log_event(
                "publish_snapshot", "success",
                "Published the snapshot id to the shared volume for the shards",
                {"snapshot_name": snapshot_id, "path": dest},
            )
            return dest
        except OSError as e:
            self._log_event(
                "publish_snapshot", "failure",
                "Failed to publish the snapshot id to the shared volume",
                {"path": dest, "error": str(e),
                 "remediation": "The shards read this file to pass --snapshot; ensure PG_OUTPUT_DIR is on the shared /work volume and writable"},
            )
            self.errors.append(f"Failed to publish snapshot id: {str(e)}")
            return None

    def capture_consistent_point(self) -> Optional[str]:
        """
        Best-effort read of the slot's ``restart_lsn`` as its consistent point.

        The exact ``consistent_point`` is returned by ``CREATE_REPLICATION_SLOT``
        (issued by the child); after creation the slot's ``restart_lsn`` in
        ``pg_replication_slots`` reflects it. Never raises -- an unavailable LSN
        just leaves the field ``None`` in the output.
        """
        if not self.conn:
            return None
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot_name,),
                )
                row = cur.fetchone()
            return row[0] if row and row[0] else None
        except PgError as e:
            self._log_event(
                "consistent_point", "warning",
                "Could not read the slot's restart_lsn; leaving consistent_point unset",
                {"slot": self.slot_name, "error": str(e)},
            )
            return None

    # ------------------------------------------------------------- assembly

    def _publication_output(self) -> Dict:
        """Assemble the ``source-pg-publication.json`` payload."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "snapshot-holder",
            "status": "success",
            "publication": {
                "name": self.publication_name,
                "all_tables": True,
            },
            "snapshot": {"mode": self.snapshot_mode, "not_consistent": self.not_consistent},
        }

    def _slot_output(self, snapshot_name: str, consistent_point: Optional[str]) -> Dict:
        """Assemble the ``source-pg-slot.json`` payload."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "snapshot-holder",
            "status": "success",
            "slot": {
                "name": self.slot_name,
                "plugin": self.plugin,
                "snapshot_name": snapshot_name,
                "consistent_point": consistent_point,
            },
            "snapshot": {
                "name": snapshot_name,
                "mode": self.snapshot_mode,
                "not_consistent": self.not_consistent,
                "id_file": self.published_snapshot_path,
            },
            "publication": {"name": self.publication_name},
        }

    def _build_error(self, message: str) -> Dict:
        """Assemble a ``*-errors.json`` payload."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "snapshot-holder",
            "status": "failure",
            "message": message,
            "errors": self.errors or ["Unknown error"],
            "remediation": "Check the messages above; a holder failure means the initial copy must --restart",
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

    # ------------------------------------------------------------- establish

    def establish(self) -> Dict:
        """
        Create the publication + slot + snapshot and write the output files.

        Never raises. Leaves the pgcopydb child running (holding the snapshot)
        on success -- call ``hold()`` next to block until shutdown. On failure
        the child (if any) is terminated so nothing is left holding a
        connection. Returns a summary dict:
            {
              "status": "success" | "failure",
              "publication_file": <path or None>,
              "slot_file": <path or None>,
              "publication_error_file": <path or None>,
              "slot_error_file": <path or None>,
              "publication": <name>, "slot": <name>,
              "snapshot_name": <str or None>,
              "snapshot_mode": <str>,
              "errors": [...],
            }
        """
        self.load_plan_overrides()
        # Start from a clean slate: the /work PVC persists across runs, and the
        # holder is non-resumable by design (a restart == a new snapshot). A
        # leftover pgcopydb catalog here would push pgcopydb into resume mode and
        # abort the consistent snapshot (issue #23); a leftover slot file would
        # trip the readiness probe prematurely.
        self.reset_work_dir()
        summary = {
            "status": "failure",
            "publication_file": None,
            "slot_file": None,
            "publication_error_file": None,
            "slot_error_file": None,
            "publication": self.publication_name,
            "slot": self.slot_name,
            "snapshot_name": None,
            "snapshot_mode": self.snapshot_mode,
            "errors": self.errors,
        }

        try:
            if not self.connect():
                summary["publication_error_file"] = self._write_json(
                    self.PUBLICATION_ERROR_FILENAME,
                    self._build_error("Failed to connect to the source database"),
                )
                return summary

            # 1) Publication -- the holder owns it (pgcopydb snapshot does not).
            if not self.create_publication():
                summary["publication_error_file"] = self._write_json(
                    self.PUBLICATION_ERROR_FILENAME,
                    self._build_error(f"Failed to create publication {self.publication_name}"),
                )
                return summary
            summary["publication_file"] = self._write_json(
                self.PUBLICATION_OUTPUT_FILENAME, self._publication_output()
            )
            if not summary["publication_file"]:
                summary["publication_error_file"] = self._write_json(
                    self.PUBLICATION_ERROR_FILENAME,
                    self._build_error("Failed to write source-pg-publication.json"),
                )
                return summary

            # 2) Free the slot name -- a prior successful holder leaves a
            # persistent slot behind, and a fresh consistent snapshot needs a
            # freshly-created slot. Drop any inactive leftover before pgcopydb
            # issues CREATE_REPLICATION_SLOT.
            if not self.drop_stale_slot():
                summary["slot_error_file"] = self._write_json(
                    self.SLOT_ERROR_FILENAME,
                    self._build_error(f"Could not free the replication slot {self.slot_name}"),
                )
                return summary

            # 2b) Free the slot-NAMED publication pgcopydb auto-creates. Its
            # CREATE is non-idempotent, so a leftover from a prior partial run
            # would make `pgcopydb snapshot --follow` abort ("Failed to create
            # publication"). Distinct from the holder's own publication above.
            if not self.drop_stale_publication():
                summary["slot_error_file"] = self._write_json(
                    self.SLOT_ERROR_FILENAME,
                    self._build_error(f"Could not free the pgcopydb publication {self.slot_name}"),
                )
                return summary

            # 3) Slot + snapshot via pgcopydb, held open by the child.
            if not self.start_snapshot():
                summary["slot_error_file"] = self._write_json(
                    self.SLOT_ERROR_FILENAME,
                    self._build_error("Failed to start the pgcopydb snapshot holder"),
                )
                return summary

            snapshot_name = self.wait_for_snapshot()
            if not snapshot_name:
                # The child failed to export a snapshot -> tear it down.
                self._terminate_child()
                summary["slot_error_file"] = self._write_json(
                    self.SLOT_ERROR_FILENAME,
                    self._build_error("pgcopydb did not export a consistent snapshot"),
                )
                return summary

            # 4) Publish the id onto the shared volume so the shards can read it
            # (pgcopydb wrote it to its node-local --dir). A failure here means no
            # shard can learn the snapshot -> abort rather than leak the child.
            if not self.publish_snapshot_id(snapshot_name):
                self._terminate_child()
                summary["slot_error_file"] = self._write_json(
                    self.SLOT_ERROR_FILENAME,
                    self._build_error("Failed to publish the snapshot id to the shared volume"),
                )
                return summary

            # 5) Capture the consistent point, then release our own connection --
            # only the pgcopydb child should hold a long-lived connection.
            consistent_point = self.capture_consistent_point()
            self.close_connection()

            slot_path = self._write_json(
                self.SLOT_OUTPUT_FILENAME, self._slot_output(snapshot_name, consistent_point)
            )
            summary["slot_file"] = slot_path
            summary["snapshot_name"] = snapshot_name
            if slot_path and summary["publication_file"]:
                summary["status"] = "success"
                self._log_event(
                    "establish", "success",
                    "Consistent snapshot established and held",
                    {"publication": self.publication_name, "slot": self.slot_name,
                     "snapshot_name": snapshot_name, "consistent_point": consistent_point,
                     "snapshot_mode": self.snapshot_mode},
                )
                return summary

            # A write failed after the snapshot was exported -> don't leak the child.
            self._terminate_child()
            summary["slot_error_file"] = self._write_json(
                self.SLOT_ERROR_FILENAME,
                self._build_error("Failed to write the slot/publication output files"),
            )
            return summary
        except Exception as e:  # noqa: BLE001 -- degrade, never raise
            self._log_event(
                "establish", "failure", "Unexpected error while establishing the snapshot",
                {"error": str(e)},
            )
            self.errors.append(f"Establish error: {str(e)}")
            self._terminate_child()
            summary["slot_error_file"] = self._write_json(
                self.SLOT_ERROR_FILENAME, self._build_error("Unexpected error while establishing the snapshot")
            )
            return summary

    # ----------------------------------------------------------------- hold

    def hold(self) -> int:
        """
        Block until SIGTERM/SIGINT, then shut the holder down cleanly.

        This is the whole point of the phase-1 pod: keep the exporting
        transaction open so the snapshot stays valid for phases 0 and 2. On a
        signal it terminates the pgcopydb child, which closes the replication
        connection -- so a graceful pod exit leaves no stale source connection.
        Returns the child's exit code (0 on a clean, signalled shutdown).
        """
        if self.proc is None:
            return 0

        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)

        self._log_event(
            "hold", "success",
            "Holding the consistent snapshot; waiting for a termination signal",
            {"slot": self.slot_name, "snapshot_path": self.snapshot_path},
        )
        # Wait for the child; a signal interrupts the wait, terminates the child,
        # and the loop then observes it has exited.
        while True:
            try:
                returncode = self.proc.wait()
                break
            except KeyboardInterrupt:
                self._on_signal(signal.SIGINT, None)
            except InterruptedError:
                continue

        if not self._stop_requested and returncode != 0:
            self._log_event(
                "hold", "failure",
                "pgcopydb snapshot holder exited unexpectedly (snapshot lost)",
                {"returncode": returncode,
                 "remediation": "The snapshot is gone; the in-flight initial copy must --restart"},
            )
        else:
            self._log_event(
                "hold", "success", "Snapshot holder stopped; source connection released",
                {"returncode": returncode},
            )
        self.close_connection()
        return returncode

    def _on_signal(self, signum, _frame) -> None:
        """Signal handler: terminate the child so the snapshot connection closes."""
        self._stop_requested = True
        self._log_event(
            "shutdown", "success",
            f"Received signal {signum}; terminating the snapshot holder",
            {"signal": signum},
        )
        self._terminate_child()

    def _terminate_child(self) -> None:
        """SIGTERM the pgcopydb child and reap it, escalating to SIGKILL."""
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            self.proc.terminate()
        except OSError:
            return
        try:
            self.proc.wait(timeout=self.shutdown_timeout)
        except subprocess.TimeoutExpired:
            self._log_event(
                "shutdown", "warning",
                "pgcopydb did not exit in time; sending SIGKILL",
                {"timeout": self.shutdown_timeout},
            )
            try:
                self.proc.kill()
                self.proc.wait(timeout=self.shutdown_timeout)
            except (OSError, subprocess.TimeoutExpired):
                pass

    def close_connection(self) -> None:
        """Close the short-lived source connection (if still open)."""
        if self.conn:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001
                pass
            finally:
                self.conn = None

    def close(self) -> None:
        """Release all resources: terminate the child and close the connection."""
        self._terminate_child()
        self.close_connection()


def hold_snapshot_from_env() -> int:
    """
    Establish and hold the consistent snapshot using environment-driven defaults.

    Returns 0 once the holder has established the snapshot and then exited
    cleanly on a termination signal; 1 if it could not be established.
    """
    holder = _holder_from_env()
    summary = holder.establish()
    print(json.dumps(summary, indent=2, default=str))
    if summary["status"] != "success":
        holder.close()
        return 1
    return 0 if holder.hold() == 0 else 1


def _holder_from_env() -> "PgCyanSnapshot":
    """Build a ``PgCyanSnapshot`` from the ``PGCYAN_*`` / ``PG_SOURCE_*`` env vars."""
    work_dir = os.getenv("PGCYAN_WORK_DIR", PgCyanSnapshot.DEFAULT_WORK_DIR)
    return PgCyanSnapshot(
        source_pguri=os.getenv("PGCOPYDB_SOURCE_PGURI") or None,
        host=os.getenv("PG_SOURCE_HOST", "localhost"),
        port=int(os.getenv("PG_SOURCE_PORT", "5432")),
        user=os.getenv("PG_SOURCE_USER", "postgres"),
        password=os.getenv("PG_SOURCE_PASSWORD", ""),
        database=os.getenv("PG_SOURCE_DATABASE", "postgres"),
        timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
        publication_name=os.getenv("PGCYAN_PUBLICATION", PgCyanSnapshot.DEFAULT_PUBLICATION),
        slot_name=os.getenv("PGCYAN_SLOT", PgCyanSnapshot.DEFAULT_SLOT),
        snapshot_mode=os.getenv("PGCYAN_SNAPSHOT_MODE", PgCyanSnapshot.DEFAULT_SNAPSHOT_MODE),
        plugin=os.getenv("PGCYAN_PLUGIN", PgCyanSnapshot.DEFAULT_PLUGIN),
        work_dir=work_dir,
        output_dir=os.getenv("PG_OUTPUT_DIR", work_dir),
        plan_file=os.getenv("PGCYAN_PLAN_FILE") or None,
        pgcopydb_bin=os.getenv("PGCOPYDB_BIN", PgCyanSnapshot.DEFAULT_PGCOPYDB_BIN),
        snapshot_timeout=int(os.getenv("PGCYAN_SNAPSHOT_TIMEOUT", str(PgCyanSnapshot.DEFAULT_SNAPSHOT_TIMEOUT))),
        shutdown_timeout=int(os.getenv("PGCYAN_SHUTDOWN_TIMEOUT", str(PgCyanSnapshot.DEFAULT_SHUTDOWN_TIMEOUT))),
    )


if __name__ == "__main__":
    sys.exit(hold_snapshot_from_env())
