"""
Change Data Capture / follow -- the controller's CDC consumer (Option A).

After the controller's DAG walk finishes (copy + post-data), the controller releases
its held snapshot and then streams changes with ``pgcopydb follow`` -- the ongoing
replication tail that applies everything written since the copy's snapshot to the
target, until an operator flips the endpoint (cutover).

WHY THE CONTROLLER STREAMS (not a separate pod). pgcopydb's CDC state -- the slot
record, the ``sentinel`` (resume coordinates), and the received/transformed change
stores -- lives in the ``--dir`` SQLite catalog. ``pgcopydb follow`` creates its
slot NON-idempotently (no flag reuses a pre-existing one); its sole reuse path
requires the slot's lsn+snapshot to already be present in the ``--dir`` ``source.db``
catalog. So the slot-creator and the streamer MUST share one ``--dir``. The
controller already created the slot (via ``pgcopydb snapshot --follow`` on its own
``--dir``); running ``follow`` on that SAME ``--dir`` finds the slot record and
reuses it instead of colliding. A separate pod with a fresh ``--dir`` cannot do
this (it would re-create the slot -> "already exists"). This is why CDC is a
controller-owned step, not a k8s step.

Consequently the controller's ``--dir`` is a PERSISTENT volume (not the fold's
node-local emptyDir): CDC state (the sentinel + prefetch stores) must survive a
controller restart. ``follow`` runs WITHOUT ``--resume`` here (see
``build_follow_command`` -- ``--resume`` would skip creating the sentinel that
``snapshot --follow`` never wrote).

WHY NOT A SHARD: logical decoding is ONE slot with ONE consumer -- an ordered stream
that cannot be fanned out. So CDC is exactly one consumer; the sharded model is an
initial-copy accelerator, and this serial tail is the price of an online cutover.

``follow`` streams CHANGES only (the copy step already did the bulk data). One more twist: its
catchup process only REPLAYS to the target once the sentinel ``apply`` flag is set,
which ``clone --follow`` flips after its base copy but our separate copy does not --
so ``start()`` enables it explicitly (``enable_apply`` -> ``stream sentinel set
apply``) once attached, else follow would prefetch forever (``replay_lsn 0/0``).

SCOPE: start CDC and confirm a single consumer is attached to the slot, then keep
streaming until signalled. The cutover itself is a separate step.

Composed into ``PgCyanController`` like ``PgCyanSnapshot``: ``start()`` launches the
``follow`` child and blocks (polling the source) until the slot shows an active
consumer, writes ``migration-cdc-log.json`` + the shared readiness file, and returns
(non-blocking; never raises). ``supervise()`` then blocks until the child exits or a
signal, clearing readiness. ``close()`` stops the child gracefully by setting
the sentinel endpos (a plain SIGTERM does not stop ``follow``'s receive worker;
see ``close`` / ``request_stop``).

LOB caveat (``notes/PostgreSQL Logical Replication Limitations ... LOBs.md``):
pgoutput does not replicate large objects; the planner aborts on LOB sources,
so a stream that reaches here is LOB-free.
"""
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from psycopg2 import Error as PgError

from pg_cyan.shutdown import graceful_terminate

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)

#: Is a consumer streaming from the slot yet?
_SLOT_ACTIVE = "SELECT active FROM pg_replication_slots WHERE slot_name = %s"
#: Does a replication origin of this name already exist on the target?
_ORIGIN_EXISTS = "SELECT 1 FROM pg_replication_origin WHERE roname = %s"


class PgCyanFollow:
    """Starts + supervises the controller's single ``pgcopydb follow`` CDC consumer."""

    LOG_OUTPUT_FILENAME = "migration-cdc-log.json"
    LOG_ERROR_FILENAME = "migration-cdc-log-errors.json"

    DEFAULT_WORK_DIR = "/pgcopydb"
    DEFAULT_PLUGIN = "pgoutput"
    DEFAULT_ORIGIN = "pgcyan"
    PGCOPYDB_BIN = "pgcopydb"
    #: How long to wait for the consumer to attach to the slot before failing.
    DEFAULT_ATTACH_TIMEOUT = 300      # 5min
    DEFAULT_POLL_INTERVAL = 3
    #: Seconds to wait for the follow child to reach the requested endpos and
    #: exit on its own after ``close()`` sets it -- the graceful stop. The child
    #: syncs the sentinel each feedback cycle (~10s) then streams the small CDC
    #: tail to endpos, so this only needs to cover a couple of those cycles.
    #: Sized (with the endpos command + signal fallback below) to fit inside the
    #: controller pod's ``terminationGracePeriodSeconds: 60``.
    DEFAULT_STOP_TIMEOUT = 35
    #: Seconds for the ``stream sentinel set endpos`` command itself (connect to
    #: source, read current LSN, write the sentinel).
    STOP_REQUEST_TIMEOUT = 10
    #: Bounded signal fallback used only if the endpos stop overruns
    #: ``DEFAULT_STOP_TIMEOUT`` (SIGTERM does not stop follow's receive worker,
    #: so this is essentially a direct SIGKILL + reap); kept short so the whole
    #: teardown still fits the 60s pod grace.
    FALLBACK_GRACE_TIMEOUT = 5
    FALLBACK_REAP_TIMEOUT = 3

    def __init__(
        self,
        source_pguri: Optional[str] = None,
        target_pguri: Optional[str] = None,
        slot_name: Optional[str] = None,
        plugin: str = DEFAULT_PLUGIN,
        origin: str = DEFAULT_ORIGIN,
        work_dir: str = DEFAULT_WORK_DIR,
        output_dir: Optional[str] = None,
        ready_file: Optional[str] = None,
        attach_timeout: int = DEFAULT_ATTACH_TIMEOUT,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        stop_timeout: int = DEFAULT_STOP_TIMEOUT,
        verbose: bool = False,
        resume: bool = False,
    ) -> None:
        """
        Args:
            source_pguri: Source libpq URI -- to POLL the slot's consumer state.
                (pgcopydb reads ``PGCOPYDB_SOURCE_PGURI`` / ``_TARGET_PGURI`` from
                the env for streaming.)
            target_pguri: Target libpq URI -- to drop a leftover replication origin
                before ``follow`` recreates it (see ``drop_stale_origin``).
            slot_name: The controller's replication slot to consume.
            plugin: Logical decoding output plugin (``pgoutput``).
            origin: Replication origin node name for apply-progress tracking.
            work_dir: pgcopydb ``--dir`` -- the SAME dir the controller's snapshot
                used, so ``follow`` reuses the slot from ``source.db``.
            output_dir: Directory for the log files (defaults to ``work_dir``).
            ready_file: Readiness-probe file the controller shares; created once the
                consumer is attached, removed when the stream stops. ``None`` -> no
                readiness file managed here (the caller owns it).
            attach_timeout: Seconds to wait for the consumer to attach.
            poll_interval: Seconds between slot-state polls.
            stop_timeout: Seconds to wait for the follow child to reach the
                requested endpos and exit on ``close()`` before the signal
                fallback (see ``close``).
            verbose: Run ``pgcopydb follow --verbose`` (logs each applied change) --
                an optional CDC debug knob; off by default.
            resume: Continue a previously-stopped stream (#112) instead of a
                first start after the copy -- see ``build_follow_command`` and
                ``start()`` for what this changes.
        """
        self.source_pguri = source_pguri
        self.target_pguri = target_pguri
        self.slot_name = slot_name
        self.plugin = plugin
        self.origin = origin
        self.work_dir = work_dir
        self.output_dir = output_dir if output_dir is not None else work_dir
        self.ready_file = ready_file
        self.attach_timeout = attach_timeout
        self.poll_interval = poll_interval
        self.stop_timeout = stop_timeout
        self.resume = resume
        self.verbose = verbose

        self.proc: Optional[subprocess.Popen] = None
        self.errors: List[str] = []

    # ------------------------------------------------------------- logging

    def _log_event(self, event_type: str, status: str, message: str,
                   details: Optional[Dict] = None) -> None:
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "follow-cdc",
            "event_type": event_type,
            "status": status,
            "message": message,
        }
        if details:
            event["details"] = details
        logger.info(json.dumps(event, default=str))

    # ------------------------------------------------------- command build

    def build_follow_command(self) -> List[str]:
        """``pgcopydb follow`` on the controller's --dir: reuse the slot.

        NOT ``--resume`` by default. Two source facts drive this (verified in
        pgcopydb):
          * slot REUSE fires on the slot record already being in the --dir
            ``source.db`` catalog (which the controller's ``snapshot --follow``
            wrote) -- it is NOT gated on ``--resume`` (``snapshot.c`` reuse branch).
          * the ``pgcopydb.sentinel`` row (follow's resume coordinates) is created
            ONLY when NOT ``--resume`` -- ``--resume`` *skips* it, expecting it to
            already exist (``stream_create_sentinel``). ``snapshot --follow`` does
            not create it, so ``follow --resume`` then reads 0 rows and aborts
            (rc=12). Running without ``--resume`` reuses the slot AND creates the
            sentinel (startpos = the slot's LSN = the copy's snapshot point), which
            is exactly a first CDC start after the copy.

        WITH ``self.resume`` (#112): the sentinel already exists from that first
        start (and ``clear_endpos()`` was called ahead of this to undo the
        graceful stop's endpos, else ``follow`` would see it already reached and
        exit immediately) -- ``--resume`` continues from it instead of trying
        to recreate it.
        """
        cmd = [
            self.PGCOPYDB_BIN, "follow",
            "--dir", self.work_dir,
            "--slot-name", self.slot_name or "",
            "--origin", self.origin,
            "--plugin", self.plugin,
            "--not-consistent",   # reuse the slot's snapshot; don't export a new one
        ]
        if self.resume:
            cmd.append("--resume")
        if self.verbose:
            cmd.append("--verbose")   # log each applied change (CDC volume is low)
        return cmd

    def clear_endpos(self) -> bool:
        """
        Clear the sentinel endpos ahead of a resume (#112).

        A graceful ``close()`` leaves the sentinel endpos set to the LSN it
        stopped at; on the next ``follow``, that persisted endpos would already
        be <= replay_lsn, and pgcopydb exits immediately ("previously reached")
        instead of streaming the tail. ``pgcopydb stream sentinel set endpos
        0/0`` clears it, direct-mode against this SAME ``--dir`` ``source.db``
        (like ``enable_apply``/``request_stop``), before ``start()`` launches
        ``follow --resume``.

        Retries a few times (same brief-SQLite-contention shape as
        ``enable_apply``), then gives up -- a resume cannot proceed without this,
        so a persistent failure is a resume-start failure.
        """
        cmd = [self.PGCOPYDB_BIN, "stream", "sentinel", "set", "endpos",
               "0/0", "--dir", self.work_dir]
        for attempt in range(3):
            try:
                result = subprocess.run(  # noqa: S603 -- fixed argv, no shell
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=60)
            except (OSError, subprocess.TimeoutExpired) as e:
                self._log_event("clear_endpos", "warning",
                                f"stream sentinel set endpos 0/0 failed to run (attempt {attempt + 1}/3)",
                                {"error": str(e)})
                time.sleep(2)
                continue
            if result.returncode == 0:
                self._log_event("clear_endpos", "success",
                                "Cleared the sentinel endpos ahead of resume", {})
                return True
            self._log_event("clear_endpos", "warning",
                            f"stream sentinel set endpos 0/0 rc={result.returncode} (attempt {attempt + 1}/3)",
                            {"output_tail": (result.stdout or "")[-500:]})
            time.sleep(2)
        self.errors.append("Could not clear the sentinel endpos; resume would see it already reached")
        return False

    # ------------------------------------------------ stale target origin

    def drop_stale_origin(self) -> bool:
        """
        Drop a leftover replication origin on the TARGET before ``follow`` runs.

        ``follow`` (no ``--resume``) CREATES the origin and accepts a pre-existing
        one ONLY if its LSN equals this run's startpos (``stream_create_origin``:
        ``acceptTrackedLSN = resume || lsn == startpos``). A prior/failed/cancelled
        run can leave the origin behind at a STALE LSN (target-side state -- not
        touched by the source slot/publication drops), so ``follow`` aborts
        ``origin "<name>" already exists`` (rc=12). Its stale LSN is from a
        different snapshot and must NOT be resumed from, so drop it and let
        ``follow`` recreate a fresh one at this run's slot LSN -- the same
        stale-state cleanup we do for the slot/publication on the source.

        Best-effort: a genuinely-absent origin (clean run) is the happy path, and a
        drop error is logged and tolerated (``start()`` proceeds; ``follow`` then
        surfaces any real problem). No-op without a target URI.
        """
        if not self.target_pguri:
            return False
        try:
            with psycopg2.connect(self.target_pguri) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(_ORIGIN_EXISTS, (self.origin,))
                    if cur.fetchone() is None:
                        return True        # nothing to drop (clean run)
                    cur.execute("SELECT pg_replication_origin_drop(%s)", (self.origin,))
            self._log_event("drop_stale_origin", "success",
                            f"Dropped leftover replication origin {self.origin} on the target",
                            {"origin": self.origin})
            return True
        except PgError as e:
            self._log_event("drop_stale_origin", "warning",
                            f"Could not drop stale origin {self.origin}; proceeding",
                            {"origin": self.origin, "error": str(e)})
            return False

    # --------------------------------------------------------- enable apply

    def enable_apply(self) -> bool:
        """
        Enable the sentinel ``apply`` flag so ``follow``'s catchup process REPLAYS
        changes to the target -- not just prefetch them.

        pgcopydb's catchup process waits until the sentinel ``apply`` column is set
        (``ld_apply.c``: "Waiting until the pgcopydb sentinel apply is enabled");
        it defaults off. ``clone --follow`` flips it on right after its base copy
        (``cli_clone_follow.c``: ``sentinel_update_apply(true)``). OUR copy is a
        separate step, so nothing enabled it -- ``follow`` would prefetch forever
        with ``replay_lsn 0/0`` and never touch the target. ``pgcopydb stream
        sentinel set apply`` sets it, direct-mode against the SAME ``--dir``
        ``source.db`` the running follow uses (SQLite WAL handles the concurrency).

        Retries a few times (brief SQLite contention with the running follow), then
        gives up. Required for CDC to actually progress, so a persistent failure is
        a CDC-start failure.
        """
        cmd = [self.PGCOPYDB_BIN, "stream", "sentinel", "set", "apply", "--dir", self.work_dir]
        for attempt in range(3):
            try:
                result = subprocess.run(  # noqa: S603 -- fixed argv, no shell
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=60)
            except (OSError, subprocess.TimeoutExpired) as e:
                self._log_event("enable_apply", "warning",
                                f"stream sentinel set apply failed to run (attempt {attempt + 1}/3)",
                                {"error": str(e)})
                time.sleep(2)
                continue
            if result.returncode == 0:
                self._log_event("enable_apply", "success",
                                "Enabled sentinel apply -- follow will replay changes to the target", {})
                return True
            self._log_event("enable_apply", "warning",
                            f"stream sentinel set apply rc={result.returncode} (attempt {attempt + 1}/3)",
                            {"output_tail": (result.stdout or "")[-500:]})
            time.sleep(2)
        self.errors.append("Could not enable the sentinel apply flag; CDC would prefetch but never apply")
        return False

    # -------------------------------------------------------- slot polling

    def _consumer_attached(self) -> Optional[bool]:
        """Is a consumer streaming from the slot? ``None`` if the check errored."""
        try:
            with psycopg2.connect(self.source_pguri) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(_SLOT_ACTIVE, (self.slot_name,))
                    row = cur.fetchone()
            if row is None:
                return False        # slot not found yet
            return bool(row[0])
        except PgError as e:
            self._log_event("poll_slot", "warning", "Could not read slot state",
                            {"error": str(e)})
            return None

    # ------------------------------------------------------------- output

    def _write_json(self, filename: str, payload: Dict) -> Optional[str]:
        path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, indent=2, default=str) + "\n")
            return path
        except OSError as e:
            self._log_event("write_output", "failure", f"Failed to write {filename}",
                            {"path": path, "error": str(e)})
            self.errors.append(f"Failed to write {filename}: {str(e)}")
            return None

    def _build_error(self, message: str) -> Dict:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "follow-cdc",
            "status": "failure",
            "slot": self.slot_name,
            "message": message,
            "errors": self.errors or ["Unknown error"],
            "remediation": "Confirm the controller's slot exists + retains WAL, that the snapshot was released (freeing the slot), and that follow's --dir matches the controller's.",
        }

    def set_ready(self, ready: bool) -> None:
        if not self.ready_file:
            return
        try:
            if ready:
                os.makedirs(os.path.dirname(self.ready_file) or ".", exist_ok=True)
                with open(self.ready_file, "w", encoding="utf-8") as fh:
                    fh.write("ready\n")
            elif os.path.exists(self.ready_file):
                os.remove(self.ready_file)
        except OSError as e:
            self._log_event("ready", "warning", "Could not update the readiness file",
                            {"path": self.ready_file, "error": str(e)})

    def request_stop(self) -> bool:
        """Ask the running ``follow`` to stop at the source's current LSN.

        This is pgcopydb's documented graceful stop for a live ``follow``:
        ``stream sentinel set endpos --current`` records
        ``pg_current_wal_flush_lsn()`` as the sentinel endpos (direct-mode
        against the SAME ``--dir`` ``source.db``, like ``enable_apply``). The
        receive worker re-reads the sentinel every feedback cycle
        (``ld_stream.c`` ``stream_sync_sentinel``), streams the small CDC tail
        up to that LSN, then exits; the apply worker catches up and exits; and
        the top ``follow`` process reaps both and exits 0 -- no SIGKILL.

        A SIGTERM alone does NOT stop ``follow``: only its apply/top processes
        act on it, while the receive worker's loop just reconnects
        (``ld_stream.c`` retries unless endpos is set) and gets force-killed at
        the grace deadline. So endpos, not a signal, is the graceful path.

        Best-effort: returns ``True`` if the endpos was set, ``False`` on any
        error (the caller then falls back to signalling). ``--current`` needs a
        source connection, taken from ``--source`` / ``PGCOPYDB_SOURCE_PGURI``.
        """
        if self.proc is None or self.proc.poll() is not None:
            return False
        cmd = [self.PGCOPYDB_BIN, "stream", "sentinel", "set", "endpos",
               "--current", "--dir", self.work_dir]
        if self.source_pguri:
            cmd += ["--source", self.source_pguri]
        try:
            result = subprocess.run(  # noqa: S603 -- fixed argv, no shell
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=self.STOP_REQUEST_TIMEOUT)
        except (OSError, subprocess.TimeoutExpired) as e:
            self._log_event("request_stop", "warning",
                            "Could not set the sentinel endpos to stop follow",
                            {"error": str(e)})
            return False
        if result.returncode == 0:
            self._log_event("request_stop", "success",
                            "Set sentinel endpos to current LSN; follow will stop cleanly",
                            {"output_tail": (result.stdout or "").strip()[-200:]})
            return True
        self._log_event("request_stop", "warning",
                        f"stream sentinel set endpos rc={result.returncode}",
                        {"output_tail": (result.stdout or "")[-500:]})
        return False

    def close(self) -> None:
        """Stop the follow child cleanly, preferring the sentinel endpos.

        ``follow`` is the live child at controller teardown (the snapshot was
        released mid-flow before CDC started), so it must stop cleanly -- flush
        in-flight changes and release the source replication connection --
        within the pod grace window (``terminationGracePeriodSeconds: 60``).

        A plain SIGTERM does not achieve that: ``follow``'s receive worker
        reconnects through the signal and is only SIGKILLed at the deadline
        (see ``request_stop``). So first request a graceful stop via the
        sentinel endpos and wait ``stop_timeout`` for the child to exit on its
        own. Only if that overruns (or the endpos could not be set) do we fall
        back to the shared ``graceful_terminate`` (see ``pg_cyan/shutdown.py``)
        with a short window -- essentially a bounded SIGKILL + reap, kept small
        so the whole teardown still fits the 60s grace.
        """
        if self.proc is None or self.proc.poll() is not None:
            return
        if self.request_stop():
            try:
                self.proc.wait(timeout=self.stop_timeout)
                self._log_event("close", "success",
                                "follow reached endpos and exited cleanly",
                                {"returncode": self.proc.returncode})
                return
            except subprocess.TimeoutExpired:
                self._log_event("close", "warning",
                                "follow did not reach endpos in time; escalating to signal",
                                {"timeout": self.stop_timeout})
        graceful_terminate(
            self.proc,
            grace_timeout=self.FALLBACK_GRACE_TIMEOUT,
            reap_timeout=self.FALLBACK_REAP_TIMEOUT,
            log_event=self._log_event,
        )

    # --------------------------------------------------------------- start

    def start(self) -> Dict:
        """
        Launch ``pgcopydb follow`` and block until a consumer is attached to the
        slot (or fail). Writes the CDC log + readiness file on success. Never raises.
        Non-blocking past attach: the child keeps streaming; call ``supervise()``.
        """
        summary = {"status": "failure", "log_file": None, "log_error_file": None,
                   "slot": self.slot_name, "errors": self.errors}

        if not self.slot_name:
            self.errors.append("No slot name: the controller's slot is required")
            self._log_event("start", "failure", "No slot name supplied", {})
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME, self._build_error("Missing slot name"))
            return summary
        if not self.source_pguri:
            self.errors.append("No source URI: set PGCOPYDB_SOURCE_PGURI")
            self._log_event("start", "failure", "No source URI supplied", {})
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME, self._build_error("Missing source URI"))
            return summary

        if self.resume:
            # Undo the graceful stop's persisted endpos so `follow --resume`
            # streams the tail instead of seeing it "already reached" and
            # exiting immediately. A resume cannot proceed without this.
            if not self.clear_endpos():
                self._log_event("start", "failure",
                                "Could not clear the sentinel endpos; cannot resume", {})
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("Could not clear the sentinel endpos"))
                return summary
        else:
            # follow recreates the target replication origin non-idempotently;
            # drop a leftover from a prior/cancelled run so it can create a fresh
            # one. NOT done when resuming -- that origin IS the progress marker
            # this resume continues from, dropping it would lose the apply
            # position (#112).
            self.drop_stale_origin()

        try:
            os.makedirs(self.work_dir, exist_ok=True)
            cmd = self.build_follow_command()
            self._log_event("start", "success", "Starting pgcopydb follow",
                            {"command": " ".join(cmd)})
            # start_new_session isolates pgcopydb's process group (it forks
            # prefetch/catchup workers with a plain fork(), sharing whatever
            # group it starts in) so shutdown.py can SIGTERM/SIGKILL the whole
            # tree without touching our own process group -- see shutdown.py.
            self.proc = subprocess.Popen(cmd, start_new_session=True)  # noqa: S603 -- fixed argv, no shell
        except (OSError, ValueError) as e:
            self.errors.append(f"Failed to launch pgcopydb follow: {str(e)}")
            self._log_event("start", "failure", "Failed to launch pgcopydb follow",
                            {"error": str(e)})
            summary["log_error_file"] = self._write_json(
                self.LOG_ERROR_FILENAME, self._build_error("Failed to launch pgcopydb follow"))
            return summary

        # Block until a consumer is attached to the slot (this stream) or fail.
        deadline = time.monotonic() + self.attach_timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:      # child died before attaching
                rc = self.proc.returncode
                self.errors.append(f"pgcopydb follow exited early (rc={rc}) before attaching")
                self._log_event("start", "failure", f"pgcopydb follow exited early (rc={rc})",
                                {"returncode": rc})
                summary["log_error_file"] = self._write_json(
                    self.LOG_ERROR_FILENAME, self._build_error("pgcopydb follow exited before attaching"))
                return summary
            if self._consumer_attached():
                # Attached (prefetching); now ENABLE APPLY so the catchup process
                # replays to the target -- else follow prefetches forever (replay_lsn
                # 0/0) and nothing lands. The sentinel exists by now (follow created
                # it during setup, before the slot went active).
                if not self.enable_apply():
                    self._log_event("start", "failure",
                                    "CDC attached but could not enable apply -- it would not progress",
                                    {"slot": self.slot_name})
                    self.close()
                    summary["log_error_file"] = self._write_json(
                        self.LOG_ERROR_FILENAME, self._build_error("Could not enable the sentinel apply flag"))
                    return summary
                summary["log_file"] = self._write_json(self.LOG_OUTPUT_FILENAME, {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "component": "follow-cdc",
                    "status": "streaming",
                    "slot": self.slot_name,
                    "plugin": self.plugin,
                    "origin": self.origin,
                    "message": ("Change data capture started: a single consumer is "
                                "attached to the slot, apply is enabled, and changes "
                                "are being replayed to the target."),
                })
                self.set_ready(True)
                summary["status"] = "success"
                self._log_event("start", "success",
                                "CDC consumer attached + apply enabled",
                                {"slot": self.slot_name, "log_file": summary["log_file"]})
                return summary
            time.sleep(self.poll_interval)

        self.errors.append(f"No consumer attached to slot {self.slot_name} within {self.attach_timeout}s")
        self._log_event("start", "failure", "Timed out waiting for the consumer to attach",
                        {"slot": self.slot_name, "timeout": self.attach_timeout})
        self.close()
        summary["log_error_file"] = self._write_json(
            self.LOG_ERROR_FILENAME, self._build_error("Timed out waiting for the consumer to attach"))
        return summary

    # ------------------------------------------------------------ supervise

    def supervise(self, stop) -> int:
        """
        Block while the ``follow`` child streams, until it exits or ``stop()`` is
        truthy (the controller's SIGTERM flag). Clears readiness on exit. Returns
        the child's exit code (0 == clean/signalled). ``stop`` is a callable
        ``() -> bool``.
        """
        if self.proc is None:
            return 0
        while not stop():
            try:
                rc = self.proc.wait(timeout=5)
                # Child exited on its own (endpos reached, or an error).
                self._log_event("supervise", "success" if rc == 0 else "failure",
                                f"pgcopydb follow exited (rc={rc})", {"returncode": rc})
                self.set_ready(False)
                return rc
            except subprocess.TimeoutExpired:
                continue
        self.close()
        self.set_ready(False)
        self._log_event("supervise", "success", "CDC stopped on signal", {})
        return self.proc.returncode if self.proc.returncode is not None else 0
