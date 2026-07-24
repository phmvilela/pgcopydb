"""
Unit tests for the snapshot holder (`pg_cyan.cli snapshot-hold`, issue #23).

DB-free: psycopg2 and the pgcopydb child are stubbed, so nothing connects or
spawns a real process. Tests exercise plan overrides, the pgcopydb command
build, idempotent publication creation, the establish() happy path and its
failure branches (connection / publication / snapshot), the output-file shape,
and the graceful-shutdown / no-stale-child behaviour.
"""
import json

import psycopg2
import pytest

from pg_cyan import snapshot as snapshot_module
from pg_cyan.snapshot import PgCyanSnapshot


# ------------------------------------------------------------------- fakes

class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        # psycopg2.sql.Composed (CREATE/DROP PUBLICATION) isn't a str; repr keeps
        # the literal SQL text ("CREATE PUBLICATION ...", "DROP PUBLICATION ...")
        # so tests can substring-match what was issued.
        text = query if isinstance(query, str) else repr(query)
        if self.conn.raise_on_drop_pub and "DROP PUBLICATION" in text:
            raise psycopg2.Error("boom dropping publication")
        self.conn.executed.append(text)
        if "pg_publication" in text:
            self._result = (1,) if self.conn.pub_exists else None
        elif "restart_lsn" in text:
            self._result = (self.conn.restart_lsn,) if self.conn.restart_lsn else None
        elif "pg_drop_replication_slot" in text:
            self.conn.dropped_slots.append(params[0] if params else None)
            self._result = None
        elif "pg_replication_slots" in text:
            self._result = (
                (self.conn.slot_active, self.conn.active_pid)
                if self.conn.slot_exists else None
            )
        else:
            self._result = None

    def fetchone(self):
        return self._result


class _FakeConn:
    def __init__(self, pub_exists=False, restart_lsn="0/16B3760",
                 slot_exists=False, slot_active=False, active_pid=4321,
                 raise_on_drop_pub=False):
        self.autocommit = False
        self.closed = False
        self.executed = []
        self.pub_exists = pub_exists
        # drop_stale_publication() error injection.
        self.raise_on_drop_pub = raise_on_drop_pub
        self.restart_lsn = restart_lsn
        # Stale-slot state used by drop_stale_slot().
        self.slot_exists = slot_exists
        self.slot_active = slot_active
        self.active_pid = active_pid
        self.dropped_slots = []

    def cursor(self):
        return _FakeCursor(self)

    def close(self):
        self.closed = True


class _FakeProc:
    """A stand-in pgcopydb child. ``alive`` controls the early-exit path."""

    def __init__(self, alive=True, returncode=None):
        self.returncode = returncode if not alive else None
        self.terminated = False
        self.killed = False

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = 0

    def kill(self):
        self.killed = True
        self.returncode = -9


def _install_fakes(monkeypatch, tmp_path, *, conn=None, writes_snapshot=True,
                   proc=None, snapshot_id="0000000A-1"):
    """Wire fake psycopg2.connect + subprocess.Popen into the module."""
    fake_conn = conn if conn is not None else _FakeConn()
    monkeypatch.setattr(snapshot_module.psycopg2, "connect",
                        lambda *a, **k: fake_conn)

    the_proc = proc if proc is not None else _FakeProc(alive=True)

    def _fake_popen(cmd, *a, **k):
        if writes_snapshot:
            (tmp_path / PgCyanSnapshot.SNAPSHOT_FILENAME).write_text(snapshot_id + "\n")
        return the_proc

    monkeypatch.setattr(snapshot_module.subprocess, "Popen", _fake_popen)
    return fake_conn, the_proc


def _make_holder(tmp_path, **kwargs):
    kwargs.setdefault("source_pguri", "postgres://u@h:5432/db")
    kwargs.setdefault("work_dir", str(tmp_path))
    kwargs.setdefault("snapshot_timeout", 5)
    return PgCyanSnapshot(**kwargs)


# --------------------------------------------------------------- plan overrides

def test_load_plan_overrides_applies_names_and_mode(tmp_path):
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({
        "publication": {"name": "plan_pub"},
        "slot": {"name": "plan_slot"},
        "snapshot": {"mode": "frozen-source"},
    }))
    holder = _make_holder(tmp_path, plan_file=str(plan))
    holder.load_plan_overrides()
    assert holder.publication_name == "plan_pub"
    assert holder.slot_name == "plan_slot"
    assert holder.snapshot_mode == "frozen-source"
    assert holder.not_consistent is True


def test_load_plan_overrides_missing_file_keeps_defaults(tmp_path):
    holder = _make_holder(tmp_path, plan_file=str(tmp_path / "nope.json"))
    holder.load_plan_overrides()
    assert holder.publication_name == PgCyanSnapshot.DEFAULT_PUBLICATION
    assert holder.slot_name == PgCyanSnapshot.DEFAULT_SLOT
    assert holder.not_consistent is False


# ------------------------------------------------------------ command building

def test_build_pgcopydb_command_has_follow_plugin_slot_dir(tmp_path):
    holder = _make_holder(tmp_path, slot_name="s", plugin="pgoutput", publication_name="p")
    cmd = holder.build_pgcopydb_command()
    assert cmd[:2] == ["pgcopydb", "snapshot"]
    assert "--follow" in cmd
    assert cmd[cmd.index("--plugin") + 1] == "pgoutput"
    assert cmd[cmd.index("--slot-name") + 1] == "s"
    # `pgcopydb snapshot` has NO --publication option (only clone/follow do);
    # passing it makes pgcopydb print usage and exit. drop_stale_publication
    # handles the slot-named publication pgcopydb auto-creates instead.
    assert "--publication" not in cmd
    assert cmd[cmd.index("--dir") + 1] == str(tmp_path)
    assert cmd[cmd.index("--source") + 1] == "postgres://u@h:5432/db"


# --------------------------------------------------------------- publication

def test_create_publication_creates_when_absent(tmp_path):
    conn = _FakeConn(pub_exists=False)
    holder = _make_holder(tmp_path, publication_name="my_pub")
    holder.conn = conn
    assert holder.create_publication() is True
    assert any("CREATE PUBLICATION" in e for e in conn.executed)


def test_create_publication_idempotent_when_present(tmp_path):
    conn = _FakeConn(pub_exists=True)
    holder = _make_holder(tmp_path, publication_name="my_pub")
    holder.conn = conn
    assert holder.create_publication() is True
    # It already exists -> no CREATE issued.
    assert not any("CREATE PUBLICATION" in e for e in conn.executed)


# -------------------------------------------------------------- work-dir reset

def test_reset_work_dir_clears_prior_pgcopydb_state(tmp_path):
    # Simulate a leftover pgcopydb catalog + a stale slot file on the PVC.
    (tmp_path / "schema").mkdir()
    (tmp_path / "schema" / "source.db").write_text("stale-catalog")
    (tmp_path / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).write_text("{}")
    (tmp_path / PgCyanSnapshot.SNAPSHOT_FILENAME).write_text("STALE\n")

    holder = _make_holder(tmp_path)
    holder.reset_work_dir()

    assert tmp_path.is_dir()
    assert not (tmp_path / "schema").exists()
    assert not (tmp_path / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).exists()
    assert not (tmp_path / PgCyanSnapshot.SNAPSHOT_FILENAME).exists()


def test_reset_work_dir_clears_stale_output_when_output_dir_differs(tmp_path):
    work = tmp_path / "work"
    out = tmp_path / "out"
    work.mkdir()
    out.mkdir()
    # A prior run's slot file in a separate output dir would trip the probe.
    (out / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).write_text("{}")

    holder = _make_holder(work, work_dir=str(work), output_dir=str(out))
    holder.reset_work_dir()

    assert not (out / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).exists()


# ------------------------------------------------------- snapshot id handoff

class _FakeCompleted:
    def __init__(self, stdout=""):
        self.stdout = stdout


def _record_kubectl(monkeypatch):
    """Capture kubectl invocations; return the recorded-calls list."""
    calls = []

    def _fake_run(cmd, *a, **k):
        calls.append({"cmd": cmd, "input": k.get("input")})
        return _FakeCompleted(stdout="apiVersion: v1\nkind: ConfigMap\n")

    monkeypatch.setattr(snapshot_module.subprocess, "run", _fake_run)
    return calls


def test_publish_snapshot_configmap_skips_when_unset(monkeypatch, tmp_path):
    # No ConfigMap configured (local run) -> no-op success, no kubectl call.
    calls = _record_kubectl(monkeypatch)
    holder = _make_holder(tmp_path)  # snapshot_configmap defaults to None
    assert holder.publish_snapshot_configmap("SNAP-1") is True
    assert calls == []


def test_publish_snapshot_configmap_invokes_kubectl(monkeypatch, tmp_path):
    calls = _record_kubectl(monkeypatch)
    holder = _make_holder(tmp_path, snapshot_configmap="pgcyan-snapshot",
                          configmap_namespace="pgcyan-migration")
    assert holder.publish_snapshot_configmap("SNAP-1") is True
    # create --dry-run ... | apply -f -
    assert [c["cmd"][:3] for c in calls] == [
        ["kubectl", "create", "configmap"], ["kubectl", "apply", "-f"],
    ]
    assert "--from-literal=snapshot=SNAP-1" in calls[0]["cmd"]
    assert calls[0]["cmd"][-2:] == ["--namespace", "pgcyan-migration"]
    # the rendered yaml from the create step is piped into apply's stdin
    assert calls[1]["input"] == "apiVersion: v1\nkind: ConfigMap\n"


def test_publish_snapshot_configmap_failure_returns_false(monkeypatch, tmp_path):
    import subprocess as real_subprocess

    def _boom(cmd, *a, **k):
        raise real_subprocess.CalledProcessError(1, cmd, stderr="forbidden")

    monkeypatch.setattr(snapshot_module.subprocess, "run", _boom)
    holder = _make_holder(tmp_path, snapshot_configmap="pgcyan-snapshot")
    assert holder.publish_snapshot_configmap("SNAP-1") is False
    assert any("ConfigMap" in e for e in holder.errors)


def test_establish_publishes_snapshot_to_configmap(monkeypatch, tmp_path):
    # End-to-end: pgcopydb writes the id into the node-local work dir; establish()
    # publishes it into the ConfigMap and records the ConfigMap in the slot file.
    calls = _record_kubectl(monkeypatch)
    _install_fakes(monkeypatch, tmp_path, snapshot_id="SNAP-9")
    holder = _make_holder(tmp_path, snapshot_configmap="pgcyan-snapshot")
    summary = holder.establish()
    assert summary["status"] == "success"
    assert any("--from-literal=snapshot=SNAP-9" in c["cmd"] for c in calls)
    slot = json.loads((tmp_path / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).read_text())
    assert slot["snapshot"]["configmap"] == "pgcyan-snapshot"
    assert slot["snapshot"]["configmap_key"] == "snapshot"


# ---------------------------------------------------------------- stale slot

def test_drop_stale_slot_noop_when_absent(tmp_path):
    conn = _FakeConn(slot_exists=False)
    holder = _make_holder(tmp_path, slot_name="pgcyan_slot")
    holder.conn = conn
    assert holder.drop_stale_slot() is True
    assert conn.dropped_slots == []  # nothing to drop


def test_drop_stale_slot_drops_inactive_leftover(tmp_path):
    conn = _FakeConn(slot_exists=True, slot_active=False)
    holder = _make_holder(tmp_path, slot_name="pgcyan_slot")
    holder.conn = conn
    assert holder.drop_stale_slot() is True
    assert conn.dropped_slots == ["pgcyan_slot"]


def test_drop_stale_slot_refuses_active_slot(tmp_path):
    conn = _FakeConn(slot_exists=True, slot_active=True, active_pid=99)
    holder = _make_holder(tmp_path, slot_name="pgcyan_slot")
    holder.conn = conn
    assert holder.drop_stale_slot() is False
    assert conn.dropped_slots == []  # an active slot must NOT be dropped
    assert any("active" in e for e in holder.errors)


# ------------------------------------------------------- stale pgcopydb pub

def test_drop_stale_publication_drops_slot_named_pub(tmp_path):
    # pgcopydb auto-creates a publication named after the SLOT; we clear it (via
    # DROP ... IF EXISTS) so pgcopydb's non-idempotent CREATE succeeds. It must
    # target the slot name, NOT the holder's own publication_name.
    conn = _FakeConn()
    holder = _make_holder(tmp_path, slot_name="pgcyan_slot", publication_name="pgcyan_pub")
    holder.conn = conn
    assert holder.drop_stale_publication() is True
    dropped = [e for e in conn.executed if "DROP PUBLICATION" in e]
    assert len(dropped) == 1
    assert "pgcyan_slot" in dropped[0]      # the slot-named pub...
    assert "pgcyan_pub" not in dropped[0]   # ...not the holder's own


def test_drop_stale_publication_reports_db_error(tmp_path):
    conn = _FakeConn(raise_on_drop_pub=True)
    holder = _make_holder(tmp_path, slot_name="pgcyan_slot")
    holder.conn = conn
    assert holder.drop_stale_publication() is False
    assert any("publication" in e for e in holder.errors)


# ------------------------------------------------------------------ establish

def test_establish_happy_path(monkeypatch, tmp_path):
    conn, proc = _install_fakes(monkeypatch, tmp_path)
    holder = _make_holder(tmp_path, slot_name="pgcyan_slot", publication_name="pgcyan_pub")
    summary = holder.establish()

    assert summary["status"] == "success"
    assert summary["snapshot_name"] == "0000000A-1"

    # Both artifacts written and well-formed, on the shared work dir.
    slot = json.loads((tmp_path / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).read_text())
    pub = json.loads((tmp_path / PgCyanSnapshot.PUBLICATION_OUTPUT_FILENAME).read_text())
    assert slot["status"] == "success"
    assert slot["slot"]["name"] == "pgcyan_slot"
    assert slot["slot"]["snapshot_name"] == "0000000A-1"
    assert slot["slot"]["consistent_point"] == "0/16B3760"
    assert pub["publication"]["name"] == "pgcyan_pub"
    assert pub["publication"]["all_tables"] is True

    # Our short-lived connection is released; the child keeps holding (not killed).
    assert conn.closed is True
    assert proc.terminated is False


def test_establish_connection_failure_writes_publication_error(monkeypatch, tmp_path):
    holder = _make_holder(tmp_path)
    monkeypatch.setattr(holder, "connect", lambda: False)
    summary = holder.establish()
    assert summary["status"] == "failure"
    assert summary["publication_error_file"] is not None
    assert (tmp_path / PgCyanSnapshot.PUBLICATION_ERROR_FILENAME).exists()


def test_establish_publication_failure_writes_publication_error(monkeypatch, tmp_path):
    _install_fakes(monkeypatch, tmp_path)
    holder = _make_holder(tmp_path)
    monkeypatch.setattr(holder, "create_publication", lambda: False)
    summary = holder.establish()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanSnapshot.PUBLICATION_ERROR_FILENAME).exists()
    # No slot file on a pre-slot failure.
    assert not (tmp_path / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).exists()


def test_establish_snapshot_early_exit_writes_slot_error(monkeypatch, tmp_path):
    dead = _FakeProc(alive=False, returncode=1)
    _install_fakes(monkeypatch, tmp_path, writes_snapshot=False, proc=dead)
    holder = _make_holder(tmp_path)
    summary = holder.establish()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanSnapshot.SLOT_ERROR_FILENAME).exists()
    # The publication succeeded and was recorded before the slot step failed.
    assert (tmp_path / PgCyanSnapshot.PUBLICATION_OUTPUT_FILENAME).exists()


def test_establish_active_slot_writes_slot_error(monkeypatch, tmp_path):
    # A live slot (another holder / leaked connection) is a hard failure -- we
    # must not drop it or proceed to CREATE a fresh snapshot on top of it.
    conn = _FakeConn(slot_exists=True, slot_active=True, active_pid=77)
    _install_fakes(monkeypatch, tmp_path, conn=conn)
    holder = _make_holder(tmp_path, slot_name="pgcyan_slot")
    summary = holder.establish()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanSnapshot.SLOT_ERROR_FILENAME).exists()
    assert conn.dropped_slots == []
    # The publication step ran and was recorded before the slot check failed.
    assert (tmp_path / PgCyanSnapshot.PUBLICATION_OUTPUT_FILENAME).exists()


def test_establish_clears_stale_slot_json_before_probe(monkeypatch, tmp_path):
    # A leftover slot file from a prior run must be gone before the fresh one is
    # written, so the readiness probe can't pass on stale output.
    (tmp_path / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).write_text('{"stale": true}')
    _install_fakes(monkeypatch, tmp_path, snapshot_id="FRESH-1")
    holder = _make_holder(tmp_path)
    summary = holder.establish()
    assert summary["status"] == "success"
    slot = json.loads((tmp_path / PgCyanSnapshot.SLOT_OUTPUT_FILENAME).read_text())
    assert slot["status"] == "success"
    assert "stale" not in slot


def test_establish_removes_stale_snapshot_file(monkeypatch, tmp_path):
    # A leftover file from a prior pod must not be mistaken for a fresh export.
    stale = tmp_path / PgCyanSnapshot.SNAPSHOT_FILENAME
    stale.write_text("STALE-SNAPSHOT\n")
    _install_fakes(monkeypatch, tmp_path, snapshot_id="FRESH-1")
    holder = _make_holder(tmp_path)
    summary = holder.establish()
    assert summary["snapshot_name"] == "FRESH-1"


# ---------------------------------------------------------------- shutdown

def test_on_signal_terminates_child(monkeypatch, tmp_path):
    proc = _FakeProc(alive=True)
    holder = _make_holder(tmp_path)
    holder.proc = proc
    holder._on_signal(15, None)
    assert proc.terminated is True
    assert holder._stop_requested is True


def test_terminate_child_noop_when_already_exited(tmp_path):
    proc = _FakeProc(alive=False, returncode=0)
    holder = _make_holder(tmp_path)
    holder.proc = proc
    holder._terminate_child()  # must not raise or re-terminate
    assert proc.terminated is False


def test_hold_returns_child_exit_code_and_closes_connection(tmp_path):
    import signal
    saved = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        conn = _FakeConn()
        proc = _FakeProc(alive=True)
        holder = _make_holder(tmp_path)
        holder.conn = conn
        holder.proc = proc
        assert holder.hold() == 0
        assert conn.closed is True
    finally:
        for s, handler in saved.items():
            signal.signal(s, handler)


def test_hold_without_child_is_noop(tmp_path):
    holder = _make_holder(tmp_path)
    assert holder.hold() == 0


def test_close_releases_connection_and_child(tmp_path):
    conn = _FakeConn()
    proc = _FakeProc(alive=True)
    holder = _make_holder(tmp_path)
    holder.conn = conn
    holder.proc = proc
    holder.close()
    assert conn.closed is True
    assert proc.terminated is True
