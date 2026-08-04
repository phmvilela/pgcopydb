"""
Unit tests for the pre-data schema transfer (`pg_cyan.cli restore-predata`).

See also: pg_cyan/predata.py

DB-free: the pgcopydb child is stubbed via a fake ``subprocess.run``, so nothing
spawns a real process or touches a database. Tests exercise the command build
(dump schema + restore pre-data), the snapshot/target guards, the run() happy
path and its failure branches, idempotency knobs (work-dir reset,
--drop-if-exists), and the output-file shape.
"""
import json

import pytest

from pg_cyan import predata as predata_module
from pg_cyan.predata import PgCyanPreData


# ------------------------------------------------------------------- fakes

class _FakeCompleted:
    def __init__(self, returncode=0, stdout=""):
        self.returncode = returncode
        self.stdout = stdout


def _fake_run_factory(recorder, returncodes=None):
    """Return a fake subprocess.run that records argv and returns canned codes.

    ``returncodes`` maps a substring of the command (e.g. "dump", "restore")
    to the return code; missing -> 0 (success).
    """
    returncodes = returncodes or {}

    def _fake_run(cmd, **kwargs):
        recorder.append(cmd)
        joined = " ".join(cmd)
        rc = 0
        for needle, code in returncodes.items():
            if needle in joined:
                rc = code
                break
        return _FakeCompleted(returncode=rc, stdout="pgcopydb: fake output")

    return _fake_run


@pytest.fixture
def worker(tmp_path):
    return PgCyanPreData(
        source_pguri="postgres://src/db",
        target_pguri="postgres://tgt/db",
        snapshot_id="0000000A-1",
        work_dir=str(tmp_path / "work"),
        output_dir=str(tmp_path / "out"),
    )


# ------------------------------------------------------------ command build

def test_dump_command_pins_the_snapshot(worker):
    cmd = worker.build_dump_command()
    assert cmd[:3] == ["pgcopydb", "dump", "schema"]
    assert "--snapshot" in cmd and "0000000A-1" in cmd
    assert "--source" in cmd
    joined = " ".join(cmd)
    assert "--dir" in joined


def test_dump_command_omits_snapshot_when_not_consistent(tmp_path):
    w = PgCyanPreData(target_pguri="postgres://tgt/db", not_consistent=True,
                      work_dir=str(tmp_path))
    assert "--snapshot" not in w.build_dump_command()


def test_restore_command_is_pre_data_only_with_no_owner(worker):
    cmd = worker.build_restore_command()
    assert cmd[:3] == ["pgcopydb", "restore", "pre-data"]
    assert "--no-owner" in cmd            # target roles pre-created
    assert "--drop-if-exists" in cmd      # idempotent retries
    assert "--target" in cmd and "postgres://tgt/db" in cmd
    # the restore must carry the SAME snapshot as the dump, or pgcopydb rejects
    # the shared --dir catalog ("current snapshot is ''")
    assert "--snapshot" in cmd and "0000000A-1" in cmd
    # pre-data ONLY: never restore the post-data section here
    assert "post-data" not in " ".join(cmd)


def test_restore_command_uses_not_consistent_instead_of_snapshot(tmp_path):
    w = PgCyanPreData(target_pguri="postgres://tgt/db", not_consistent=True,
                      work_dir=str(tmp_path))
    cmd = w.build_restore_command()
    assert "--not-consistent" in cmd
    assert "--snapshot" not in cmd


def test_dump_and_restore_share_the_same_snapshot(worker):
    dump = worker.build_dump_command()
    restore = worker.build_restore_command()
    # same id stamped into the shared catalog on dump must be presented on restore
    assert dump[dump.index("--snapshot") + 1] == restore[restore.index("--snapshot") + 1]


def test_restore_command_owner_and_no_drop(tmp_path):
    w = PgCyanPreData(target_pguri="postgres://tgt/db", snapshot_id="s",
                      no_owner=False, drop_if_exists=False, work_dir=str(tmp_path))
    cmd = w.build_restore_command()
    assert "--no-owner" not in cmd
    assert "--drop-if-exists" not in cmd


# ------------------------------------------------------------------ run()

def test_run_happy_path_writes_success_log(worker, monkeypatch, tmp_path):
    recorder = []
    monkeypatch.setattr(predata_module.subprocess, "run", _fake_run_factory(recorder))
    summary = worker.run()

    assert summary["status"] == "success"
    # dump ran before restore
    assert " ".join(recorder[0]).startswith("pgcopydb dump schema")
    assert " ".join(recorder[1]).startswith("pgcopydb restore pre-data")

    # the success artifact exists, is non-empty, and carries a success message
    log_path = tmp_path / "out" / PgCyanPreData.LOG_OUTPUT_FILENAME
    assert log_path.exists()
    payload = json.loads(log_path.read_text())
    assert payload["status"] == "success"
    assert payload["section"] == "pre-data"
    assert payload["snapshot"] == "0000000A-1"
    assert "message" in payload and payload["message"]
    # no error file on the happy path
    assert not (tmp_path / "out" / PgCyanPreData.LOG_ERROR_FILENAME).exists()


def test_run_requires_snapshot_when_consistent(tmp_path, monkeypatch):
    w = PgCyanPreData(target_pguri="postgres://tgt/db", snapshot_id=None,
                      work_dir=str(tmp_path / "w"), output_dir=str(tmp_path / "o"))
    ran = []
    monkeypatch.setattr(predata_module.subprocess, "run", _fake_run_factory(ran))
    summary = w.run()
    assert summary["status"] == "failure"
    assert ran == []  # never launched pgcopydb
    assert (tmp_path / "o" / PgCyanPreData.LOG_ERROR_FILENAME).exists()


def test_run_requires_target(tmp_path, monkeypatch):
    w = PgCyanPreData(target_pguri=None, snapshot_id="s",
                      work_dir=str(tmp_path / "w"), output_dir=str(tmp_path / "o"))
    ran = []
    monkeypatch.setattr(predata_module.subprocess, "run", _fake_run_factory(ran))
    summary = w.run()
    assert summary["status"] == "failure"
    assert ran == []


def test_run_writes_error_log_when_dump_fails(worker, monkeypatch, tmp_path):
    recorder = []
    monkeypatch.setattr(predata_module.subprocess, "run",
                        _fake_run_factory(recorder, {"dump": 1}))
    summary = worker.run()
    assert summary["status"] == "failure"
    # restore must NOT run if the dump failed
    assert len(recorder) == 1
    err_path = tmp_path / "out" / PgCyanPreData.LOG_ERROR_FILENAME
    assert err_path.exists()
    payload = json.loads(err_path.read_text())
    assert payload["status"] == "failure"
    assert payload["errors"]
    assert not (tmp_path / "out" / PgCyanPreData.LOG_OUTPUT_FILENAME).exists()


def test_run_writes_error_log_when_restore_fails(worker, monkeypatch, tmp_path):
    recorder = []
    monkeypatch.setattr(predata_module.subprocess, "run",
                        _fake_run_factory(recorder, {"restore": 1}))
    summary = worker.run()
    assert summary["status"] == "failure"
    assert len(recorder) == 2  # dump ok, restore failed
    assert (tmp_path / "out" / PgCyanPreData.LOG_ERROR_FILENAME).exists()


def test_reset_work_dir_clears_prior_run(tmp_path, monkeypatch):
    work = tmp_path / "work"
    work.mkdir()
    (work / "schema").mkdir()
    (work / "schema" / "schema.dump").write_text("stale")
    (work / "loose.txt").write_text("stale")

    w = PgCyanPreData(target_pguri="postgres://tgt/db", snapshot_id="s",
                      work_dir=str(work), output_dir=str(tmp_path / "o"))
    recorder = []
    monkeypatch.setattr(predata_module.subprocess, "run", _fake_run_factory(recorder))
    w.run()
    # the stale dump was wiped before the fresh run
    assert not (work / "schema" / "schema.dump").exists()
    assert not (work / "loose.txt").exists()
