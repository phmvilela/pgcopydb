"""
Unit tests for the sequence sync (`pg_cyan.cli copy-sequences`).

See also: pg_cyan/sequences.py

DB-free: psycopg2 connections are faked. Tests cover source introspection, the
setval-per-sequence apply (incl. skipping NULL last_value), the introspection-error
path, and the output shape.
"""
import json

import pytest

from pg_cyan import sequences as seq_module
from pg_cyan.sequences import PgCyanSequences

# 3 source sequences; one never advanced (last_value NULL -> skipped).
_SEQS = [
    ("public", "customers_id_seq", 100000),
    ("public", "orders_id_seq", 1000000),
    ("public", "empty_id_seq", None),
]


class _SourceCursor:
    def execute(self, query, params=None):
        pass

    def fetchall(self):
        return list(_SEQS)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _TargetCursor:
    def __init__(self, fail=None):
        self._fail = fail            # sequence name whose setval raises
        self.executed = []

    def execute(self, query, params=None):
        text = str(query)
        self.executed.append(text)
        if self._fail and self._fail in text:
            raise seq_module.PgError("cannot setval")

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self, cursor):
        self.autocommit = False
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def _connect_factory(target_cursor, raise_on=None):
    def _connect(dsn):
        if raise_on and raise_on in dsn:
            raise seq_module.PgError("could not connect")
        return _Conn(_SourceCursor()) if "src" in dsn else _Conn(target_cursor)
    return _connect


def _make(tmp_path, **kwargs):
    defaults = dict(source_pguri="postgres://src/db", target_pguri="postgres://tgt/db",
                    output_dir=str(tmp_path))
    defaults.update(kwargs)
    return PgCyanSequences(**defaults)


def test_run_happy_path_setvals_each_nonnull_sequence(tmp_path, monkeypatch):
    tgt = _TargetCursor()
    monkeypatch.setattr(seq_module.psycopg2, "connect", _connect_factory(tgt))
    summary = _make(tmp_path).run()

    assert summary["status"] == "success"
    assert summary["sequences_total"] == 3
    assert summary["sequences_synced"] == 3          # NULL one counts as synced (skipped)
    setvals = [e for e in tgt.executed if "setval" in e]
    assert len(setvals) == 2                          # only the two non-NULL sequences
    log = json.loads((tmp_path / PgCyanSequences.LOG_OUTPUT_FILENAME).read_text())
    assert log["status"] == "completed" and log["sequences_synced"] == 3


def test_null_last_value_is_skipped_not_failed(tmp_path, monkeypatch):
    tgt = _TargetCursor()
    monkeypatch.setattr(seq_module.psycopg2, "connect", _connect_factory(tgt))
    w = _make(tmp_path)
    w.run()
    # the empty sequence (NULL) must NOT produce a setval
    assert not any("empty_id_seq" in e for e in tgt.executed)


def test_setval_failure_is_a_step_failure(tmp_path, monkeypatch):
    tgt = _TargetCursor(fail="orders_id_seq")
    monkeypatch.setattr(seq_module.psycopg2, "connect", _connect_factory(tgt))
    summary = _make(tmp_path).run()
    assert summary["status"] == "failure"
    assert summary["sequences_synced"] == 2          # customers + empty(skipped); orders failed
    assert (tmp_path / PgCyanSequences.LOG_ERROR_FILENAME).exists()
    assert not (tmp_path / PgCyanSequences.LOG_OUTPUT_FILENAME).exists()


def test_connection_failure_writes_error(tmp_path, monkeypatch):
    monkeypatch.setattr(seq_module.psycopg2, "connect",
                        _connect_factory(_TargetCursor(), raise_on="tgt"))
    summary = _make(tmp_path).run()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanSequences.LOG_ERROR_FILENAME).exists()
