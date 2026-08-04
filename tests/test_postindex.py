"""
Unit tests for the post-data index + PK/unique build (`pg_cyan.cli build-indexes`)
-- custom SQL.

See also: pg_cyan/postindex.py

DB-free: psycopg2 connections are faked. Tests cover shard-filter parsing, source
introspection of secondary indexes + PK/unique, idempotent apply (DuplicateTable /
DuplicateObject), the introspection-error-is-a-failure path, and the output shape.
"""
import json

import psycopg2
import pytest

from pg_cyan import postindex as pi_module
from pg_cyan.postindex import PgCyanPostIndex

# Per source table: its secondary indexes and its PK/unique constraints.
_INDEXES = {
    "public.orders": [("orders_customer_id_idx", "CREATE INDEX orders_customer_id_idx ON public.orders USING btree (customer_id)")],
    "public.customers": [],
}
_PKUQ = {
    "public.orders": [("orders_pkey", "PRIMARY KEY (id)")],
    "public.customers": [("customers_pkey", "PRIMARY KEY (id)")],
}


class _SourceCursor:
    def __init__(self):
        self._rows = []

    def execute(self, query, params=None):
        table = params[0]
        q = str(query)
        if "pg_get_indexdef" in q:
            self._rows = _INDEXES.get(table, [])
        elif "pg_get_constraintdef" in q:
            self._rows = _PKUQ.get(table, [])
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _TargetCursor:
    def __init__(self, dup_index=None, dup_constraint=None, fail=None):
        self._dup_index = dup_index
        self._dup_constraint = dup_constraint
        self._fail = fail            # substring whose CREATE/ALTER raises PgError
        self.executed = []

    def execute(self, query, params=None):
        text = str(query)
        self.executed.append(text)
        if "SET statement_timeout" in text:
            return
        if self._fail and self._fail in text:
            raise pi_module.PgError("boom")
        if "CREATE INDEX" in text and self._dup_index and self._dup_index in text:
            raise psycopg2.errors.DuplicateTable("exists")
        if "ADD CONSTRAINT" in text and self._dup_constraint and self._dup_constraint in text:
            raise psycopg2.errors.DuplicateObject("exists")

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self, cursor):
        self.autocommit = False
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def _connect_factory(target_cursor, raise_on=None):
    def _connect(dsn):
        if raise_on and raise_on in dsn:
            raise pi_module.PgError("could not connect")
        return _Conn(_SourceCursor()) if "src" in dsn else _Conn(target_cursor)
    return _connect


@pytest.fixture
def filter_file(tmp_path):
    p = tmp_path / "shard-0.filters"
    p.write_text("[include-only-table]\npublic.orders\npublic.customers\n", encoding="utf-8")
    return str(p)


def _make(tmp_path, filter_file, **kwargs):
    defaults = dict(source_pguri="postgres://src/db", target_pguri="postgres://tgt/db",
                    filter_file=filter_file, shard_name="idx-0", output_dir=str(tmp_path))
    defaults.update(kwargs)
    return PgCyanPostIndex(**defaults)


def test_parse_shard_tables(tmp_path, filter_file):
    w = _make(tmp_path, filter_file)
    assert w.parse_shard_tables() == ["public.orders", "public.customers"]


def test_run_happy_path_builds_indexes_and_constraints(tmp_path, filter_file, monkeypatch):
    tgt = _TargetCursor()
    monkeypatch.setattr(pi_module.psycopg2, "connect", _connect_factory(tgt))
    summary = _make(tmp_path, filter_file).run()

    assert summary["status"] == "success"
    assert summary["indexes_built"] == 1        # one secondary index (orders)
    assert summary["constraints_built"] == 2    # two PKs (orders, customers)
    creates = [e for e in tgt.executed if "CREATE INDEX" in e]
    adds = [e for e in tgt.executed if "ADD CONSTRAINT" in e]
    assert len(creates) == 1 and len(adds) == 2
    log = json.loads((tmp_path / PgCyanPostIndex.LOG_OUTPUT_FILENAME).read_text())
    assert log["status"] == "completed" and log["shard"] == "idx-0"


def test_idempotent_existing_objects_are_skipped(tmp_path, filter_file, monkeypatch):
    tgt = _TargetCursor(dup_index="orders_customer_id_idx", dup_constraint="orders_pkey")
    monkeypatch.setattr(pi_module.psycopg2, "connect", _connect_factory(tgt))
    summary = _make(tmp_path, filter_file).run()
    # duplicates are treated as already-applied (success), not failures
    assert summary["status"] == "success"
    assert summary["indexes_built"] == 1 and summary["constraints_built"] == 2


def test_apply_failure_is_a_phase_failure(tmp_path, filter_file, monkeypatch):
    tgt = _TargetCursor(fail="ADD CONSTRAINT")
    monkeypatch.setattr(pi_module.psycopg2, "connect", _connect_factory(tgt))
    summary = _make(tmp_path, filter_file).run()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanPostIndex.LOG_ERROR_FILENAME).exists()
    assert not (tmp_path / PgCyanPostIndex.LOG_OUTPUT_FILENAME).exists()


def test_missing_filter_file_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(pi_module.psycopg2, "connect", _connect_factory(_TargetCursor()))
    summary = _make(tmp_path, str(tmp_path / "nope.filters")).run()
    assert summary["status"] == "failure"
    assert summary["shard"] == "idx-0"


def test_connection_failure_writes_error(tmp_path, filter_file, monkeypatch):
    monkeypatch.setattr(pi_module.psycopg2, "connect",
                        _connect_factory(_TargetCursor(), raise_on="tgt"))
    summary = _make(tmp_path, filter_file).run()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanPostIndex.LOG_ERROR_FILENAME).exists()
