"""
Unit tests for the post-data FK build (`pg_cyan.cli build-foreign-keys`).

See also: pg_cyan/postfk.py

DB-free: psycopg2 connections are faked. Tests cover source FK introspection,
the NOT VALID + VALIDATE apply (happy path, idempotent DuplicateObject, per-FK
failure), the target verification counts, and the authoritative
target-pg-postload-log.json shape.
"""
import json

import psycopg2
import pytest

from pg_cyan import postfk as fk_module
from pg_cyan.postfk import PgCyanForeignKeys

# 2 source FKs.
_FKS = [
    ("public", "orders", "orders_customer_fk", "FOREIGN KEY (customer_id) REFERENCES customers(id)"),
    ("public", "order_items", "oi_order_fk", "FOREIGN KEY (order_id) REFERENCES orders(id)"),
]


class _SourceCursor:
    def __init__(self):
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append(str(query))

    def fetchall(self):
        return list(_FKS)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _TargetCursor:
    def __init__(self, add_dupe=None, fail_validate=None, counts=None):
        #: constraint name whose ADD raises DuplicateObject (already present).
        self._add_dupe = add_dupe
        #: constraint name whose VALIDATE raises (bad data / missing parent).
        self._fail_validate = fail_validate
        #: (index_count, validated_fk_count) returned by the two count queries.
        self._counts = counts or (5, len(_FKS))
        self.executed = []

    def execute(self, query, params=None):
        text = str(query)
        self.executed.append(text)
        if "ADD CONSTRAINT" in text and self._add_dupe and self._add_dupe in text:
            raise psycopg2.errors.DuplicateObject("already exists")
        if "VALIDATE CONSTRAINT" in text and self._fail_validate and self._fail_validate in text:
            raise fk_module.PgError("validation failed: violates FK")

    def fetchone(self):
        # order: _COUNT_INDEXES first, then _COUNT_FKS_VALIDATED
        last = self.executed[-1]
        return (self._counts[0],) if "pg_indexes" in last else (self._counts[1],)

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
    """Route source vs target dsn to the right fake cursor."""
    def _connect(dsn):
        if raise_on and raise_on in dsn:
            raise fk_module.PgError("could not connect")
        return _Conn(_SourceCursor()) if "src" in dsn else _Conn(target_cursor)
    return _connect


def _make(tmp_path, **kwargs):
    defaults = dict(source_pguri="postgres://src/db", target_pguri="postgres://tgt/db",
                    output_dir=str(tmp_path))
    defaults.update(kwargs)
    return PgCyanForeignKeys(**defaults)


def test_run_happy_path_applies_not_valid_then_validate(tmp_path, monkeypatch):
    tgt = _TargetCursor()
    monkeypatch.setattr(fk_module.psycopg2, "connect", _connect_factory(tgt))
    w = _make(tmp_path)
    summary = w.run()

    assert summary["status"] == "success"
    assert summary["fk_total"] == 2 and summary["fk_validated"] == 2
    # each FK: ADD ... NOT VALID then VALIDATE CONSTRAINT
    adds = [e for e in tgt.executed if "ADD CONSTRAINT" in e and "NOT VALID" in e]
    vals = [e for e in tgt.executed if "VALIDATE CONSTRAINT" in e]
    assert len(adds) == 2 and len(vals) == 2

    log = json.loads((tmp_path / PgCyanForeignKeys.LOG_OUTPUT_FILENAME).read_text())
    assert log["status"] == "completed"
    assert log["fk_total"] == 2 and log["fk_validated"] == 2
    assert log["target_index_count"] == 5


def test_idempotent_existing_constraint_still_validates(tmp_path, monkeypatch):
    # ADD raises DuplicateObject for the first FK; it must still be validated.
    tgt = _TargetCursor(add_dupe="orders_customer_fk")
    monkeypatch.setattr(fk_module.psycopg2, "connect", _connect_factory(tgt))
    w = _make(tmp_path)
    summary = w.run()
    assert summary["status"] == "success"
    assert summary["fk_validated"] == 2
    assert sum(1 for e in tgt.executed if "VALIDATE CONSTRAINT" in e) == 2


def test_validate_failure_is_a_phase_failure(tmp_path, monkeypatch):
    tgt = _TargetCursor(fail_validate="oi_order_fk", counts=(5, 1))
    monkeypatch.setattr(fk_module.psycopg2, "connect", _connect_factory(tgt))
    w = _make(tmp_path)
    summary = w.run()
    assert summary["status"] == "failure"
    assert summary["fk_validated"] == 1        # only the first FK validated
    assert (tmp_path / PgCyanForeignKeys.LOG_ERROR_FILENAME).exists()
    assert not (tmp_path / PgCyanForeignKeys.LOG_OUTPUT_FILENAME).exists()


def test_connection_failure_writes_error(tmp_path, monkeypatch):
    monkeypatch.setattr(fk_module.psycopg2, "connect",
                        _connect_factory(_TargetCursor(), raise_on="tgt"))
    w = _make(tmp_path)
    summary = w.run()
    assert summary["status"] == "failure"
    assert (tmp_path / PgCyanForeignKeys.LOG_ERROR_FILENAME).exists()
