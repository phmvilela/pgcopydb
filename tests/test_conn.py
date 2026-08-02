"""Unit tests for the source-connection resolver (pg_cyan.postgres.conn).

DB-free: exercises only the env/URI parsing, no Postgres connection.
"""
from pg_cyan.postgres.conn import source_conn_defaults


def test_defaults_when_nothing_set(monkeypatch):
    for var in ("PGCOPYDB_SOURCE_PGURI", "PG_SOURCE_HOST", "PG_SOURCE_PORT",
                "PG_SOURCE_USER", "PG_SOURCE_PASSWORD", "PG_SOURCE_DATABASE",
                "PGSSLMODE"):
        monkeypatch.delenv(var, raising=False)
    assert source_conn_defaults() == {
        "host": "localhost", "port": 5432, "user": "postgres",
        "password": "", "database": "postgres", "sslmode": None,
    }


def test_discrete_env_when_no_uri(monkeypatch):
    monkeypatch.delenv("PGCOPYDB_SOURCE_PGURI", raising=False)
    monkeypatch.setenv("PG_SOURCE_HOST", "h")
    monkeypatch.setenv("PG_SOURCE_PORT", "6543")
    monkeypatch.setenv("PG_SOURCE_USER", "u")
    monkeypatch.setenv("PG_SOURCE_PASSWORD", "pw")
    monkeypatch.setenv("PG_SOURCE_DATABASE", "d")
    monkeypatch.setenv("PGSSLMODE", "require")
    d = source_conn_defaults()
    assert d == {"host": "h", "port": 6543, "user": "u", "password": "pw",
                 "database": "d", "sslmode": "require"}
    assert isinstance(d["port"], int)


def test_uri_takes_precedence_and_decodes(monkeypatch):
    # userinfo/db are percent-encoded in the URI (built with the parts quoted).
    monkeypatch.setenv("PG_SOURCE_HOST", "ignored")  # URI wins wholesale
    monkeypatch.setenv(
        "PGCOPYDB_SOURCE_PGURI",
        "postgres://us%40er:p%2Fw@db.example.com:6432/my%20db?sslmode=require",
    )
    d = source_conn_defaults()
    assert d["host"] == "db.example.com"
    assert d["port"] == 6432
    assert d["user"] == "us@er"
    assert d["password"] == "p/w"
    assert d["database"] == "my db"
    assert d["sslmode"] == "require"


def test_uri_without_sslmode_falls_back_to_pgsslmode(monkeypatch):
    monkeypatch.setenv("PGSSLMODE", "prefer")
    monkeypatch.setenv("PGCOPYDB_SOURCE_PGURI", "postgres://u:pw@h:5432/db")
    assert source_conn_defaults()["sslmode"] == "prefer"


def test_uri_missing_parts_use_defaults(monkeypatch):
    monkeypatch.delenv("PGSSLMODE", raising=False)
    # no port, no userinfo, no path
    monkeypatch.setenv("PGCOPYDB_SOURCE_PGURI", "postgres://only-host")
    d = source_conn_defaults()
    assert d["host"] == "only-host"
    assert d["port"] == 5432
    assert d["user"] == "postgres"
    assert d["password"] == ""
    assert d["database"] == "postgres"
    assert d["sslmode"] is None
