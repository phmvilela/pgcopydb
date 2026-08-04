"""Request-model tests."""
import pytest
from pydantic import ValidationError

from app.models import DbConn, MigrationRequest


def test_dbconn_from_parts_builds_percent_encoded_uri():
    c = DbConn(host="h", port=6432, user="u@x", password="p:w", database="db")
    assert c.to_pguri() == "postgres://u%40x:p%3Aw@h:6432/db?sslmode=require"


def test_dbconn_uri_passthrough_wins():
    c = DbConn(uri="postgres://u:p@h:5432/db?sslmode=require", host="ignored")
    assert c.to_pguri() == "postgres://u:p@h:5432/db?sslmode=require"


def test_endpoint_label_omits_credentials():
    # from parts
    assert DbConn(host="h", port=6432, user="u", password="p",
                  database="db").endpoint() == "h:6432/db"
    # from a uri (default port when absent)
    assert DbConn(uri="postgres://u:p@h/db").endpoint() == "h:5432/db"
    assert DbConn(uri="postgres://u:p@h:7000/db?sslmode=require").endpoint() == "h:7000/db"


def test_dbconn_requires_uri_or_parts():
    with pytest.raises(ValidationError):
        DbConn(host="h")  # missing user + database


def test_migration_request_rejects_nonpositive_shards():
    src = DbConn(uri="postgres://a/b")
    tgt = DbConn(uri="postgres://c/d")
    with pytest.raises(ValidationError):
        MigrationRequest(source=src, target=tgt, shards=0)
    # None (omitted) is fine.
    assert MigrationRequest(source=src, target=tgt).shards is None
