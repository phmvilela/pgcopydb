"""
Source-connection resolution shared by the psycopg2-based pre-migration stages.

The pgcopydb-native stages (``copy``, ``schema``, …) take a single
libpq URI via ``PGCOPYDB_SOURCE_PGURI``/``PGCOPYDB_TARGET_PGURI``. The
introspection stages (``source-pg-metrics``, ``source-largeobjects``) instead
connect with psycopg2 and historically read the discrete ``PG_SOURCE_*`` vars.

In-cluster there is now a single ``pgcyan-db-uris`` Secret carrying the two
pgcopydb URIs (no separate ``PG_SOURCE_*`` Secret), so the collector Job that
runs those two stages only has ``PGCOPYDB_SOURCE_PGURI`` in its environment.
``source_conn_defaults`` bridges the two conventions: it prefers
``PGCOPYDB_SOURCE_PGURI`` when set and otherwise falls back to the discrete
``PG_SOURCE_*`` vars, so the same commands work locally and in the pod.
"""
import os
from typing import Dict, Optional
from urllib.parse import parse_qs, unquote, urlparse


def _conn_defaults(uri_env: str, prefix: str) -> Dict[str, object]:
    """Resolve psycopg2 connection settings from a pgcopydb URI or discrete vars.

    Prefers a single ``$<uri_env>`` (e.g.
    ``postgres://user:pw@host:5432/db?sslmode=require``); when it is absent, falls
    back to the discrete ``$<prefix>_HOST``/``_PORT``/``_USER``/``_PASSWORD``/
    ``_DATABASE`` vars. URI userinfo/path components are percent-decoded (the URI
    is built with the parts URL-encoded). ``sslmode`` comes from the URI query (or
    ``PGSSLMODE``) and is ``None`` when unspecified — psycopg2 omits it then.

    Returns:
        Dict with ``host``/``port``/``user``/``password``/``database`` and
        ``sslmode`` (``Optional[str]``).
    """
    uri = os.getenv(uri_env)
    if uri:
        parts = urlparse(uri)
        db = parts.path.lstrip("/")
        sslmode = parse_qs(parts.query).get("sslmode", [None])[0]
        return {
            "host": parts.hostname or "localhost",
            "port": parts.port or 5432,
            "user": unquote(parts.username) if parts.username else "postgres",
            "password": unquote(parts.password) if parts.password else "",
            "database": unquote(db) if db else "postgres",
            "sslmode": sslmode or os.getenv("PGSSLMODE"),
        }
    return {
        "host": os.getenv(f"{prefix}_HOST", "localhost"),
        "port": int(os.getenv(f"{prefix}_PORT", "5432")),
        "user": os.getenv(f"{prefix}_USER", "postgres"),
        "password": os.getenv(f"{prefix}_PASSWORD", ""),
        "database": os.getenv(f"{prefix}_DATABASE", "postgres"),
        "sslmode": os.getenv("PGSSLMODE"),
    }


def source_conn_defaults() -> Dict[str, object]:
    """Source connection settings: ``PGCOPYDB_SOURCE_PGURI`` else ``PG_SOURCE_*``."""
    return _conn_defaults("PGCOPYDB_SOURCE_PGURI", "PG_SOURCE")


def target_conn_defaults() -> Dict[str, object]:
    """Target connection settings: ``PGCOPYDB_TARGET_PGURI`` else ``PG_TARGET_*``."""
    return _conn_defaults("PGCOPYDB_TARGET_PGURI", "PG_TARGET")


def _source_default(field: str):
    """Zero-arg callable resolving one source setting at click parse time."""
    def _get() -> Optional[object]:
        return source_conn_defaults()[field]

    return _get


def _target_default(field: str):
    """Zero-arg callable resolving one target setting at click parse time."""
    def _get() -> Optional[object]:
        return target_conn_defaults()[field]

    return _get
