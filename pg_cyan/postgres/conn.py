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


def source_conn_defaults() -> Dict[str, object]:
    """Resolve the source connection settings for the introspection stages.

    Prefers a single ``PGCOPYDB_SOURCE_PGURI`` (e.g.
    ``postgres://user:pw@host:5432/db?sslmode=require``); when it is absent,
    falls back to the discrete ``PG_SOURCE_*`` vars (same defaults as before).
    URI userinfo/path components are percent-decoded (the URI is built with the
    parts URL-encoded). ``sslmode`` comes from the URI query (or ``PGSSLMODE``)
    and is ``None`` when unspecified — psycopg2's connect omits it in that case.

    Returns:
        Dict with ``host``/``port``/``user``/``password``/``database`` and
        ``sslmode`` (``Optional[str]``).
    """
    uri = os.getenv("PGCOPYDB_SOURCE_PGURI")
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
        "host": os.getenv("PG_SOURCE_HOST", "localhost"),
        "port": int(os.getenv("PG_SOURCE_PORT", "5432")),
        "user": os.getenv("PG_SOURCE_USER", "postgres"),
        "password": os.getenv("PG_SOURCE_PASSWORD", ""),
        "database": os.getenv("PG_SOURCE_DATABASE", "postgres"),
        "sslmode": os.getenv("PGSSLMODE"),
    }


def _source_default(field: str):
    """Return a zero-arg callable resolving one source setting at parse time.

    Used as a click option ``default`` so the URI is consulted lazily when the
    command runs (not at import), mirroring the existing ``os.getenv`` lambdas.
    """
    def _get() -> Optional[object]:
        return source_conn_defaults()[field]

    return _get
