"""
Node-local SQLite registry of control-plane migrations (issue #88).

The control plane provisions per-migration namespaces (#87); this records each one
in a SQLite database on the ReadWriteOnce PVC (#86) so the state survives API pod
restarts and so we can enforce **one active control-plane migration at a time**.

Only non-sensitive endpoint *labels* (``host:port/db``) are stored -- never
credentials. The connection URIs live solely in the per-migration Kubernetes
Secret, not here.

A row's ``state`` mirrors the controller lifecycle
(``waiting-to-start`` -> ``running`` -> ``succeeded`` | ``failed`` | ``halted``);
the first two are *active*, the rest terminal.

**Singleton enforcement is delegated to SQLite.** A partial unique index over a
constant expression, restricted to the active states, makes it physically
impossible to have more than one active row. ``reserve`` inserts a parked row and
returns ``False`` when that constraint fires -- the loser of a race, decided by
the database, not by application-level check-then-act. When a migration reaches a
terminal state it drops out of the index, freeing the slot automatically (no
explicit release, so a crashed pod can't wedge the control plane).
"""
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Dict, List, Optional

#: States that count as an in-flight migration for the singleton rule.
ACTIVE_STATES = ("waiting-to-start", "running")

#: SQL list literal of the active states, for the partial-index predicate.
_ACTIVE_SQL = ", ".join("'%s'" % s for s in ACTIVE_STATES)

_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS migrations (
    id          TEXT PRIMARY KEY,
    namespace   TEXT NOT NULL,
    source      TEXT NOT NULL,
    target      TEXT NOT NULL,
    state       TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- At most one active migration, enforced by the database: a unique index on a
-- constant, partial to the active states, so all active rows share one key.
CREATE UNIQUE INDEX IF NOT EXISTS one_active_migration
    ON migrations((1)) WHERE state IN ({_ACTIVE_SQL});
"""


class Registry:
    """SQLite-backed record of provisioned migrations (one file on the PVC)."""

    def __init__(self, db_path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn, conn:
            conn.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        # Default 5s busy timeout lets a concurrent writer wait rather than raise
        # SQLITE_BUSY -- the loser of a reserve race then sees the winner's row.
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def reserve(self, migration_id: str, namespace: str, source: str, target: str,
                now: str) -> bool:
        """Insert a parked (``waiting-to-start``) migration, claiming the slot.

        Returns ``True`` if the row was created, ``False`` if an active migration
        already holds the slot (the partial unique index rejected the insert).
        This is the atomic singleton gate: the database picks the winner.
        """
        try:
            with closing(self._connect()) as conn, conn:
                conn.execute(
                    "INSERT INTO migrations "
                    "(id, namespace, source, target, state, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 'waiting-to-start', ?, ?)",
                    (migration_id, namespace, source, target, now, now),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def remove(self, migration_id: str) -> None:
        """Delete a migration row (used to roll back a failed reservation)."""
        with closing(self._connect()) as conn, conn:
            conn.execute("DELETE FROM migrations WHERE id = ?", (migration_id,))

    def record(self, migration_id: str, namespace: str, source: str, target: str,
               state: str, now: str) -> None:
        """Insert a migration in an explicit state (test/seed helper).

        Raises ``sqlite3.IntegrityError`` if it would create a second active
        migration -- production code claims the slot via :meth:`reserve`.
        """
        with closing(self._connect()) as conn, conn:
            conn.execute(
                "INSERT INTO migrations "
                "(id, namespace, source, target, state, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (migration_id, namespace, source, target, state, now, now),
            )

    def set_state(self, migration_id: str, state: str, now: str) -> None:
        """Advance a migration's lifecycle state (terminal states free the slot)."""
        with closing(self._connect()) as conn, conn:
            conn.execute(
                "UPDATE migrations SET state = ?, updated_at = ? WHERE id = ?",
                (state, now, migration_id),
            )

    def get(self, migration_id: str) -> Optional[Dict]:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM migrations WHERE id = ?", (migration_id,)
            ).fetchone()
        return dict(row) if row else None

    def list(self) -> List[Dict]:
        """All migrations, newest first."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM migrations ORDER BY created_at DESC, id DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def active(self) -> List[Dict]:
        """Migrations still in flight (0 or 1, given the singleton index)."""
        placeholders = ", ".join("?" * len(ACTIVE_STATES))
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT * FROM migrations WHERE state IN ({placeholders}) "
                "ORDER BY created_at DESC",
                ACTIVE_STATES,
            ).fetchall()
        return [dict(r) for r in rows]
