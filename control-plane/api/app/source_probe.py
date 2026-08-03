"""
Best-effort clash detection on the source database (issue #88).

Before provisioning, we check whether the source already carries the pgcyan
logical-replication artifacts (the ``pgcyan_slot`` replication slot / ``pgcyan_pub``
publication). Their presence means another migration -- a torn-down or externally
run one the registry can't see -- is using the source; provisioning a second would
collide on the slot at start.

This is *best effort*: connection/permission failures do not block provisioning
(non-clashing databases remain the operator's responsibility), they simply mean
"nothing detected". Kubernetes I/O and DB probing are injected so the API logic
stays unit-testable without a cluster or a live database.
"""
from typing import List

#: Fixed logical-replication names the migration uses (see the controller manifest
#: and the planner defaults). Kept identical here so we look for the real thing.
DEFAULT_SLOT = "pgcyan_slot"
DEFAULT_PUBLICATION = "pgcyan_pub"


class SourceProbe:
    """Looks for pre-existing pgcyan slot/publication on a source database."""

    def __init__(self, slot: str = DEFAULT_SLOT, publication: str = DEFAULT_PUBLICATION,
                 connect_timeout: int = 5):
        self.slot = slot
        self.publication = publication
        self.connect_timeout = connect_timeout

    def existing_artifacts(self, source_uri: str) -> List[str]:
        """Return human-readable names of pgcyan artifacts already on the source.

        Empty list means none found *or* the source could not be probed -- callers
        treat both as "no known clash" (best effort).
        """
        import psycopg2

        found: List[str] = []
        try:
            conn = psycopg2.connect(source_uri, connect_timeout=self.connect_timeout)
        except Exception:
            # Can't reach/authenticate: not our job to gate on that here; the
            # provisioned controller will surface a real connection error at start.
            return found
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot,),
                )
                if cur.fetchone():
                    found.append(f"replication slot '{self.slot}'")
                cur.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (self.publication,),
                )
                if cur.fetchone():
                    found.append(f"publication '{self.publication}'")
        except Exception:
            return []
        finally:
            conn.close()
        return found
