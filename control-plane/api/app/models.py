"""Request/response models for the control-plane API."""
from typing import List, Optional
from urllib.parse import quote, urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DbConn(BaseModel):
    """A source or target Postgres connection.

    Either a full ``uri`` (pgcopydb-style ``postgres://user:pw@host:port/db``) or
    the discrete parts (``host`` + ``user`` + ``database`` required) — the parts
    are assembled into a URI with userinfo percent-encoded.
    """
    uri: Optional[str] = None
    host: Optional[str] = None
    port: int = 5432
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    sslmode: str = "require"

    @model_validator(mode="after")
    def _require_uri_or_parts(self) -> "DbConn":
        if not self.uri and not (self.host and self.user and self.database):
            raise ValueError("provide either 'uri' or host + user + database")
        return self

    def to_pguri(self) -> str:
        """Return the libpq/pgcopydb URI (percent-encoding userinfo)."""
        if self.uri:
            return self.uri
        user = quote(self.user or "", safe="")
        pw = quote(self.password or "", safe="")
        auth = f"{user}:{pw}@" if (self.user or self.password) else ""
        return f"postgres://{auth}{self.host}:{self.port}/{self.database}?sslmode={self.sslmode}"

    def endpoint(self) -> str:
        """Credential-free ``host:port/db`` label for logs and the registry.

        Never includes user/password -- the registry must not store secrets.
        """
        if self.uri:
            parts = urlsplit(self.uri)
            host = parts.hostname or ""
            port = parts.port or 5432
            db = parts.path.lstrip("/")
            return f"{host}:{port}/{db}"
        return f"{self.host}:{self.port}/{self.database}"


class CapacityInputs(BaseModel):
    """Operator-declared capacity objects for the capacity-aware planner.

    Each is a free-form JSON object validated at the endpoint against the shared
    contract (``pg_cyan/collect/capacity_contract.py``) — the same rules the planner and
    the collectors use, so what the API accepts is exactly what the planner reads.
    The values are intentionally untyped here (``dict``) to avoid re-encoding the
    contract in pydantic; only the operator-declared subset is accepted. The
    machine-measured `copylog-history` is produced by a collector, not declared,
    so it is deliberately not a field here (unknown keys are rejected).

    Where a collector later supplies a measured value for the same input, it
    overrides the declared one; until then the declared value is what the plan
    uses. Absent inputs fall back to the planner's static defaults.
    """
    source_capacity: Optional[dict] = Field(default=None, description="source read ceiling, connections, WAL headroom")
    target_capacity: Optional[dict] = Field(default=None, description="target write ceiling, connections")
    network: Optional[dict] = Field(default=None, description="source<->cluster bandwidth + RTT")
    cluster_capacity: Optional[dict] = Field(default=None, description="allocatable CPU, nodes, pods/node")
    constraints: Optional[dict] = Field(default=None, description="operator clamp layer (tighten-only)")

    model_config = ConfigDict(extra="forbid")

    def to_bundle(self) -> dict:
        """The present objects, keyed for ``capacity_contract`` / the declared file."""
        present = {
            "source_capacity": self.source_capacity,
            "target_capacity": self.target_capacity,
            "network": self.network,
            "cluster_capacity": self.cluster_capacity,
            "constraints": self.constraints,
        }
        return {k: v for k, v in present.items() if v is not None}


class MigrationRequest(BaseModel):
    """Body of ``POST /migrations``: source + target, optional shard count + capacity."""
    source: DbConn = Field(description="the source Postgres to migrate FROM")
    target: DbConn = Field(description="the target Postgres to migrate TO")
    shards: Optional[int] = Field(default=None, ge=1, description="copy shard count; planner default if omitted")
    capacity: Optional[CapacityInputs] = Field(
        default=None,
        description="operator-declared capacity inputs; validated against the shared contract")

    model_config = ConfigDict(json_schema_extra={"examples": [{
        "source": {"host": "src.postgres.database.azure.com", "port": 5432,
                   "user": "migrator", "password": "s3cret", "database": "appdb"},
        "target": {"uri": "postgres://migrator:s3cret@tgt.postgres.database.azure.com"
                          ":5432/appdb?sslmode=require"},
        "shards": 4,
        "capacity": {
            "source_capacity": {"read_throughput_bytes_per_sec": 209715200,
                                "max_connections": 500},
            "network": {"bandwidth_bytes_per_sec": 1073741824, "rtt_ms": 12},
        },
    }]})


class MigrationCreated(BaseModel):
    """Response of ``POST /migrations``."""
    id: str
    namespace: str
    state: str = "waiting-to-start"
    #: Capacity objects delivered into the namespace for the planner (bundle keys).
    capacity_declared: List[str] = []

    model_config = ConfigDict(json_schema_extra={"examples": [
        {"id": "a1b2c3d4e5", "namespace": "pgcyan-mig-a1b2c3d4e5",
         "state": "waiting-to-start", "capacity_declared": ["network", "source_capacity"]}]})


class MigrationRecord(BaseModel):
    """A registry row as returned by ``GET /migrations`` (no credentials)."""
    id: str
    namespace: str
    source: str
    target: str
    state: str
    created_at: str
    updated_at: str

    model_config = ConfigDict(json_schema_extra={"examples": [{
        "id": "a1b2c3d4e5", "namespace": "pgcyan-mig-a1b2c3d4e5",
        "source": "src.postgres.database.azure.com:5432/appdb",
        "target": "tgt.postgres.database.azure.com:5432/appdb",
        "state": "running",
        "created_at": "2026-08-03T12:00:00+00:00",
        "updated_at": "2026-08-03T12:03:21+00:00"}]})


class MigrationStarted(BaseModel):
    """Response of ``POST /migrations/{id}/start``."""
    id: str
    namespace: str
    state: str            # registry state after the start call
    message: str

    model_config = ConfigDict(json_schema_extra={"examples": [
        {"id": "a1b2c3d4e5", "namespace": "pgcyan-mig-a1b2c3d4e5",
         "state": "running", "message": "migration start accepted"}]})


class MigrationDone(BaseModel):
    """Response of ``POST /migrations/{id}/done``."""
    id: str
    namespace: str
    state: str = "done"
    graceful: bool
    #: Whether the controller dropped the source slot + publications before the
    #: namespace was deleted. ``False`` means a leaked slot may remain (e.g. the
    #: controller was unreachable) -- surfaced so the operator can reclaim it.
    released: bool = False
    message: str

    model_config = ConfigDict(json_schema_extra={"examples": [
        {"id": "a1b2c3d4e5", "namespace": "pgcyan-mig-a1b2c3d4e5", "state": "done",
         "graceful": True, "released": True,
         "message": "released the source and deleted the namespace"}]})


class MigrationStatus(BaseModel):
    """Response of ``GET /migrations/{id}/status``.

    ``state`` is the registry's view (authoritative for the singleton). The
    controller's live ``/status`` payload is nested under ``controller`` when the
    pod is reachable, else ``null`` with ``controller_reachable=false``.
    """
    id: str
    namespace: str
    state: str
    controller_reachable: bool
    controller: Optional[dict] = None

    model_config = ConfigDict(json_schema_extra={"examples": [{
        "id": "a1b2c3d4e5", "namespace": "pgcyan-mig-a1b2c3d4e5",
        "state": "running", "controller_reachable": True,
        "controller": {"state": "running", "ready": True, "snapshot": "00000002-0000002A-1",
                       "cdc": None, "steps": [{"id": "-1", "status": "succeeded"}],
                       "errors": []}}]})
