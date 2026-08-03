"""Request/response models for the control-plane API (issues #87, #88)."""
from typing import Optional
from urllib.parse import quote, urlsplit

from pydantic import BaseModel, Field, model_validator


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


class MigrationRequest(BaseModel):
    """Body of ``POST /migrations``: source + target, optional shard count."""
    source: DbConn
    target: DbConn
    shards: Optional[int] = Field(default=None, ge=1, description="copy shard count; planner default if omitted")


class MigrationCreated(BaseModel):
    """Response of ``POST /migrations``."""
    id: str
    namespace: str
    state: str = "waiting-to-start"


class MigrationRecord(BaseModel):
    """A registry row as returned by ``GET /migrations`` (no credentials)."""
    id: str
    namespace: str
    source: str
    target: str
    state: str
    created_at: str
    updated_at: str


class MigrationStarted(BaseModel):
    """Response of ``POST /migrations/{id}/start``."""
    id: str
    namespace: str
    state: str            # registry state after the start call
    message: str


class MigrationStatus(BaseModel):
    """Response of ``GET /migrations/{id}/status`` (issue #89).

    ``state`` is the registry's view (authoritative for the singleton). The
    controller's live ``/status`` payload is nested under ``controller`` when the
    pod is reachable, else ``null`` with ``controller_reachable=false``.
    """
    id: str
    namespace: str
    state: str
    controller_reachable: bool
    controller: Optional[dict] = None
