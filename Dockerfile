# pgcyan migration image (issue #39)
# ---------------------------------------------------------------------------
# One image serves every deploy/k8s/ pod: the planner Job, the pgcopydb phase
# Jobs/Deployments, and the custom-Python phases. It therefore bundles three
# things on PATH:
#   * pgcopydb        -- the copy/CDC engine (phase Jobs invoke `pgcopydb ...`)
#   * kubectl         -- the planner publishes the plan ConfigMap with kubectl
#   * python + pg_cyan CLI -- planner/monitoring/cutover (`python -m pg_cyan.cli`)
#
# Tool versions are set via build ARGs. kubectl is fully pinned + checksum
# verified; pgcopydb takes the PGDG repo's current build unless you pin an exact
# apt version (PGCOPYDB_APT_VERSION) -- an unavailable pin fails the build.
#
# Acceptance (issue #39):
#   docker run <image> pgcopydb --version           -> succeeds
#   docker run <image> python -m pg_cyan.cli --help  -> succeeds
# ---------------------------------------------------------------------------
FROM python:3.10-slim AS base

# Pinned tool versions.
#   PGCOPYDB_APT_VERSION -- exact PGDG apt version string pgcopydb is pinned to
#     (verified building on Debian trixie: `pgcopydb --version` -> 0.18). Bump
#     deliberately; an unavailable pin fails the build rather than drifting.
#     Set to empty to float to the PGDG repo's current build.
#   KUBECTL_VERSION  -- matches the AKS control-plane version pinned in
#     terraform/aks (kubernetes 1.33); kubectl is supported +/-1 minor.
ARG PGCOPYDB_APT_VERSION=0.18-1.pgdg13+1
ARG KUBECTL_VERSION=v1.33.0

WORKDIR /app

# --- pgcopydb (+ postgres client) from the PostgreSQL PGDG apt repository ----
# PGDG is pgcopydb's canonical distribution and resolves its shared-library
# deps (libpq, etc.) transitively, which copying a bare binary would not.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg; \
    install -d /usr/share/postgresql-common/pgdg; \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc; \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(. /etc/os-release && echo "$VERSION_CODENAME")-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    if [ -n "${PGCOPYDB_APT_VERSION}" ]; then \
        apt-get install -y --no-install-recommends postgresql-client "pgcopydb=${PGCOPYDB_APT_VERSION}"; \
    else \
        apt-get install -y --no-install-recommends postgresql-client pgcopydb; \
    fi; \
    pgcopydb --version; \
    apt-get purge -y --auto-remove gnupg; \
    rm -rf /var/lib/apt/lists/*

# --- kubectl (pinned, checksum-verified against upstream) -------------------
# The planner Job runs `kubectl apply` to publish the pgcyan-plan ConfigMap.
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    base="https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${arch}"; \
    curl -fsSLo /usr/local/bin/kubectl "${base}/kubectl"; \
    curl -fsSLo /tmp/kubectl.sha256 "${base}/kubectl.sha256"; \
    echo "$(cat /tmp/kubectl.sha256)  /usr/local/bin/kubectl" | sha256sum -c -; \
    chmod +x /usr/local/bin/kubectl; \
    rm -f /tmp/kubectl.sha256; \
    kubectl version --client

# --- Python app -------------------------------------------------------------
# Copy requirements first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY pg_cyan/ ./pg_cyan/
# The orchestrator (pg_cyan.cli orchestrate, issue #40) applies these manifests
# from PgCyanOrchestrator.DEFAULT_MANIFESTS_DIR, which resolves to ./deploy/k8s
# next to ./pg_cyan -- keep this path in sync with that default.
COPY deploy/k8s/ ./deploy/k8s/

# Create non-root user for security
RUN useradd -m -u 1000 migration && chown -R migration:migration /app

USER migration

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# No hardcoded ENTRYPOINT on purpose. Every deploy/k8s/ pod sets its own
# `command:` (pgcopydb / kubectl / sh), and issue #39's acceptance runs the
# tools directly -- `docker run <img> pgcopydb --version`,
# `docker run <img> python -m pg_cyan.cli --help`. A hardcoded entrypoint would
# swallow those into `python -m pg_cyan.cli <args>`. With only a default CMD,
# `docker run <img>` still prints the CLI help, but any explicit command runs
# as given.
CMD ["python", "-m", "pg_cyan.cli", "--help"]
