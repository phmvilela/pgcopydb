# pgcyan migration image
# ---------------------------------------------------------------------------
# One image serves every deploy/k8s/ pod: the planner Job, the pgcopydb step
# Jobs/Deployments, and the custom-Python steps. It therefore bundles three
# things on PATH:
#   * pgcopydb        -- the copy/CDC engine (step Jobs invoke `pgcopydb ...`)
#   * kubectl         -- the planner publishes the plan ConfigMap with kubectl
#   * python + pg_cyan CLI -- planner/monitoring/cutover (`python -m pg_cyan.cli`)
#
# Tool versions are set via build ARGs. kubectl is fully pinned + checksum
# verified; pgcopydb is compiled from the phmvilela/pgcopydb fork pinned to an
# exact commit (PGCOPYDB_GIT_REF) in a multi-stage builder -- see the pgcopydb
# section below for why we build from source rather than apt-install PGDG.
# ---------------------------------------------------------------------------

# --- pgcopydb: compiled from the phmvilela/pgcopydb fork (multi-stage) -------
# Built from source rather than apt-installing PGDG because the migration needs
# a fork patch to the SQLite CDC apply path: flush the libpq pipeline when the
# replay queue drains, so a trailing CDC change lands on the target (stock 0.18
# does not). Pin PGCOPYDB_GIT_REF to the fork commit for a reproducible image.
#   PGVERSION -- the postgresql-server-dev / libpq major the binary links.
ARG PGVERSION=16
ARG PGCOPYDB_GIT_URL=https://github.com/phmvilela/pgcopydb.git
ARG PGCOPYDB_GIT_REF=428a7f5137660da078ff7da5ee4ae00d2f75ba72

FROM debian:12-slim AS pgcopydb-build
ARG PGVERSION
ARG PGCOPYDB_GIT_URL
ARG PGCOPYDB_GIT_REF
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg git; \
    install -d /usr/share/postgresql-common/pgdg; \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc; \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main ${PGVERSION}" \
        > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        build-essential make \
        libedit-dev libgc-dev libicu-dev libkrb5-dev liblz4-dev libncurses-dev \
        libpam-dev libpq-dev libpq5 libreadline-dev libselinux1-dev libssl-dev \
        libxml2-dev libxslt1-dev libzstd-dev zlib1g-dev \
        "postgresql-server-dev-${PGVERSION}"; \
    rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src
RUN set -eux; \
    git clone --filter=blob:none "${PGCOPYDB_GIT_URL}" pgcopydb; \
    cd pgcopydb; \
    git checkout "${PGCOPYDB_GIT_REF}"
WORKDIR /usr/src/pgcopydb
# Build the bundled SQLite (CDC store) first, then pgcopydb itself. `install`
# drops the binary at /usr/lib/postgresql/${PGVERSION}/bin/pgcopydb.
RUN set -eux; \
    make -C src/bin/lib/sqlite clean sqlite3.o sqlite3; \
    install src/bin/lib/sqlite/sqlite3 /usr/local/bin/sqlite3; \
    make -s clean; \
    make -s -j"$(nproc)" install; \
    "/usr/lib/postgresql/${PGVERSION}/bin/pgcopydb" --version

# ---------------------------------------------------------------------------
FROM python:3.10-slim AS base

# Pinned tool versions.
#   KUBECTL_VERSION  -- matches the AKS control-plane version pinned in
#     terraform/aks (kubernetes 1.33); kubectl is supported +/-1 minor.
ARG PGVERSION=16
ARG KUBECTL_VERSION=v1.33.0

WORKDIR /app

# --- pgcopydb runtime: the fork binary + its shared-lib deps ----------------
# psql (postgresql-client) is used by the step Jobs and pulls libpq5; libgc1
# is pgcopydb's Boehm-GC runtime dep. Both from PGDG so the major matches the
# builder's libpq. The pgcopydb binary + bundled sqlite3 are copied from the
# builder stage.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg; \
    install -d /usr/share/postgresql-common/pgdg; \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc; \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(. /etc/os-release && echo "$VERSION_CODENAME")-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        "postgresql-client-${PGVERSION}" \
        libgc1 libedit2 libncurses6 liblz4-1 libzstd1 \
        libxml2 libxslt1.1 libselinux1 libreadline8; \
    apt-get purge -y --auto-remove gnupg; \
    rm -rf /var/lib/apt/lists/*
COPY --from=pgcopydb-build /usr/lib/postgresql/${PGVERSION}/bin/pgcopydb /usr/local/bin/pgcopydb
COPY --from=pgcopydb-build /usr/local/bin/sqlite3 /usr/local/bin/sqlite3
RUN pgcopydb --version

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
# The orchestrator (pg_cyan.cli orchestrate) applies these manifests from
# PgCyanOrchestrator.DEFAULT_MANIFESTS_DIR, which resolves to ./deploy/k8s
# next to ./pg_cyan -- keep this path in sync with that default.
# See also: pg_cyan/orchestrator.py
COPY deploy/k8s/ ./deploy/k8s/

# Create non-root user for security
RUN useradd -m -u 1000 migration && chown -R migration:migration /app

USER migration

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# No hardcoded ENTRYPOINT on purpose. Every deploy/k8s/ pod sets its own
# `command:` (pgcopydb / kubectl / sh), and the acceptance checks run the
# tools directly -- `docker run <img> pgcopydb --version`,
# `docker run <img> python -m pg_cyan.cli --help`. A hardcoded entrypoint would
# swallow those into `python -m pg_cyan.cli <args>`. With only a default CMD,
# `docker run <img>` still prints the CLI help, but any explicit command runs
# as given.
CMD ["python", "-m", "pg_cyan.cli", "--help"]
