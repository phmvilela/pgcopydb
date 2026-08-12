# pgcyan migration image -- the thin app layer on top of pgcyan-base.
# ---------------------------------------------------------------------------
# The heavy, rarely-changing bits (compiled pgcopydb, kubectl, python + the pip
# deps) live in pgcyan-base (see Dockerfile.base). This image only adds the
# pg_cyan code + the deploy/k8s manifests, so a normal commit builds in seconds:
# no compile, no pip install, no big context.
#
# BASE_IMAGE is supplied by CI as the hash-tagged base already published to the
# ACR (build.yml). The default is only for a local build after you have built
# the base once:  docker build -f Dockerfile.base -t pgcyan-base:local .
# ---------------------------------------------------------------------------
ARG BASE_IMAGE=pgcyan-base:local
FROM ${BASE_IMAGE}

WORKDIR /app

# App code + the manifests the orchestrator applies. The orchestrator
# (pg_cyan.cli orchestrate) reads these from
# PgCyanOrchestrator.DEFAULT_MANIFESTS_DIR, which resolves to ./deploy/k8s next
# to ./pg_cyan -- keep this path in sync with that default.
# See also: pg_cyan/orchestrator.py
COPY --chown=migration:migration pg_cyan/ ./pg_cyan/
COPY --chown=migration:migration deploy/k8s/ ./deploy/k8s/

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
