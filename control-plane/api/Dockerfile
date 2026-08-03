# pgcyan control-plane API image (issues #86, #87).
# ---------------------------------------------------------------------------
# A small, standalone FastAPI service -- NOT the heavy migration image (that one
# compiles pgcopydb + bundles the pg_cyan CLI for the DAG pods). The control
# plane talks to the Kubernetes API via its in-cluster ServiceAccount to
# provision per-migration namespaces.
#
# BUILD CONTEXT = REPO ROOT (same as the migration Dockerfile), so it can bake
# in the shared deploy/k8s manifests it applies verbatim (no drift with the
# integration test). Build with an explicit --file:
#   docker build -f control-plane/api/Dockerfile -t pgcyan-control-plane:dev .
#   az acr build -r <acr> -f control-plane/api/Dockerfile \
#       --image pgcyan-control-plane:<tag> .
#
# Runs as non-root (uid/gid 1000) to match the pod securityContext.
# Acceptance (#86): `GET /health` returns 200. (#87): `POST /migrations` provisions.
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PGCYAN_MANIFESTS_DIR=/app/deploy/k8s

WORKDIR /app

# Deps first for layer caching, then the app + the shared manifests.
COPY control-plane/api/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY control-plane/api/app ./app
# The migration manifests the provisioner applies verbatim (issue #87).
COPY deploy/k8s ./deploy/k8s

# Non-root runtime user (uid 1000 == pod runAsUser/fsGroup).
RUN groupadd --gid 1000 app && useradd --uid 1000 --gid 1000 --no-create-home app
USER 1000:1000

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
