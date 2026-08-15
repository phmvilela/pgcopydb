# pgcyan control-plane API image -- the thin app layer on top of the base.
# ---------------------------------------------------------------------------
# A small, standalone FastAPI service -- NOT the heavy migration image (that one
# compiles pgcopydb). The control plane talks to the Kubernetes API via its
# in-cluster ServiceAccount to provision per-migration namespaces.
#
# The python + pip deps live in the base (see control-plane/api/Dockerfile.base,
# rebuilt only when requirements.txt changes); this image adds the app code +
# the shared deploy/k8s manifests it applies verbatim (no drift with the
# integration test), so a normal commit builds in seconds.
#
# BUILD CONTEXT = REPO ROOT, so it can bake in deploy/k8s. Build with:
#   docker build -f control-plane/api/Dockerfile \
#       --build-arg BASE_IMAGE=pgcyan-control-plane-base:local -t pgcyan-control-plane:dev .
#   az acr build -r <acr> -f control-plane/api/Dockerfile \
#       --build-arg BASE_IMAGE=<acr>/pgcyan-control-plane-base:<tag> \
#       --image pgcyan-control-plane:<tag> .
#
# Runs as non-root (uid/gid 1000) to match the pod securityContext.
# Acceptance (#86): `GET /health` returns 200. (#87): `POST /migrations` provisions.
# ---------------------------------------------------------------------------
ARG BASE_IMAGE=pgcyan-control-plane-base:local
FROM ${BASE_IMAGE}

WORKDIR /app

COPY --chown=app:app control-plane/api/app ./app
# The migration manifests the provisioner applies verbatim (issue #87).
COPY --chown=app:app deploy/k8s ./deploy/k8s
# The shared, dependency-free capacity-input contract (issue #141). Vendored as
# the two leaf files rather than installing the heavy migration package, so
# `from pg_cyan import capacity_contract` resolves at runtime and the API
# validates request bodies by the exact rules the planner reads (no drift).
COPY --chown=app:app pg_cyan/__init__.py pg_cyan/capacity_contract.py ./pg_cyan/

USER 1000:1000

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
