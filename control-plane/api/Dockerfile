# pgcyan control-plane API image (issue #86).
# ---------------------------------------------------------------------------
# A small, standalone FastAPI service -- deliberately NOT the heavy migration
# image (that one compiles pgcopydb + bundles kubectl for the DAG pods). The
# control plane talks to the Kubernetes API via the in-cluster ServiceAccount,
# so it needs no kubectl/pgcopydb; keep it slim. The k8s client + DB drivers
# arrive with the provisioning/registry tickets (#87/#88).
#
# Runs as a non-root user (uid/gid 1000) to match the pod securityContext
# (runAsNonRoot + fsGroup 1000 so the mounted /data RWO volume is writable).
#
# Acceptance (issue #86): `GET /health` returns 200 once the pod is Running.
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install deps first for layer caching, then the app.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Non-root runtime user (uid 1000 == pod runAsUser/fsGroup).
RUN groupadd --gid 1000 app && useradd --uid 1000 --gid 1000 --no-create-home app
USER 1000:1000

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
