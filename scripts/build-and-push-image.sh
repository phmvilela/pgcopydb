#!/usr/bin/env bash
# Build and push the pgcyan migration image to Azure Container Registry (#39).
#
# One image serves every deploy/k8s/ pod (planner, pgcopydb steps, custom
# Python steps). This script builds it and pushes it to the ACR that
# terraform/aks provisions, then prints the digest-pinned reference to drop
# into the manifests.
#
# It prefers `az acr build` (server-side build via ACR Tasks -- no local Docker
# daemon needed); if you pass --local it falls back to `docker build` + push
# after `az acr login`.
#
# Usage:
#   scripts/build-and-push-image.sh [--local] [--acr NAME] [--tag TAG]
#
# Environment overrides (flags win over env):
#   ACR_NAME   ACR registry name          (default: pgcyanacr, matches terraform/aks)
#   IMAGE_REPO image repository           (default: pgcyan)
#   IMAGE_TAG  image tag                  (default: <pyproject version>)
#
# Prereqs: az CLI logged in (`az login`) with push rights on the registry;
# for --local also a running Docker daemon.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ACR_NAME="${ACR_NAME:-pgcyanacr}"
IMAGE_REPO="${IMAGE_REPO:-pgcyan}"
# Default tag = the version declared in pyproject/pg_cyan.__version__.
DEFAULT_TAG="$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "${REPO_ROOT}/pg_cyan/__init__.py")"
IMAGE_TAG="${IMAGE_TAG:-${DEFAULT_TAG:-latest}}"
MODE="acr"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --local) MODE="local"; shift ;;
    --acr)   ACR_NAME="$2"; shift 2 ;;
    --tag)   IMAGE_TAG="$2"; shift 2 ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

command -v az >/dev/null || { echo "az CLI not found; install the Azure CLI" >&2; exit 1; }

LOGIN_SERVER="$(az acr show --name "${ACR_NAME}" --query loginServer -o tsv)"
IMAGE="${LOGIN_SERVER}/${IMAGE_REPO}:${IMAGE_TAG}"

# The image is split into a heavy, rarely-changing base (compiled pgcopydb +
# kubectl + pip deps, Dockerfile.base) and a thin app layer (Dockerfile) that
# does `FROM <base>`. Build the base first (keyed on a hash of its inputs so an
# unchanged base is reused), then the app on top. See build.yml for the CI form.
BASE_TAG="$(cat "${REPO_ROOT}/Dockerfile.base" "${REPO_ROOT}/requirements.txt" | sha256sum | cut -c1-16)"

echo ">> Building ${IMAGE} (mode: ${MODE})"

if [[ "${MODE}" == "acr" ]]; then
  # Server-side build in ACR Tasks; no local Docker required.
  base="pgcyan-base:${BASE_TAG}"
  if az acr repository show --name "${ACR_NAME}" --image "${base}" >/dev/null 2>&1; then
    echo ">> Reusing published base ${base}"
  else
    echo ">> Building base ${base} (compiles pgcopydb + installs pip deps)"
    az acr build --registry "${ACR_NAME}" --image "${base}" \
      --file "${REPO_ROOT}/Dockerfile.base" "${REPO_ROOT}"
  fi
  az acr build \
    --registry "${ACR_NAME}" \
    --image "${IMAGE_REPO}:${IMAGE_TAG}" \
    --build-arg BASE_IMAGE="${LOGIN_SERVER}/${base}" \
    --file "${REPO_ROOT}/Dockerfile" \
    "${REPO_ROOT}"
else
  command -v docker >/dev/null || { echo "docker not found; --local needs a Docker daemon" >&2; exit 1; }
  az acr login --name "${ACR_NAME}"
  base="pgcyan-base:${BASE_TAG}"
  docker build -t "${base}" -f "${REPO_ROOT}/Dockerfile.base" "${REPO_ROOT}"
  docker build -t "${IMAGE}" --build-arg BASE_IMAGE="${base}" \
    -f "${REPO_ROOT}/Dockerfile" "${REPO_ROOT}"
  docker push "${IMAGE}"
fi

# Resolve the immutable digest so callers can pin by digest, not just tag.
DIGEST="$(az acr repository show --name "${ACR_NAME}" \
  --image "${IMAGE_REPO}:${IMAGE_TAG}" --query digest -o tsv 2>/dev/null || true)"

echo
echo ">> Pushed. Reference this image in deploy/k8s/ manifests:"
echo "   tag-pinned:    ${IMAGE}"
if [[ -n "${DIGEST}" ]]; then
  echo "   digest-pinned: ${LOGIN_SERVER}/${IMAGE_REPO}@${DIGEST}"
fi
