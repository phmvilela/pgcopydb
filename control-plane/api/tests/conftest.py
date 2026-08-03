"""Shared fixtures for the control-plane API tests."""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app, get_provisioner, get_registry, get_source_probe
from app.provisioner import Provisioner
from app.registry import Registry

# The real shared manifests the provisioner reads (repo deploy/k8s), so the
# tests exercise the actual files that ship in the image (no drift).
MANIFESTS_DIR = Path(__file__).resolve().parents[3] / "deploy" / "k8s"


class FakeApplier:
    """Records what the provisioner would apply, without touching a cluster."""

    def __init__(self):
        self.namespaces = []
        self.secrets = []
        self.docs = []

    def create_namespace(self, name, labels):
        self.namespaces.append((name, labels))

    def create_secret(self, namespace, name, string_data, labels=None):
        self.secrets.append((namespace, name, string_data))

    def apply_doc(self, doc):
        self.docs.append(doc)


class FakeProbe:
    """Reports whatever artifacts it's told to -- no real DB connection."""

    def __init__(self, artifacts=None):
        self.artifacts = list(artifacts or [])
        self.probed = []

    def existing_artifacts(self, source_uri):
        self.probed.append(source_uri)
        return list(self.artifacts)


@pytest.fixture
def manifests_dir():
    return MANIFESTS_DIR


@pytest.fixture
def fake_applier():
    return FakeApplier()


@pytest.fixture
def registry(tmp_path):
    """A real SQLite registry on a throwaway file (exercises persistence)."""
    return Registry(tmp_path / "registry.db")


@pytest.fixture
def fake_probe():
    """Clash probe that finds nothing by default; mutate .artifacts to simulate."""
    return FakeProbe()


@pytest.fixture
def client(fake_applier, registry, fake_probe):
    """TestClient with provisioner, registry and probe dependencies overridden."""
    app.dependency_overrides[get_provisioner] = lambda: Provisioner(
        MANIFESTS_DIR, "acr.example/pgcyan:test", fake_applier)
    app.dependency_overrides[get_registry] = lambda: registry
    app.dependency_overrides[get_source_probe] = lambda: fake_probe
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
