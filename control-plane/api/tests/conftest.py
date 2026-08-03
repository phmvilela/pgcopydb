"""Shared fixtures for the control-plane API tests."""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app, get_provisioner
from app.provisioner import Provisioner

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


@pytest.fixture
def manifests_dir():
    return MANIFESTS_DIR


@pytest.fixture
def fake_applier():
    return FakeApplier()


@pytest.fixture
def client(fake_applier):
    """TestClient with the provisioner dependency overridden to use the fake."""
    app.dependency_overrides[get_provisioner] = lambda: Provisioner(
        MANIFESTS_DIR, "acr.example/pgcyan:test", fake_applier)
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
