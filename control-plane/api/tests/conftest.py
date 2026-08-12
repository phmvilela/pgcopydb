"""Shared fixtures for the control-plane API tests."""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.controller_client import ControllerUnreachable
from app.main import (
    app,
    get_controller_client,
    get_provisioner,
    get_registry,
    get_source_probe,
    get_teardown,
)
from app.provisioner import Provisioner
from app.registry import Registry
from app.teardown import TeardownError

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


class FakeController:
    """Stands in for a migration's controller control API -- no real pod.

    Configure ``start_response`` / ``status_response`` (``(code, body)`` tuples),
    or set ``unreachable=True`` to raise ``ControllerUnreachable``.
    """

    def __init__(self):
        self.start_response = (202, {"state": "starting", "message": "start accepted"})
        self.status_response = (200, {"state": "running", "steps": []})
        self.unreachable = False
        self.started = []
        self.polled = []

    def start(self, namespace):
        self.started.append(namespace)
        if self.unreachable:
            raise ControllerUnreachable(f"controller in {namespace} is unreachable")
        return self.start_response

    def status(self, namespace):
        self.polled.append(namespace)
        if self.unreachable:
            raise ControllerUnreachable(f"controller in {namespace} is unreachable")
        return self.status_response


class FakeTeardown:
    """Stands in for the cluster teardown -- records namespaces, no real cluster.

    Set ``graceful=False`` to simulate a controller that didn't stop in time,
    ``released=False`` to simulate an unreachable controller (source not released),
    or ``error=True`` to raise ``TeardownError``.
    """

    def __init__(self):
        self.torn = []
        self.graceful = True
        self.released = True
        self.error = False

    def teardown(self, namespace):
        self.torn.append(namespace)
        if self.error:
            raise TeardownError(f"teardown of {namespace} failed")
        return {"namespace": namespace, "graceful": self.graceful,
                "released": self.released}


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
def fake_controller():
    """Controller stand-in; tune .start_response/.status_response/.unreachable."""
    return FakeController()


@pytest.fixture
def fake_teardown():
    """Teardown stand-in; tune .graceful/.error."""
    return FakeTeardown()


@pytest.fixture
def client(fake_applier, registry, fake_probe, fake_controller, fake_teardown):
    """TestClient with all external-dependency hooks overridden by fakes."""
    app.dependency_overrides[get_provisioner] = lambda: Provisioner(
        MANIFESTS_DIR, "acr.example/pgcyan:test", fake_applier)
    app.dependency_overrides[get_registry] = lambda: registry
    app.dependency_overrides[get_source_probe] = lambda: fake_probe
    app.dependency_overrides[get_controller_client] = lambda: fake_controller
    app.dependency_overrides[get_teardown] = lambda: fake_teardown
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
