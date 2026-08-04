"""Skeleton control-plane API tests.

Run from ``control-plane/api``:  ``python -m pytest``
(These live outside the top-level ``tests/`` tree that the pg_cyan suite
collects, so they are not part of ``python -m pytest`` at the repo root.)
"""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health_returns_200_ok():
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "pgcyan-control-plane"


def test_health_reports_data_dir(monkeypatch, tmp_path):
    # A mounted, writable /data (here a tmp dir) is reported as writable.
    # Patch the resolved module attribute rather than reloading the module:
    # reloading swaps out app/get_* objects and pollutes other tests' overrides.
    import app.main as main

    monkeypatch.setattr(main, "DATA_DIR", str(tmp_path))
    body = TestClient(main.app).get("/health").json()
    assert body["data_dir"] == str(tmp_path)
    assert body["data_dir_writable"] is True
