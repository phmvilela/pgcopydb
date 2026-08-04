"""ControllerClient tests -- stdlib urllib, no real network."""
import io
import urllib.error

import pytest

from app import controller_client as cc
from app.controller_client import ControllerClient, ControllerUnreachable


def test_base_url_targets_the_in_namespace_service():
    assert (cc.controller_base_url("pgcyan-mig-abc")
            == "http://pgcyan-controller.pgcyan-mig-abc.svc.cluster.local:9999")


class _Resp(io.BytesIO):
    def __init__(self, status, raw):
        super().__init__(raw)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def test_start_posts_and_parses(monkeypatch):
    seen = {}

    def fake_urlopen(req, timeout=None):
        seen["url"] = req.full_url
        seen["method"] = req.get_method()
        return _Resp(202, b'{"state": "starting", "message": "ok"}')

    monkeypatch.setattr(cc.urllib.request, "urlopen", fake_urlopen)
    code, body = ControllerClient().start("pgcyan-mig-x")
    assert code == 202 and body["state"] == "starting"
    assert seen["method"] == "POST"
    assert seen["url"].endswith("/start")


def test_http_error_surfaces_status(monkeypatch):
    def fake_urlopen(req, timeout=None):
        raise urllib.error.HTTPError(req.full_url, 404, "nf", {},
                                     io.BytesIO(b'{"error": "not found"}'))

    monkeypatch.setattr(cc.urllib.request, "urlopen", fake_urlopen)
    code, body = ControllerClient().status("ns")
    assert code == 404 and body["error"] == "not found"


def test_connection_failure_raises_unreachable(monkeypatch):
    def fake_urlopen(req, timeout=None):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(cc.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(ControllerUnreachable):
        ControllerClient().status("ns")
