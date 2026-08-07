"""start + status proxy endpoint tests."""

_SRC = {"host": "src", "user": "su", "password": "sp", "database": "sdb"}
_TGT = {"uri": "postgres://tu:tp@tgt:5432/tdb?sslmode=require"}


def _provision(client):
    resp = client.post("/migrations", json={"source": dict(_SRC), "target": dict(_TGT)})
    assert resp.status_code == 201
    return resp.json()["id"]


# --------------------------------------------------------------- start

def test_start_proxies_to_controller_and_marks_running(client, fake_controller, registry):
    mid = _provision(client)
    resp = client.post(f"/migrations/{mid}/start")
    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] == "running"
    assert body["message"] == "start accepted"
    # the controller in the migration's namespace was called
    assert fake_controller.started == [f"pgcyan-mig-{mid}"]
    # registry advanced -> still active (one running migration)
    assert registry.get(mid)["state"] == "running"


def test_start_unknown_migration_404(client):
    assert client.post("/migrations/nope/start").status_code == 404


def test_start_rejects_finished_migration_409(client, registry):
    mid = _provision(client)
    registry.set_state(mid, "succeeded", "2026-01-01T00:00:00+00:00")
    resp = client.post(f"/migrations/{mid}/start")
    assert resp.status_code == 409
    assert "already finished" in resp.json()["detail"]


def test_start_controller_unreachable_503(client, fake_controller):
    mid = _provision(client)
    fake_controller.unreachable = True
    assert client.post(f"/migrations/{mid}/start").status_code == 503


def test_start_controller_error_502(client, fake_controller):
    mid = _provision(client)
    fake_controller.start_response = (500, {"error": "boom"})
    assert client.post(f"/migrations/{mid}/start").status_code == 502


# --------------------------------------------------------------- status

def test_status_aggregates_controller_and_registry(client, fake_controller):
    mid = _provision(client)
    fake_controller.status_response = (200, {"state": "running", "snapshot": "42/AB",
                                             "steps": [{"id": "1", "status": "done"}]})
    resp = client.get(f"/migrations/{mid}/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["controller_reachable"] is True
    assert body["state"] == "running"
    assert body["controller"]["snapshot"] == "42/AB"
    assert fake_controller.polled == [f"pgcyan-mig-{mid}"]


def test_status_terminal_writes_back_and_frees_slot(client, fake_controller, registry):
    mid = _provision(client)
    fake_controller.status_response = (200, {"state": "succeeded", "steps": []})
    body = client.get(f"/migrations/{mid}/status").json()
    assert body["state"] == "succeeded"
    # registry converged to terminal -> the singleton slot is now free
    assert registry.get(mid)["state"] == "succeeded"
    assert registry.active() == []
    # a fresh migration can now be provisioned
    assert client.post("/migrations", json={"source": dict(_SRC),
                                            "target": dict(_TGT)}).status_code == 201


def test_status_unreachable_falls_back_to_registry(client, fake_controller):
    mid = _provision(client)
    fake_controller.unreachable = True
    body = client.get(f"/migrations/{mid}/status").json()
    assert body["controller_reachable"] is False
    assert body["controller"] is None
    assert body["state"] == "waiting-to-start"   # registry view preserved


def test_status_unknown_migration_404(client):
    assert client.get("/migrations/nope/status").status_code == 404
