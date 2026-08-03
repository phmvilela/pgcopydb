"""POST /migrations/{id}/done teardown endpoint tests (issue #90)."""

_SRC = {"host": "src", "user": "su", "password": "sp", "database": "sdb"}
_TGT = {"uri": "postgres://tu:tp@tgt:5432/tdb?sslmode=require"}


def _provision(client):
    resp = client.post("/migrations", json={"source": dict(_SRC), "target": dict(_TGT)})
    assert resp.status_code == 201
    return resp.json()["id"]


def test_done_tears_down_and_frees_the_singleton(client, fake_teardown, registry):
    mid = _provision(client)
    registry.set_state(mid, "running", "2026-01-01T00:00:00+00:00")

    resp = client.post(f"/migrations/{mid}/done")
    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] == "done"
    assert body["graceful"] is True
    # the migration's namespace was torn down
    assert fake_teardown.torn == [f"pgcyan-mig-{mid}"]
    # registry marked done -> no longer active
    assert registry.get(mid)["state"] == "done"
    assert registry.active() == []
    # ...so a fresh migration is now accepted
    assert client.post("/migrations", json={"source": dict(_SRC),
                                            "target": dict(_TGT)}).status_code == 201


def test_done_works_from_waiting_to_start(client, fake_teardown, registry):
    mid = _provision(client)   # never started
    assert client.post(f"/migrations/{mid}/done").status_code == 200
    assert fake_teardown.torn == [f"pgcyan-mig-{mid}"]
    assert registry.get(mid)["state"] == "done"


def test_done_is_idempotent(client, fake_teardown, registry):
    mid = _provision(client)
    assert client.post(f"/migrations/{mid}/done").status_code == 200
    # second call: already done -> no cluster op, still 200
    resp = client.post(f"/migrations/{mid}/done")
    assert resp.status_code == 200
    assert resp.json()["message"] == "already torn down"
    assert fake_teardown.torn == [f"pgcyan-mig-{mid}"]   # not torn down twice


def test_done_reports_non_graceful_stop(client, fake_teardown):
    mid = _provision(client)
    fake_teardown.graceful = False
    body = client.post(f"/migrations/{mid}/done").json()
    assert body["state"] == "done"           # teardown still completed
    assert body["graceful"] is False


def test_done_unknown_migration_404(client):
    assert client.post("/migrations/nope/done").status_code == 404


def test_done_teardown_failure_502_leaves_state(client, fake_teardown, registry):
    mid = _provision(client)
    registry.set_state(mid, "running", "2026-01-01T00:00:00+00:00")
    fake_teardown.error = True
    resp = client.post(f"/migrations/{mid}/done")
    assert resp.status_code == 502
    # not marked done -- still active so the operator can retry
    assert registry.get(mid)["state"] == "running"
