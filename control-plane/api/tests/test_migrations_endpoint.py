"""POST/GET /migrations endpoint tests."""

_SRC = {"host": "src", "user": "su", "password": "sp", "database": "sdb"}
_TGT = {"uri": "postgres://tu:tp@tgt:5432/tdb?sslmode=require"}


def _post(client, **over):
    body = {"source": dict(_SRC), "target": dict(_TGT)}
    body.update(over)
    return client.post("/migrations", json=body)


def test_post_migrations_provisions_and_records(client, fake_applier, registry):
    resp = _post(client, shards=4)
    assert resp.status_code == 201
    body = resp.json()
    assert body["namespace"].startswith("pgcyan-mig-")
    assert body["state"] == "waiting-to-start"
    # the provisioner actually ran through the fake applier
    assert fake_applier.namespaces and fake_applier.secrets and fake_applier.docs
    # ...and the migration was persisted to the registry (no credentials)
    rows = registry.list()
    assert len(rows) == 1
    assert rows[0]["id"] == body["id"]
    assert rows[0]["source"] == "src:5432/sdb"
    assert rows[0]["target"] == "tgt:5432/tdb"


def test_post_migrations_rejects_incomplete_source(client):
    resp = client.post("/migrations", json={
        "source": {"host": "only-host"},
        "target": {"uri": "postgres://c/d"},
    })
    assert resp.status_code == 422


def test_post_migrations_accepts_and_delivers_declared_capacity(client, fake_applier):
    resp = _post(client, capacity={
        "source_capacity": {"read_throughput_bytes_per_sec": 2e8, "max_connections": 500},
        "network": {"bandwidth_bytes_per_sec": 1e9, "rtt_ms": 12},
    })
    assert resp.status_code == 201
    assert sorted(resp.json()["capacity_declared"]) == ["network", "source_capacity"]
    # The declared bundle was stamped into the namespace as a ConfigMap.
    assert any(d.get("kind") == "ConfigMap"
               and d["metadata"]["name"] == "pgcyan-capacity-declared"
               for d in fake_applier.docs)


def test_post_migrations_rejects_invalid_capacity_without_claiming_slot(client, registry):
    # network with no throughput signal fails the shared contract.
    resp = _post(client, capacity={"network": {"rtt_ms": 12}})
    assert resp.status_code == 422
    assert "network.json" in resp.json()["detail"]["capacity"]
    # The singleton slot must be free -- a bad request never reserves it.
    assert registry.list() == []
    assert _post(client, shards=4).status_code == 201


def test_post_migrations_rejects_collector_owned_capacity_key(client):
    # copylog-history is machine-collected, not operator-declared -> not accepted.
    resp = _post(client, capacity={"copylog_history": {"observations": []}})
    assert resp.status_code == 422


def test_singleton_rejects_second_while_active(client, registry):
    assert _post(client).status_code == 201
    resp = _post(client)  # one already active
    assert resp.status_code == 409
    assert "already active" in resp.json()["detail"]


def test_singleton_allows_new_after_previous_terminal(client, registry):
    first = _post(client).json()
    registry.set_state(first["id"], "succeeded", "2026-01-01T00:00:00+00:00")
    assert _post(client).status_code == 201  # no longer active -> allowed


def test_clash_probe_blocks_when_source_carries_slot(client, fake_probe, registry):
    fake_probe.artifacts = ["replication slot 'pgcyan_slot'"]
    resp = _post(client)
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert "already carries" in detail and "src:5432/sdb" in detail
    # probed with the fully-formed source URI
    assert fake_probe.probed and fake_probe.probed[0].startswith("postgres://su:")
    # the reservation was rolled back -- a clash must not wedge the singleton slot
    assert registry.list() == []


def test_clash_rollback_leaves_slot_available(client, fake_probe):
    fake_probe.artifacts = ["publication 'pgcyan_pub'"]
    assert _post(client).status_code == 409
    fake_probe.artifacts = []          # source now clean
    assert _post(client).status_code == 201


def test_provision_failure_rolls_back_reservation(client, registry):
    from app.main import app, get_provisioner

    class Boom:
        def provision(self, req, migration_id=None):
            raise RuntimeError("kaboom")

    app.dependency_overrides[get_provisioner] = lambda: Boom()
    resp = _post(client)   # client fixture clears overrides at teardown
    assert resp.status_code == 502
    assert registry.list() == []   # slot released for the next request


def test_get_migrations_lists_records(client):
    created = _post(client).json()
    resp = client.get("/migrations")
    assert resp.status_code == 200
    listed = resp.json()
    assert [r["id"] for r in listed] == [created["id"]]
    assert listed[0]["state"] == "waiting-to-start"
    # response model carries no credential fields
    assert set(listed[0]) == {
        "id", "namespace", "source", "target", "state", "created_at", "updated_at"}
