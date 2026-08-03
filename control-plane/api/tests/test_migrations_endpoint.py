"""POST /migrations endpoint tests (issue #87)."""


def test_post_migrations_provisions_and_returns_201(client, fake_applier):
    resp = client.post("/migrations", json={
        "source": {"host": "src", "user": "su", "password": "sp", "database": "sdb"},
        "target": {"uri": "postgres://tu:tp@tgt:5432/tdb?sslmode=require"},
        "shards": 4,
    })
    assert resp.status_code == 201
    body = resp.json()
    assert body["namespace"].startswith("pgcyan-mig-")
    assert body["state"] == "waiting-to-start"
    # the provisioner actually ran through the fake applier
    assert fake_applier.namespaces and fake_applier.secrets and fake_applier.docs


def test_post_migrations_rejects_incomplete_source(client):
    # source has neither uri nor host+user+database -> 422 validation error
    resp = client.post("/migrations", json={
        "source": {"host": "only-host"},
        "target": {"uri": "postgres://c/d"},
    })
    assert resp.status_code == 422
