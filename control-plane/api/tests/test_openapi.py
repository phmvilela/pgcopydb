"""OpenAPI / Swagger enrichment tests."""

_MIGRATION_PATHS = [
    ("/migrations", "get"),
    ("/migrations", "post"),
    ("/migrations/{migration_id}/start", "post"),
    ("/migrations/{migration_id}/status", "get"),
    ("/migrations/{migration_id}/done", "post"),
]


def test_interactive_docs_are_served(client):
    assert client.get("/docs").status_code == 200          # Swagger UI
    assert client.get("/redoc").status_code == 200          # ReDoc
    assert client.get("/openapi.json").status_code == 200   # raw spec


def test_app_metadata_is_rich(client):
    info = client.get("/openapi.json").json()["info"]
    assert info["title"] == "pgcyan control-plane API"
    # the lifecycle + the real-cluster warning are described for explorers
    assert "pgcyan control plane" in info["description"]
    assert "Try it out" in info["description"]


def test_tags_are_declared(client):
    schema = client.get("/openapi.json").json()
    assert {t["name"] for t in schema["tags"]} >= {"migrations", "health"}


def test_every_migration_endpoint_is_tagged_and_summarised(client):
    paths = client.get("/openapi.json").json()["paths"]
    for path, method in _MIGRATION_PATHS:
        op = paths[path][method]
        assert op["tags"] == ["migrations"], (path, method)
        assert op.get("summary"), (path, method)


def test_provision_documents_its_error_responses(client):
    post = client.get("/openapi.json").json()["paths"]["/migrations"]["post"]
    assert {"201", "409", "422", "502", "503"} <= set(post["responses"])
    # the 409 carries an example body so explorers see what a clash/singleton looks like
    assert post["responses"]["409"]["content"]["application/json"]["example"]["detail"]


def test_request_body_has_an_example(client):
    schema = client.get("/openapi.json").json()
    mr = schema["components"]["schemas"]["MigrationRequest"]
    assert mr.get("examples") or mr.get("example")
