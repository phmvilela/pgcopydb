"""Provisioning-logic tests, exercised against the real manifests."""
import yaml
import pytest

from app.models import CapacityInputs, DbConn, MigrationRequest
from app.provisioner import (
    CAPACITY_CONFIGMAP,
    CAPACITY_DECLARED_KEY,
    Provisioner,
    ProvisionError,
    render_placeholders,
    retarget_namespace,
)


def _req(shards=None, capacity=None):
    return MigrationRequest(
        source=DbConn(host="src", user="su", password="sp", database="sdb"),
        target=DbConn(host="tgt", user="tu", password="tp", database="tdb"),
        shards=shards,
        capacity=capacity,
    )


def test_render_placeholders_only_fills_whitelisted_keys():
    assert render_placeholders("a=${X} b=${Y}", {"X": "1"}) == "a=1 b=${Y}"


def test_retarget_namespace_rewrites_metadata_and_subjects_only():
    doc = {
        "metadata": {"namespace": "pgcyan-migration"},
        "subjects": [{"kind": "ServiceAccount", "namespace": "pgcyan-migration"}],
        "other": {"namespace": "somewhere-else"},
    }
    retarget_namespace(doc, "pgcyan-mig-abc")
    assert doc["metadata"]["namespace"] == "pgcyan-mig-abc"
    assert doc["subjects"][0]["namespace"] == "pgcyan-mig-abc"
    assert doc["other"]["namespace"] == "somewhere-else"  # untouched


def test_provision_requires_a_migration_image(manifests_dir, fake_applier):
    p = Provisioner(manifests_dir, None, fake_applier)
    with pytest.raises(ProvisionError):
        p.provision(_req())


def test_provision_stands_up_a_parked_namespace(manifests_dir, fake_applier):
    p = Provisioner(manifests_dir, "acr/pgcyan:abc123", fake_applier, follow_cdc=False)
    res = p.provision(_req(shards=6), migration_id="deadbeef01")

    ns = "pgcyan-mig-deadbeef01"
    assert res == {"id": "deadbeef01", "namespace": ns, "capacity_declared": []}

    # Namespace created first, carrying the migration-id label.
    assert fake_applier.namespaces[0][0] == ns
    assert fake_applier.namespaces[0][1]["pgcyan.dev/migration-id"] == "deadbeef01"

    # DB secret with both URIs, in the new namespace.
    sns, sname, sdata = fake_applier.secrets[0]
    assert (sns, sname) == (ns, "pgcyan-db-uris")
    assert sdata["PGCOPYDB_SOURCE_PGURI"] == "postgres://su:sp@src:5432/sdb?sslmode=require"
    assert sdata["PGCOPYDB_TARGET_PGURI"] == "postgres://tu:tp@tgt:5432/tdb?sslmode=require"

    # The full scaffolding was applied.
    kinds = [d.get("kind") for d in fake_applier.docs]
    for k in ("ServiceAccount", "Role", "RoleBinding", "PersistentVolumeClaim",
              "Service", "StatefulSet", "PodDisruptionBudget"):
        assert k in kinds, f"missing {k}"

    # Every retargeted doc dropped the fixed namespace token. The one deliberate
    # exception is the external-observer binding (checked on its own below), whose
    # subject must keep pointing at the fixed observer namespace.
    retargeted = [d for d in fake_applier.docs
                  if d.get("metadata", {}).get("name") != "pgcyan-viewer-external"]
    dumped = yaml.safe_dump_all(retargeted)
    assert "pgcyan-migration" not in dumped
    assert ns in dumped

    # Controller is parked pre-snapshot, on the right image, with shards captured;
    # no unfilled placeholders remain.
    sts = next(d for d in fake_applier.docs if d["kind"] == "StatefulSet")
    container = sts["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "acr/pgcyan:abc123"
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["PGCYAN_WAIT_FOR_START"] == "true"
    assert env["PGCYAN_STOP_AFTER"] == ""
    assert env["PGCYAN_FOLLOW_CDC"] == ""
    assert env["PGCYAN_SHARDS"] == "6"
    assert "${" not in yaml.safe_dump(sts)


def test_provision_delivers_declared_capacity_configmap(manifests_dir, fake_applier):
    import json

    cap = CapacityInputs(
        source_capacity={"read_throughput_bytes_per_sec": 2e8, "max_connections": 500},
        network={"bandwidth_bytes_per_sec": 1e9, "rtt_ms": 12},
    )
    p = Provisioner(manifests_dir, "acr/pgcyan:abc123", fake_applier)
    res = p.provision(_req(capacity=cap), migration_id="deadbeef01")

    assert res["capacity_declared"] == ["network", "source_capacity"]
    cm = next(d for d in fake_applier.docs
              if d.get("kind") == "ConfigMap"
              and d["metadata"]["name"] == CAPACITY_CONFIGMAP)
    assert cm["metadata"]["namespace"] == "pgcyan-mig-deadbeef01"
    # The declared bundle is the exact shape `capacity-inputs` reads.
    bundle = json.loads(cm["data"][CAPACITY_DECLARED_KEY])
    assert bundle["network"]["bandwidth_bytes_per_sec"] == 1e9
    assert set(bundle) == {"network", "source_capacity"}


def test_provision_without_capacity_creates_no_capacity_configmap(manifests_dir, fake_applier):
    p = Provisioner(manifests_dir, "acr/pgcyan:abc123", fake_applier)
    res = p.provision(_req(), migration_id="deadbeef01")
    assert res["capacity_declared"] == []
    names = [d.get("metadata", {}).get("name") for d in fake_applier.docs]
    assert CAPACITY_CONFIGMAP not in names


def test_provision_binds_external_viewer_read_only(manifests_dir, fake_applier):
    p = Provisioner(manifests_dir, "img:1", fake_applier)
    ns = "pgcyan-mig-deadbeef01"
    p.provision(_req(), migration_id="deadbeef01")

    vb = next(d for d in fake_applier.docs
              if d.get("kind") == "RoleBinding"
              and d["metadata"]["name"] == "pgcyan-viewer-external")
    # Bound INTO the dynamic namespace, reusing that namespace's read-only Role...
    assert vb["metadata"]["namespace"] == ns
    assert vb["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "pgcyan-viewer"}
    # ...but the subject stays the FIXED out-of-cluster observer (not retargeted).
    assert vb["subjects"] == [{
        "kind": "ServiceAccount", "name": "pgcyan-viewer", "namespace": "pgcyan-migration"}]


def test_provision_external_viewer_is_disable_able(manifests_dir, fake_applier):
    p = Provisioner(manifests_dir, "img:1", fake_applier, viewer_sa=None)
    p.provision(_req(), migration_id="deadbeef01")
    names = [d.get("metadata", {}).get("name") for d in fake_applier.docs]
    assert "pgcyan-viewer-external" not in names


def test_provision_follow_cdc_flag_sets_controller_env(manifests_dir, fake_applier):
    p = Provisioner(manifests_dir, "img:1", fake_applier, follow_cdc=True)
    p.provision(_req(), migration_id="c0ffee")
    sts = next(d for d in fake_applier.docs if d["kind"] == "StatefulSet")
    env = {e["name"]: e.get("value") for e in sts["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["PGCYAN_FOLLOW_CDC"] == "true"
    assert "PGCYAN_SHARDS" not in env   # omitted when not requested
