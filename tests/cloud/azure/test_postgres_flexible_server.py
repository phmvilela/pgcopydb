"""
SDK-free tests for pg_cyan/cloud/azure/postgres_flexible_server.py.

The Azure SDK client construction/calls are monkeypatched at the module import
site so these never touch the network; they pin the resource-id parsing and the
graceful-None-on-failure contract every function here promises.
"""
import pytest

from pg_cyan.cloud.azure import postgres_flexible_server as azure_pg

VALID_ID = ("/subscriptions/11111111-1111-1111-1111-111111111111"
            "/resourceGroups/my-rg"
            "/providers/Microsoft.DBforPostgreSQL/flexibleServers/my-server")


def test_parse_resource_id_splits_the_components():
    subscription_id, resource_group, server_name = azure_pg.parse_resource_id(VALID_ID)
    assert subscription_id == "11111111-1111-1111-1111-111111111111"
    assert resource_group == "my-rg"
    assert server_name == "my-server"


@pytest.mark.parametrize("bad_id", [
    "",
    "not-a-resource-id",
    "/subscriptions/s/resourceGroups/rg/providers/Microsoft.DBforPostgreSQL/servers/name",
    "/subscriptions/s/providers/Microsoft.DBforPostgreSQL/flexibleServers/name",
])
def test_parse_resource_id_rejects_malformed_ids(bad_id):
    with pytest.raises(ValueError):
        azure_pg.parse_resource_id(bad_id)


def test_get_storage_profile_returns_none_for_a_bad_resource_id():
    assert azure_pg.get_storage_profile("not-a-resource-id", credential=object()) is None


def test_get_storage_profile_returns_none_when_the_sdk_raises(monkeypatch):
    class _BoomServers:
        def get(self, *a, **k):
            raise RuntimeError("boom")

    class _BoomClient:
        def __init__(self, *a, **k):
            self.servers = _BoomServers()

    monkeypatch.setattr(
        "azure.mgmt.postgresqlflexibleservers.PostgreSQLManagementClient", _BoomClient,
        raising=False)
    assert azure_pg.get_storage_profile(VALID_ID, credential=object()) is None


def test_get_storage_profile_extracts_size_and_throughput(monkeypatch):
    class _Storage:
        storage_size_gb = 256
        throughput = 500

    class _Server:
        storage = _Storage()

    class _Servers:
        def get(self, resource_group, server_name):
            assert resource_group == "my-rg" and server_name == "my-server"
            return _Server()

    class _Client:
        def __init__(self, *a, **k):
            self.servers = _Servers()

    monkeypatch.setattr(
        "azure.mgmt.postgresqlflexibleservers.PostgreSQLManagementClient", _Client,
        raising=False)
    profile = azure_pg.get_storage_profile(VALID_ID, credential=object())
    assert profile == {"storage_size_gb": 256, "throughput_mibps": 500}


def test_get_storage_profile_omits_throughput_when_the_sdk_does_not_report_it(monkeypatch):
    class _Storage:
        storage_size_gb = 256
        throughput = None

    class _Server:
        storage = _Storage()

    class _Servers:
        def get(self, resource_group, server_name):
            return _Server()

    class _Client:
        def __init__(self, *a, **k):
            self.servers = _Servers()

    monkeypatch.setattr(
        "azure.mgmt.postgresqlflexibleservers.PostgreSQLManagementClient", _Client,
        raising=False)
    profile = azure_pg.get_storage_profile(VALID_ID, credential=object())
    assert profile == {"storage_size_gb": 256}


def test_get_storage_used_bytes_returns_none_for_a_bad_resource_id():
    assert azure_pg.get_storage_used_bytes("nope", credential=object()) is None


def test_get_storage_used_bytes_returns_the_latest_average(monkeypatch):
    class _Point:
        def __init__(self, average):
            self.average = average

    class _Series:
        data = [_Point(None), _Point(111.0), _Point(222.0)]

    class _Metric:
        timeseries = [_Series()]

    class _Response:
        value = [_Metric()]

    class _Metrics:
        def list(self, **kwargs):
            assert kwargs["resource_uri"] == VALID_ID
            assert kwargs["metricnames"] == "storage_used"
            return _Response()

    class _Client:
        def __init__(self, *a, **k):
            self.metrics = _Metrics()

    monkeypatch.setattr("azure.mgmt.monitor.MonitorManagementClient", _Client, raising=False)
    assert azure_pg.get_storage_used_bytes(VALID_ID, credential=object()) == 222.0


def test_get_storage_used_bytes_returns_none_when_the_sdk_raises(monkeypatch):
    class _BoomMetrics:
        def list(self, **kwargs):
            raise RuntimeError("boom")

    class _Client:
        def __init__(self, *a, **k):
            self.metrics = _BoomMetrics()

    monkeypatch.setattr("azure.mgmt.monitor.MonitorManagementClient", _Client, raising=False)
    assert azure_pg.get_storage_used_bytes(VALID_ID, credential=object()) is None
