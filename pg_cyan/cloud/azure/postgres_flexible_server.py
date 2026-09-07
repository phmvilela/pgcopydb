"""
Azure Database for PostgreSQL Flexible Server: storage/SKU + disk-used lookups.

Thin wrappers around the ``azure-mgmt-postgresqlflexibleservers`` and
``azure-mgmt-monitor`` SDKs -- the only place their client classes are
constructed, so ``pg_cyan/collect/sourcecap_azure.py`` (the collector that calls
these) stays testable without the real SDKs installed or reachable.

What Azure exposes and where:

* SKU tier, provisioned storage size, IOPS, and (for some tiers) throughput are
  resource properties on the server itself --
  ``PostgreSQLManagementClient.servers.get()`` -> ``Server.storage``.
* Storage **used** is not on that resource at all -- it is only published as an
  Azure Monitor metric (``storage_used``, bytes, on
  ``Microsoft.DBforPostgreSQL/flexibleServers``). There is no ``storage_free``
  metric, so free space is derived as ``storage_size_gb * 2**30 - storage_used``
  by the caller.

Every function here returns ``None`` on any failure (missing/malformed resource
id, auth, HTTP, or SDK error) -- it never raises. That is what lets the collector
degrade per-field instead of failing the whole run when, say, Monitor access is
granted but Reader is not (or vice versa).

See also: pg_cyan/collect/sourcecap_azure.py (the collector), pg_cyan/collect/sourcecap.py
(the Postgres-catalog half this pairs with).
"""
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

_RESOURCE_ID_RE = re.compile(
    r"^/subscriptions/(?P<subscription_id>[^/]+)"
    r"/resourceGroups/(?P<resource_group>[^/]+)"
    r"/providers/Microsoft\.DBforPostgreSQL/flexibleServers/(?P<server_name>[^/]+)/?$",
    re.IGNORECASE,
)


def parse_resource_id(resource_id: str) -> Tuple[str, str, str]:
    """
    Split a Flexible Server ARM resource id into (subscription_id, resource_group,
    server_name).

    Raises:
        ValueError: ``resource_id`` doesn't match the expected
            ``/subscriptions/.../resourceGroups/.../providers/Microsoft.DBforPostgreSQL/
            flexibleServers/...`` shape.
    """
    match = _RESOURCE_ID_RE.match((resource_id or "").strip())
    if not match:
        raise ValueError(f"Not a Flexible Server resource id: {resource_id!r}")
    return (match.group("subscription_id"), match.group("resource_group"),
            match.group("server_name"))


def resolve_credential():
    """A ``DefaultAzureCredential`` (Workload Identity in-cluster, no static creds)."""
    from azure.identity import DefaultAzureCredential
    return DefaultAzureCredential()


def get_storage_profile(resource_id: str, credential, timeout: int = 30) -> Optional[Dict]:
    """
    ``{"storage_size_gb": ..., "throughput_mibps": ...}`` for the server, or None.

    ``throughput_mibps`` is only present when the SDK reports it for that storage
    tier -- callers must not assume it is always there.
    """
    try:
        subscription_id, resource_group, server_name = parse_resource_id(resource_id)
    except ValueError as e:
        logger.warning("get_storage_profile: %s", e)
        return None
    try:
        from azure.mgmt.postgresqlflexibleservers import PostgreSQLManagementClient
        client = PostgreSQLManagementClient(credential, subscription_id,
                                             connection_timeout=timeout)
        server = client.servers.get(resource_group, server_name)
    except Exception as e:  # SDK/auth/HTTP error -- degrade, never raise
        logger.warning("get_storage_profile: server lookup failed for %s: %s",
                        resource_id, e)
        return None

    storage = getattr(server, "storage", None)
    storage_size_gb = getattr(storage, "storage_size_gb", None) if storage else None
    if storage_size_gb is None:
        return None
    profile = {"storage_size_gb": storage_size_gb}
    throughput = getattr(storage, "throughput", None) if storage else None
    if throughput:
        profile["throughput_mibps"] = throughput
    return profile


def get_storage_used_bytes(resource_id: str, credential, timeout: int = 30,
                            lookback_minutes: int = 15) -> Optional[float]:
    """The most recent ``storage_used`` Monitor data point (bytes), or None."""
    try:
        subscription_id, _, _ = parse_resource_id(resource_id)
    except ValueError as e:
        logger.warning("get_storage_used_bytes: %s", e)
        return None
    try:
        from azure.mgmt.monitor import MonitorManagementClient
        client = MonitorManagementClient(credential, subscription_id,
                                          connection_timeout=timeout)
        end = datetime.utcnow()
        start = end - timedelta(minutes=lookback_minutes)
        response = client.metrics.list(
            resource_uri=resource_id,
            metricnames="storage_used",
            timespan=f"{start.isoformat()}Z/{end.isoformat()}Z",
            interval="PT1M",
            aggregation="Average",
        )
    except Exception as e:  # SDK/auth/HTTP error -- degrade, never raise
        logger.warning("get_storage_used_bytes: metric query failed for %s: %s",
                        resource_id, e)
        return None

    for metric in response.value or []:
        for series in metric.timeseries or []:
            for point in reversed(series.data or []):
                if point.average is not None:
                    return float(point.average)
    return None
