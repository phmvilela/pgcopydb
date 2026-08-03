"""
In-cluster Kubernetes applier (issue #87).

Thin I/O layer over the official client so `Provisioner` can stay pure and
unit-testable (tests inject a fake with the same three methods). Imports of the
`kubernetes` package are local so the module loads without cluster config.
"""
from typing import Dict, Optional


class K8sApplier:
    """Applies objects to the cluster using the in-cluster ServiceAccount."""

    def __init__(self, api_client=None):
        from kubernetes import client, config

        if api_client is None:
            config.load_incluster_config()
            api_client = client.ApiClient()
        self._api_client = api_client
        self._core = client.CoreV1Api(api_client)

    def create_namespace(self, name: str, labels: Dict[str, str]) -> None:
        from kubernetes import client

        self._core.create_namespace(
            client.V1Namespace(metadata=client.V1ObjectMeta(name=name, labels=labels))
        )

    def create_secret(self, namespace: str, name: str, string_data: Dict[str, str],
                      labels: Optional[Dict[str, str]] = None) -> None:
        from kubernetes import client

        self._core.create_namespaced_secret(
            namespace,
            client.V1Secret(
                metadata=client.V1ObjectMeta(name=name, namespace=namespace, labels=labels or {}),
                string_data=string_data,
            ),
        )

    def apply_doc(self, doc: dict) -> None:
        """Create any namespaced object dict (dispatches by kind)."""
        from kubernetes import utils

        utils.create_from_dict(self._api_client, doc)
