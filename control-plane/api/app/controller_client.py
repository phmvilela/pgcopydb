"""
Talk to a migration's in-namespace controller control API (issue #89).

Each provisioned migration (#87) runs the folded controller (#85) with a control
API on port 9999, fronted by a **headless Service** ``pgcyan-controller`` in the
migration's namespace. The controller makes itself Ready while parked in
wait-to-start, so the Service routes to it before the migration begins.

From the control-plane pod we reach it by cross-namespace DNS:
``http://pgcyan-controller.<namespace>.svc.cluster.local:9999``. This is a thin,
injectable HTTP client (stdlib ``urllib`` -- no new runtime dep) so the API's
start/status logic stays unit-testable with a fake.
"""
import json
import urllib.error
import urllib.request
from typing import Dict, Tuple

#: The controller's headless Service name and control-API port (see
#: deploy/k8s/35-statefulset-controller.yaml).
CONTROLLER_SERVICE = "pgcyan-controller"
CONTROLLER_PORT = 9999


class ControllerUnreachable(Exception):
    """The controller could not be reached (DNS/connection/timeout)."""


def controller_base_url(namespace: str) -> str:
    """The in-cluster base URL of the controller in ``namespace``."""
    return (f"http://{CONTROLLER_SERVICE}.{namespace}.svc.cluster.local"
            f":{CONTROLLER_PORT}")


class ControllerClient:
    """HTTP client for a migration controller's ``POST /start`` / ``GET /status``."""

    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout

    def _request(self, namespace: str, method: str, path: str) -> Tuple[int, Dict]:
        url = controller_base_url(namespace) + path
        req = urllib.request.Request(url, method=method)
        if method == "POST":
            req.data = b""  # controller drains and ignores the body
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status, _parse(resp.read())
        except urllib.error.HTTPError as exc:
            # A response with a non-2xx status -- surface it (e.g. controller 404).
            return exc.code, _parse(exc.read())
        except (urllib.error.URLError, OSError) as exc:
            raise ControllerUnreachable(
                f"controller in {namespace} is unreachable: {exc}") from exc

    def start(self, namespace: str) -> Tuple[int, Dict]:
        """POST /start on the controller (idempotent server-side)."""
        return self._request(namespace, "POST", "/start")

    def status(self, namespace: str) -> Tuple[int, Dict]:
        """GET /status from the controller."""
        return self._request(namespace, "GET", "/status")


def _parse(raw: bytes) -> Dict:
    if not raw:
        return {}
    try:
        body = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}
    return body if isinstance(body, dict) else {"body": body}
