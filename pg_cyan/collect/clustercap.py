"""
Cluster-capacity collector: what the Kubernetes cluster can actually run.

The capacity-aware planner reads ``cluster-capacity.json`` to bound the copy by
the *copy environment*: ``allocatable_cpu`` feeds the ``k8s_cpu`` term of C*
(``allocatable_cpu / cpu_per_stream``), and ``node_count`` x ``max_pods_per_node``
caps how many shard pods can actually be placed. Unlike the other collectors this
one talks to the **Kubernetes API**, not Postgres: it lists the cluster's nodes
and aggregates their allocatable capacity.

It counts only nodes that are **Ready and schedulable** (a cordoned or NotReady
node is not capacity), sums their allocatable CPU, and takes the max allocatable
pods per node. It **merges** into any existing ``cluster-capacity.json`` so a
declared ``cpu_per_stream`` (or other operator override) survives; a measured
value overrides a declared one.

Like the other collectors it is **optional**: no in-cluster config, missing
node-list RBAC, or an API error preserves the existing ``cluster-capacity.json``
(the fallback), writes an errors file, and still exits 0 -- it never breaks the
walk. (Reading nodes is cluster-scoped: the collect ServiceAccount needs a
``nodes: get,list`` ClusterRole -- see deploy/k8s/01-rbac.yaml.)

See also: pg_cyan/collect/capacity.py (consumes allocatable_cpu + the pod cap),
pg_cyan/collect/capacity_contract.py, pg_cyan/orchestrator.py (the k8s-client precedent).
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

from pg_cyan.collect import capacity_contract

logger = logging.getLogger(__name__)


def _parse_cpu(quantity) -> Optional[float]:
    """Parse a Kubernetes CPU quantity ('4', '3920m') into float cores."""
    if quantity is None:
        return None
    s = str(quantity).strip()
    try:
        if s.endswith("m"):
            return float(s[:-1]) / 1000.0
        return float(s)
    except ValueError:
        return None


class PgCyanClusterCapacity:
    """Aggregates node allocatable capacity into ``cluster-capacity.json``."""

    OUTPUT_FILENAME = "cluster-capacity.json"
    ERROR_FILENAME = "cluster-capacity-errors.json"

    def __init__(self, output_dir: str = "."):
        self.output_dir = output_dir
        self.errors: List[str] = []

    def _log_event(self, event_type, status, message, details=None) -> None:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "cluster-capacity",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }))

    def _load_nodes(self) -> Optional[List[Dict]]:
        """
        List the cluster's nodes as light dicts, or None if unreachable.

        Each dict is ``{allocatable, ready, schedulable}``. Any failure (no
        in-cluster config, missing RBAC, API error) is logged and returns None so
        the collector degrades rather than raising.
        """
        try:
            from kubernetes import client as k8s_client
            from kubernetes import config as k8s_config
            from kubernetes.client.rest import ApiException
        except ImportError as e:  # pragma: no cover - kubernetes is a pinned dep
            self.errors.append(f"kubernetes client unavailable: {e}")
            return None
        try:
            k8s_config.load_incluster_config()
            nodes = k8s_client.CoreV1Api().list_node().items
        except Exception as e:  # config load or ApiException (RBAC/connectivity)
            self._log_event("load_nodes", "warning", "Could not list cluster nodes",
                            {"error": str(e),
                             "remediation": "Run in-cluster with a nodes:get,list "
                                            "ClusterRole bound to the collect ServiceAccount"})
            self.errors.append(f"Node list failed: {e}")
            return None
        result = []
        for n in nodes:
            conditions = (n.status.conditions or []) if n.status else []
            ready = any(c.type == "Ready" and c.status == "True" for c in conditions)
            result.append({
                "allocatable": (n.status.allocatable or {}) if n.status else {},
                "ready": ready,
                "schedulable": not bool(n.spec.unschedulable) if n.spec else True,
            })
        return result

    def measure_capacity(self, nodes: List[Dict]) -> Optional[Dict]:
        """
        Aggregate Ready+schedulable nodes into the cluster-capacity fields.

        Returns ``{allocatable_cpu, node_count, max_pods_per_node}`` or None when
        no usable node contributes CPU.
        """
        usable = [n for n in nodes if n.get("ready") and n.get("schedulable")]
        total_cpu = 0.0
        max_pods = 0
        counted = 0
        for n in usable:
            alloc = n.get("allocatable") or {}
            cpu = _parse_cpu(alloc.get("cpu"))
            if cpu is None:
                continue
            total_cpu += cpu
            counted += 1
            try:
                pods = int(alloc.get("pods"))
                max_pods = max(max_pods, pods)
            except (TypeError, ValueError):
                pass
        if counted == 0 or total_cpu <= 0:
            return None
        cap = {"allocatable_cpu": round(total_cpu, 3), "node_count": counted}
        if max_pods > 0:
            cap["max_pods_per_node"] = max_pods
        return cap

    def _read_existing(self) -> Dict:
        path = os.path.join(self.output_dir, self.OUTPUT_FILENAME)
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _write_json(self, filename: str, payload: Dict) -> Optional[str]:
        path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            return path
        except OSError as e:
            self._log_event("write_output", "failure", f"Failed to write {filename}",
                            {"path": path, "error": str(e)})
            self.errors.append(f"Failed to write {filename}: {e}")
            return None

    def _degrade(self) -> Dict:
        error_path = self._write_json(self.ERROR_FILENAME, {
            "component": "cluster-capacity", "status": "failure",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "errors": self.errors or ["cluster-capacity probe produced nothing usable"],
            "remediation": "Any existing cluster-capacity.json stays in effect; the "
                           "planner degrades the k8s_cpu term and pod cap otherwise.",
        })
        return {"status": "degraded", "component": "cluster-capacity",
                "output_file": None, "error_file": error_path, "errors": self.errors}

    def collect(self) -> Dict:
        """List nodes, aggregate capacity, merge into cluster-capacity.json, or degrade."""
        nodes = self._load_nodes()
        if nodes is None:
            return self._degrade()

        cap = self.measure_capacity(nodes)
        if cap is None:
            self.errors.append("No Ready, schedulable node contributed allocatable CPU")
            return self._degrade()

        merged = dict(self._read_existing())
        merged.update(cap)
        merged["component"] = "cluster-capacity"
        merged["status"] = "success"
        merged["timestamp"] = datetime.utcnow().isoformat() + "Z"
        payload = capacity_contract.stamp(merged)

        contract_errors = capacity_contract.validate(self.OUTPUT_FILENAME, payload)
        if contract_errors:
            self.errors.extend(contract_errors)
            self._log_event("validate", "warning",
                            "Merged cluster-capacity failed contract validation",
                            {"errors": contract_errors})
            return self._degrade()

        output_path = self._write_json(self.OUTPUT_FILENAME, payload)
        if output_path is None:
            return self._degrade()
        self._log_event("collect", "success", "Wrote cluster-capacity from node allocatable",
                        {k: merged.get(k) for k in ("allocatable_cpu", "node_count",
                                                     "max_pods_per_node")})
        return {"status": "success", "component": "cluster-capacity",
                "output_file": output_path, "error_file": None, "errors": self.errors}


def collect_cluster_capacity_from_env() -> int:
    """
    Run the cluster-capacity collector from environment/CLI settings.

    Optional collector: returns 0 on success AND on graceful degradation (must not
    break the walk); 2 only on interrupt. Any existing cluster-capacity.json
    survives a degraded run.
    """
    try:
        collector = PgCyanClusterCapacity(output_dir=os.getenv("PG_OUTPUT_DIR", "."))
        summary = collector.collect()
        print(json.dumps(summary, indent=2, default=str))
        return 0
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Cluster-capacity collection interrupted by user",
        }))
        return 2


if __name__ == "__main__":
    sys.exit(collect_cluster_capacity_from_env())
