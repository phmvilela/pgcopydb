"""
The capacity-input contract: the schemas the planner consumes, and their validator.

The capacity-aware planner (see ``pg_cyan/collect/capacity.py``, ``feasibility.py``,
``calibration.py``) reads a family of optional artifacts -- ``source-capacity``,
``target-capacity``, ``network``, ``cluster-capacity``, ``copylog-history`` -- to
size the concurrency budget, gate on the WAL-retention deadline, and calibrate the
cost model. Nothing produces them yet, so every real run falls back to the static
defaults. Those artifacts arrive by three routes: an operator-authored file (the
static provider, ``pg_cyan/collect/capinputs.py``), the control-plane API on
``POST /migrations``, and -- later -- automated collectors. This module is the one
place their shapes are defined and validated, so those routes cannot drift apart:
what the API accepts is exactly what the planner reads.

It is deliberately **dependency-free** (standard library only, no other ``pg_cyan``
imports) so a separate service -- the control-plane API image, which does not
install the migration package -- can vendor or import it and validate a request
body against the very same rules.

The field lists mirror the consumers exactly (all sizes bytes, all rates per
second, ``rtt_ms`` in milliseconds, utilizations a 0..1 fraction):

  network.json          bandwidth_bytes_per_sec, rtt_ms?, tcp_window_bytes?,
                        per_stream_bytes_per_sec?      (measured single-stream wins)
  source-capacity.json  read_throughput_bytes_per_sec, max_connections,
                        baseline_read_utilization?, current_connections?,
                        wal_disk_free_bytes?, wal_generation_bytes_per_sec?
  target-capacity.json  write_throughput_bytes_per_sec, max_connections,
                        baseline_write_utilization?, current_connections?
  cluster-capacity.json allocatable_cpu, node_count?, max_pods_per_node?,
                        cpu_per_stream?
  copylog-history.json  prior per-table durations (lenient: a list, or a dict with
                        observations/tables, or runs each carrying them)

Validation never raises; it returns a list of human-readable error strings (empty
means valid) so a caller can surface field-level problems. Unknown keys are
allowed on purpose -- the contract is forward-compatible, and a collector may add
detail a consumer does not read yet.

See also: pg_cyan/collect/capacity.py, pg_cyan/collect/feasibility.py, pg_cyan/collect/calibration.py
(the consumers), pg_cyan/collect/capinputs.py (the static provider).
"""
from typing import Dict, List, Optional

#: Bumped when a field is renamed, removed, or its meaning changes. Stamped into
#: every artifact the provider writes (``_contract_version``) for auditability;
#: consumers ignore it, so adding it never disturbs a plan.
CONTRACT_VERSION = 1

#: Bundle key (as accepted on the control-plane API body and in a declared file)
#: -> the canonical artifact filename the planner reads from its inputs dir.
BUNDLE_KEY_TO_FILENAME = {
    "source_capacity": "source-capacity.json",
    "target_capacity": "target-capacity.json",
    "network": "network.json",
    "cluster_capacity": "cluster-capacity.json",
    "copylog_history": "copylog-history.json",
    "constraints": "constraints.json",
}

#: The capacity artifacts this contract owns (constraints.json predates it and is
#: validated leniently as a pass-through clamp layer).
CAPACITY_FILENAMES = (
    "source-capacity.json",
    "target-capacity.json",
    "network.json",
    "cluster-capacity.json",
    "copylog-history.json",
)


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_number(errors: List[str], obj: Dict, field: str, *,
                  positive: bool = False, ratio: bool = False,
                  nonneg: bool = False) -> None:
    """Type/range-check ``field`` if present; append a message on violation."""
    if field not in obj or obj[field] is None:
        return
    value = obj[field]
    if not _is_number(value):
        errors.append(f"{field} must be a number, got {type(value).__name__}")
        return
    if positive and value <= 0:
        errors.append(f"{field} must be > 0, got {value}")
    if nonneg and value < 0:
        errors.append(f"{field} must be >= 0, got {value}")
    if ratio and not (0.0 <= value <= 1.0):
        errors.append(f"{field} must be a fraction in [0, 1], got {value}")


def _require_any(errors: List[str], obj: Dict, fields: List[str]) -> None:
    """At least one of ``fields`` must be present and a positive number."""
    if not any(_is_number(obj.get(f)) and obj[f] > 0 for f in fields):
        errors.append("at least one of " + ", ".join(fields) +
                      " must be present and > 0")


def _validate_network(obj: Dict) -> List[str]:
    errors: List[str] = []
    _require_any(errors, obj, ["bandwidth_bytes_per_sec", "per_stream_bytes_per_sec"])
    _check_number(errors, obj, "bandwidth_bytes_per_sec", positive=True)
    _check_number(errors, obj, "per_stream_bytes_per_sec", positive=True)
    _check_number(errors, obj, "rtt_ms", positive=True)
    _check_number(errors, obj, "tcp_window_bytes", positive=True)
    return errors


def _validate_source_capacity(obj: Dict) -> List[str]:
    errors: List[str] = []
    _require_any(errors, obj, ["read_throughput_bytes_per_sec", "max_connections",
                               "wal_disk_free_bytes"])
    _check_number(errors, obj, "read_throughput_bytes_per_sec", positive=True)
    _check_number(errors, obj, "max_connections", positive=True)
    _check_number(errors, obj, "current_connections", nonneg=True)
    _check_number(errors, obj, "baseline_read_utilization", ratio=True)
    _check_number(errors, obj, "wal_disk_free_bytes", positive=True)
    _check_number(errors, obj, "wal_generation_bytes_per_sec", positive=True)
    # WAL feasibility needs both halves; one alone silently does nothing.
    has_free = _is_number(obj.get("wal_disk_free_bytes"))
    has_rate = _is_number(obj.get("wal_generation_bytes_per_sec"))
    if has_free != has_rate:
        errors.append("wal_disk_free_bytes and wal_generation_bytes_per_sec must "
                      "be given together to enable the WAL-retention deadline")
    return errors


def _validate_target_capacity(obj: Dict) -> List[str]:
    errors: List[str] = []
    _require_any(errors, obj, ["write_throughput_bytes_per_sec", "max_connections"])
    _check_number(errors, obj, "write_throughput_bytes_per_sec", positive=True)
    _check_number(errors, obj, "max_connections", positive=True)
    _check_number(errors, obj, "current_connections", nonneg=True)
    _check_number(errors, obj, "baseline_write_utilization", ratio=True)
    return errors


def _validate_cluster_capacity(obj: Dict) -> List[str]:
    errors: List[str] = []
    _check_number(errors, obj, "allocatable_cpu", positive=True)
    if not _is_number(obj.get("allocatable_cpu")):
        errors.append("allocatable_cpu is required and must be > 0")
    _check_number(errors, obj, "node_count", positive=True)
    _check_number(errors, obj, "max_pods_per_node", positive=True)
    _check_number(errors, obj, "cpu_per_stream", positive=True)
    return errors


def _validate_history(obj) -> List[str]:
    """
    Lenient, matching ``pg_cyan/collect/calibration.py``: a list of rows, or a dict with a
    top-level ``observations``/``tables`` list, or ``runs`` each carrying one. Each
    usable row needs a positive ``duration_seconds`` (or ``seconds``) and some
    volume. An empty/shapeless artifact is not an error -- calibration simply keeps
    the default constants -- so this only rejects an outright wrong container type.
    """
    if isinstance(obj, list):
        return []
    if not isinstance(obj, dict):
        return ["copylog-history must be a list of rows or an object carrying them"]
    runs = obj.get("runs")
    if runs is not None and not isinstance(runs, list):
        return ["runs must be a list"]
    for key in ("observations", "tables"):
        if obj.get(key) is not None and not isinstance(obj[key], list):
            return [f"{key} must be a list"]
    return []


def _validate_constraints(obj: Dict) -> List[str]:
    """The pre-existing operator clamp layer: type-check the numeric caps only."""
    errors: List[str] = []
    for field in ("max_source_connections", "max_concurrency", "min_shards",
                  "max_shards", "max_table_jobs_per_shard", "max_copy_seconds",
                  "wal_disk_pct_pause", "max_lag_seconds"):
        _check_number(errors, obj, field, positive=True)
    return errors


_VALIDATORS = {
    "network.json": _validate_network,
    "source-capacity.json": _validate_source_capacity,
    "target-capacity.json": _validate_target_capacity,
    "cluster-capacity.json": _validate_cluster_capacity,
    "copylog-history.json": _validate_history,
    "constraints.json": _validate_constraints,
}


def validate(filename: str, obj) -> List[str]:
    """
    Validate one artifact against the contract.

    Args:
        filename: The canonical artifact filename (e.g. ``source-capacity.json``).
        obj: The parsed artifact.

    Returns:
        A list of human-readable error strings; empty means valid. An unknown
        filename yields a single "unknown artifact" error rather than raising.
    """
    validator = _VALIDATORS.get(filename)
    if validator is None:
        return [f"unknown artifact: {filename}"]
    if filename == "copylog-history.json":
        return validator(obj)
    if not isinstance(obj, dict):
        return [f"{filename} must be a JSON object"]
    return validator(obj)


def validate_bundle(bundle: Dict) -> Dict[str, List[str]]:
    """
    Validate a declared bundle keyed by bundle key (``source_capacity`` etc.).

    Returns a map of artifact filename -> error list for every present key,
    including ``{}`` (no errors) for the ones that validated. An unknown bundle
    key is reported under its own name. The control-plane API validates its
    request body with exactly this call.
    """
    result: Dict[str, List[str]] = {}
    if not isinstance(bundle, dict):
        return {"<bundle>": ["bundle must be a JSON object keyed by artifact"]}
    for key, value in bundle.items():
        filename = BUNDLE_KEY_TO_FILENAME.get(key)
        if filename is None:
            result[key] = [f"unknown bundle key: {key}"]
            continue
        result[filename] = validate(filename, value)
    return result


def stamp(obj: Dict) -> Dict:
    """Return ``obj`` with the contract version recorded (non-destructive copy)."""
    if not isinstance(obj, dict):
        return obj
    stamped = dict(obj)
    stamped.setdefault("_contract_version", CONTRACT_VERSION)
    return stamped
