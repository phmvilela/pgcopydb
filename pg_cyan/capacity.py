"""
Capacity model for the migration planner: the concurrency budget and shard shape.

The planner used to run a hardcoded 4 shards regardless of what the source, the
target, the network, or the Kubernetes cluster could actually sustain. This
module turns the collected capacity artifacts into a single **concurrency budget**
``C*`` -- the number of parallel COPY streams the copy can run without overloading
any participant -- and from it derives how many shards to run and how many
``table_jobs`` each shard gets.

    C* = min( source connection headroom,
              source read ceiling / per-stream throughput,
              streams to fill the pipe (bandwidth, RTT),
              target connection headroom,
              target write ceiling / per-stream throughput,
              allocatable k8s CPU / CPU per stream )

Only the smallest term ("binding term") actually limits the copy; naming it tells
the operator what to change to go faster. Every term is optional: a term is only
included in the ``min`` when its inputs are present, so partial capacity data
still yields a budget from what is known. With no capacity data at all the budget
degrades to the planner's static defaults.

It is a pure, DB-free transform. This module also fixes the **input contract**
the (out-of-scope) collectors must fill; all sizes are bytes, all rates bytes or
ops per second, all times as noted:

  network.json          {bandwidth_bytes_per_sec, rtt_ms, tcp_window_bytes?,
                         per_stream_bytes_per_sec?}  # measured single-stream wins
  source-capacity.json  {read_throughput_bytes_per_sec, baseline_read_utilization?,
                         max_connections, current_connections?}
  target-capacity.json  {write_throughput_bytes_per_sec, baseline_write_utilization?,
                         max_connections, current_connections?}
  cluster-capacity.json {allocatable_cpu, node_count?, max_pods_per_node?,
                         cpu_per_stream?}
  constraints.json      {max_source_connections?, max_concurrency?, min_shards?,
                         max_shards?, max_table_jobs_per_shard?}  # clamp only

``constraints.json`` is the operator's clamp layer: it can only *tighten* the
physics (lower a ceiling, cap the shard count), never loosen it.

See also: pg_cyan/cost_model.py (per-stream throughput feeds the copy-time weight),
          pg_cyan/planner.py (consumes the budget to shape the plan).
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

#: Assumed TCP window for a single stream when the network probe omits it. A
#: single stream can hold at most one window in flight per round trip, so on a
#: high-RTT link this is what makes many streams necessary to fill the pipe.
DEFAULT_TCP_WINDOW_BYTES = 4 * 1024 * 1024
#: vCPU one COPY stream consumes, used to turn allocatable CPU into a stream count.
DEFAULT_CPU_PER_STREAM = 0.25
#: Realistic throughput one COPY stream achieves end to end -- bounded by
#: per-connection source read, row decode, and target write, not just the raw
#: network window. Used as the fallback and as an upper cap on the network-derived
#: per-stream rate, so a fat low-latency link does not imply a single giant stream.
#: Matches the cost model's default. See also: pg_cyan/cost_model.py.
DEFAULT_PER_STREAM_BYTES_PER_SEC = 50 * 1024 * 1024


def _num(value, default: Optional[float] = None) -> Optional[float]:
    """Coerce to float; return ``default`` for None/missing/non-numeric."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _pos(value) -> Optional[float]:
    """A strictly-positive float, or None (so it drops out of a ``min``)."""
    n = _num(value)
    return n if (n is not None and n > 0) else None


@dataclass
class Budget:
    """
    The computed concurrency budget and the shard shape derived from it.

    Attributes:
        c_star: Total parallel COPY streams to run, or ``None`` when no capacity
            input was available (the caller then keeps its static defaults).
        binding_term: Name of the term that set ``c_star`` (e.g. ``target_write``);
            ``"none"`` when degraded.
        per_stream_bytes_per_sec: Effective single-stream throughput, fed to the
            cost model so copy-time estimates match the real link.
        shard_count: Number of copy shards to produce.
        table_jobs: pgcopydb ``--table-jobs`` per shard (``shard_count * table_jobs``
            approximates ``c_star``).
        max_table_jobs: Per-shard job ceiling -- the most COPY workers one shard
            may run, so the most parallelism a single table can get from
            within-shard splitting. See also: pg_cyan/splitting.py.
        terms: Every computable ceiling term, for explainability.
        clamps: Human-readable constraints that tightened the result.
        degraded: True when there was no capacity data and defaults were kept.
    """

    c_star: Optional[int]
    binding_term: str
    per_stream_bytes_per_sec: float
    shard_count: int
    table_jobs: int
    max_table_jobs: int
    terms: Dict[str, float] = field(default_factory=dict)
    clamps: List[str] = field(default_factory=list)
    degraded: bool = False

    def as_plan_block(self) -> Dict:
        """The ``concurrency`` block embedded in ``plan.json``."""
        return {
            "c_star": self.c_star,
            "binding_term": self.binding_term,
            "per_stream_bytes_per_sec": round(self.per_stream_bytes_per_sec, 2),
            "shard_count": self.shard_count,
            "table_jobs_per_shard": self.table_jobs,
            "terms": {k: round(v, 4) for k, v in self.terms.items()},
            "clamps": self.clamps,
            "degraded": self.degraded,
            "note": (
                "C* = min(terms), the parallel COPY stream budget; the binding "
                "term is the smallest. shard_count x table_jobs_per_shard ~= C*. "
                "constraints.json clamps tighten only. Absent inputs drop their "
                "term; no capacity at all keeps the static defaults."
            ),
        }


def _per_stream_throughput(network: Optional[Dict]) -> float:
    """
    Effective throughput of one COPY stream, in bytes/sec.

    A measured ``per_stream_bytes_per_sec`` from the probe wins outright. Failing
    that, a single stream is bounded by the link bandwidth, by how much it can
    keep in flight per round trip (``tcp_window / rtt``), and by the realistic
    per-connection ceiling -- so a fat low-latency link does not translate into
    one implausibly fast stream. With no network probe the ceiling is assumed.
    """
    if not network:
        return DEFAULT_PER_STREAM_BYTES_PER_SEC
    measured = _pos(network.get("per_stream_bytes_per_sec"))
    if measured is not None:
        return measured
    bandwidth = _pos(network.get("bandwidth_bytes_per_sec"))
    if bandwidth is None:
        return DEFAULT_PER_STREAM_BYTES_PER_SEC
    net_single = bandwidth
    rtt_ms = _pos(network.get("rtt_ms"))
    if rtt_ms is not None:
        window = _pos(network.get("tcp_window_bytes")) or DEFAULT_TCP_WINDOW_BYTES
        net_single = min(bandwidth, window / (rtt_ms / 1000.0))
    return min(net_single, DEFAULT_PER_STREAM_BYTES_PER_SEC)


def _available(ceiling: Optional[float], baseline_util: Optional[float]) -> Optional[float]:
    """Ceiling minus the fraction already in use (baseline utilization)."""
    if ceiling is None:
        return None
    util = _num(baseline_util, 0.0) or 0.0
    util = min(max(util, 0.0), 1.0)
    return ceiling * (1.0 - util)


def _conn_headroom(cap: Optional[Dict]) -> Optional[float]:
    """Free connections = max_connections - current_connections."""
    if not cap:
        return None
    max_conns = _pos(cap.get("max_connections"))
    if max_conns is None:
        return None
    current = _num(cap.get("current_connections"), 0.0) or 0.0
    return max(max_conns - current, 0.0)


def compute_budget(
    *,
    constraints: Optional[Dict],
    source_capacity: Optional[Dict],
    target_capacity: Optional[Dict],
    network: Optional[Dict],
    cluster_capacity: Optional[Dict],
    table_count: int,
    fallback_shards: int,
    fallback_table_jobs: int,
) -> Budget:
    """
    Compute ``C*`` and the shard shape from the capacity artifacts.

    Deterministic and never raises: malformed or missing fields drop their term.
    When no term can be computed the budget is marked ``degraded`` and the
    ``fallback_*`` values (the planner's static defaults) are returned unchanged,
    still clamped to the table count.
    """
    constraints = constraints or {}
    per_stream = _per_stream_throughput(network)

    terms: Dict[str, float] = {}

    # Source-side ceilings.
    src_conns = _conn_headroom(source_capacity)
    # constraints.max_source_connections can only tighten the connection budget.
    max_src_conns = _pos(constraints.get("max_source_connections"))
    if src_conns is not None and max_src_conns is not None:
        src_conns = min(src_conns, max_src_conns)
    elif src_conns is None and max_src_conns is not None:
        src_conns = max_src_conns
    if src_conns is not None:
        terms["source_connections"] = src_conns

    if source_capacity:
        src_read = _available(
            _pos(source_capacity.get("read_throughput_bytes_per_sec")),
            source_capacity.get("baseline_read_utilization"),
        )
        if src_read is not None:
            terms["source_read"] = src_read / per_stream

    # Network: how many single streams it takes to fill the pipe.
    if network:
        bandwidth = _pos(network.get("bandwidth_bytes_per_sec"))
        if bandwidth is not None:
            terms["network_fill"] = bandwidth / per_stream

    # Target-side ceilings.
    tgt_conns = _conn_headroom(target_capacity)
    if tgt_conns is not None:
        terms["target_connections"] = tgt_conns
    if target_capacity:
        tgt_write = _available(
            _pos(target_capacity.get("write_throughput_bytes_per_sec")),
            target_capacity.get("baseline_write_utilization"),
        )
        if tgt_write is not None:
            terms["target_write"] = tgt_write / per_stream

    # Kubernetes CPU headroom.
    if cluster_capacity:
        cpu = _pos(cluster_capacity.get("allocatable_cpu"))
        cpu_per_stream = _pos(cluster_capacity.get("cpu_per_stream")) or DEFAULT_CPU_PER_STREAM
        if cpu is not None:
            terms["k8s_cpu"] = cpu / cpu_per_stream

    clamps: List[str] = []

    # No capacity signal at all -> keep the static plan.
    if not terms:
        shard_count = max(1, min(fallback_shards, table_count))
        return Budget(
            c_star=None,
            binding_term="none",
            per_stream_bytes_per_sec=per_stream,
            shard_count=shard_count,
            table_jobs=fallback_table_jobs,
            max_table_jobs=fallback_table_jobs,
            terms={},
            clamps=[],
            degraded=True,
        )

    binding_term = min(terms, key=terms.get)
    c_star = terms[binding_term]

    # constraints.max_concurrency clamps the whole budget (tighten only).
    max_conc = _pos(constraints.get("max_concurrency"))
    if max_conc is not None and max_conc < c_star:
        c_star = max_conc
        binding_term = "constraints_max_concurrency"
        clamps.append(f"max_concurrency={int(max_conc)}")

    c_star = max(1, int(c_star))
    shard_count, table_jobs, max_table_jobs = _shape_shards(
        c_star, constraints, cluster_capacity, table_count, fallback_table_jobs, clamps,
    )

    return Budget(
        c_star=c_star,
        binding_term=binding_term,
        per_stream_bytes_per_sec=per_stream,
        shard_count=shard_count,
        table_jobs=table_jobs,
        max_table_jobs=max_table_jobs,
        terms=terms,
        clamps=clamps,
        degraded=False,
    )


def _shape_shards(
    c_star: int,
    constraints: Dict,
    cluster_capacity: Optional[Dict],
    table_count: int,
    fallback_table_jobs: int,
    clamps: List[str],
) -> tuple:
    """
    Split ``C*`` streams into ``shard_count`` pods of ``table_jobs`` each.

    Uses as few shards as the per-pod job ceiling allows, then clamps the count
    by the operator's min/max, the table count, and the cluster's pod capacity.
    ``table_jobs`` is refilled from the final shard count so the product still
    approximates ``C*``.
    """
    max_tj = int(_pos(constraints.get("max_table_jobs_per_shard")) or fallback_table_jobs)
    max_tj = max(1, max_tj)

    shard_count = max(1, -(-c_star // max_tj))  # ceil(c_star / max_tj)

    min_shards = _pos(constraints.get("min_shards"))
    max_shards = _pos(constraints.get("max_shards"))
    if min_shards is not None and shard_count < min_shards:
        shard_count = int(min_shards)
        clamps.append(f"min_shards={int(min_shards)}")
    if max_shards is not None and shard_count > max_shards:
        shard_count = int(max_shards)
        clamps.append(f"max_shards={int(max_shards)}")

    # Never more shards than tables (empty shards are pointless) ...
    if shard_count > table_count and table_count > 0:
        shard_count = table_count
        clamps.append(f"table_count={table_count}")

    # ... nor more pods than the cluster can place.
    if cluster_capacity:
        nodes = _pos(cluster_capacity.get("node_count"))
        pods_per_node = _pos(cluster_capacity.get("max_pods_per_node"))
        if nodes is not None and pods_per_node is not None:
            pod_cap = int(nodes * pods_per_node)
            if pod_cap >= 1 and shard_count > pod_cap:
                shard_count = pod_cap
                clamps.append(f"cluster_pod_capacity={pod_cap}")

    shard_count = max(1, shard_count)
    # Refill jobs per shard from the final count, capped at the per-pod ceiling.
    table_jobs = min(max_tj, max(1, -(-c_star // shard_count)))  # ceil(c_star / shard_count)
    return shard_count, table_jobs, max_tj
