"""
Time-based cost model for the migration planner.

Heap bytes are a weak proxy for how long a table takes to COPY: a bloated table's
heap is mostly dead tuples COPY never ships, a narrow high-row table is bound by
per-row overhead rather than bytes, and a TOAST-heavy table costs more per byte
to read (detoasting). This module turns a table's size/shape into an estimated
**single-stream copy time in seconds** so the planner can balance shards by time
instead of bytes.

It is a pure, DB-free transform, kept separate from the shard-assignment logic so
it can be unit-tested on its own. Every field it reads is optional: when the
extended ``source-pg-metrics.json`` fields are absent the cost collapses to
``heap_bytes / per_stream_bytes_per_sec`` -- a constant rescaling of raw heap
bytes, so the packer's assignment is unchanged.

Formula (per table, one COPY stream):

    live_bytes ~= heap_bytes * (1 - dead_tuple_ratio)      # COPY ships live tuples only
    t(table)   ~= max( live_bytes / per_stream_bytes_per_sec,
                       row_estimate / per_stream_rows_per_sec )

The two throughput constants are meant to be calibrated against real copy
durations. The TOAST portion of a table can be surcharged via
``toast_overhead_factor`` to reflect detoast cost; it defaults to ``1.0``
(neutral) so no magic constant changes behaviour until it is calibrated.

See also: pg_cyan/plan/planner.py (consumes this to weight the bin-packer).
"""
from dataclasses import dataclass
from typing import Dict


def _num(value, default: float = 0.0) -> float:
    """Coerce ``value`` to float, treating ``None``/non-numeric as ``default``."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp_ratio(value: float) -> float:
    """Clamp a fraction to ``[0.0, 1.0]`` (dead-tuple ratios can arrive noisy)."""
    if value <= 0.0:
        return 0.0
    if value >= 1.0:
        return 1.0
    return value


@dataclass(frozen=True)
class CostModel:
    """
    Calibratable coefficients that estimate a table's single-stream copy time.

    Attributes:
        per_stream_bytes_per_sec: Effective throughput of ONE COPY stream, in
            bytes/sec -- ``min(network, source-read, target-write)`` per stream.
            The first constant worth calibrating against real copy durations.
        per_stream_rows_per_sec: Effective rows/sec for one stream. Bounds narrow,
            row-heavy tables whose cost is per-row overhead rather than bytes.
        toast_overhead_factor: Multiplier applied to the TOAST portion of a
            table to reflect detoast read cost. ``1.0`` = neutral (default);
            values > 1 make TOAST-heavy tables cost proportionally more.
    """

    #: 50 MiB/s -- a conservative single cross-region COPY stream.
    per_stream_bytes_per_sec: float = 50 * 1024 * 1024
    per_stream_rows_per_sec: float = 200_000.0
    toast_overhead_factor: float = 1.0

    def live_bytes(self, table: Dict) -> float:
        """
        Bytes COPY actually ships for ``table``.

        ``heap_bytes * (1 - dead_tuple_ratio)``, with the (live fraction of the)
        TOAST portion surcharged by ``toast_overhead_factor``. ``heap_bytes``
        already includes TOAST (``pg_table_size``), so the surcharge only adds
        the *extra* cost above 1x. With no extended fields present this is just
        ``heap_bytes`` -- today's weight.
        """
        heap = _num(table.get("heap_bytes"))
        if heap <= 0.0:
            return 0.0
        dead = _clamp_ratio(_num(table.get("dead_tuple_ratio")))
        live = heap * (1.0 - dead)
        # toast_bytes is a subset of heap_bytes; clamp so a noisy value can't
        # push the surcharge past the table itself.
        toast = min(_num(table.get("toast_bytes")), heap)
        toast_surcharge = toast * (1.0 - dead) * (self.toast_overhead_factor - 1.0)
        return live + toast_surcharge

    def estimate_seconds(self, table: Dict) -> float:
        """
        Estimated single-stream copy time for ``table``, in seconds.

        ``max(bytes-bound time, rows-bound time)`` -- whichever resource binds.
        Never negative; returns ``0.0`` for an empty table. Never raises: unknown
        fields degrade to ``0.0`` and drop out of the ``max``.
        """
        bytes_term = self.live_bytes(table) / self.per_stream_bytes_per_sec
        rows = _num(table.get("row_estimate"))
        # reltuples is -1 when the table was never analysed; treat as unknown.
        rows_term = (rows / self.per_stream_rows_per_sec) if rows > 0.0 else 0.0
        return max(bytes_term, rows_term)

    def as_dict(self) -> Dict[str, float]:
        """The constants + assumptions, for embedding in ``plan.json``."""
        return {
            "per_stream_bytes_per_sec": self.per_stream_bytes_per_sec,
            "per_stream_rows_per_sec": self.per_stream_rows_per_sec,
            "toast_overhead_factor": self.toast_overhead_factor,
        }


#: The default model used when the planner is not handed a calibrated one.
DEFAULT = CostModel()
