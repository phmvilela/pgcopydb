"""
Human-readable plan report for the migration planner.

``plan.json`` is the machine-oriented plan every downstream step consumes, but
nobody should approve a production migration by eyeballing raw JSON. This module
renders the same plan as a reviewable ``plan-summary.md`` -- shard layout,
predicted makespan and feasibility verdict, the concurrency budget and its
binding term, the cost-model constants and how they were calibrated, and any
gates/warnings an operator must see before hitting start.

It is a **pure renderer** over the plan dict (``render_plan_report(plan) -> str``),
decoupled from planning so it is DB-free, independently unit-testable, and
reusable by the control-plane API to surface the same content. Consistent with
the planner contract it **never raises**: every field is read defensively, so an
older plan carrying only the early-stage blocks still renders, and any unexpected
shape degrades to a note rather than an exception -- a rendering failure must
never fail the plan.

See also: pg_cyan/planner.py (writes plan.json then calls this), and the block
producers pg_cyan/capacity.py, pg_cyan/splitting.py, pg_cyan/feasibility.py,
pg_cyan/attribution.py, pg_cyan/calibration.py.
"""
from typing import Dict, List, Optional

_BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]


def _human_bytes(value) -> str:
    """Render a byte count as a compact binary-unit string (``1.50 GiB``)."""
    try:
        n = float(value)
    except (TypeError, ValueError):
        return "?"
    if n < 0:
        return "?"
    unit = 0
    while n >= 1024.0 and unit < len(_BYTE_UNITS) - 1:
        n /= 1024.0
        unit += 1
    return (f"{int(n)} {_BYTE_UNITS[unit]}" if unit == 0
            else f"{n:.2f} {_BYTE_UNITS[unit]}")


def _human_seconds(value) -> str:
    """Render a duration in seconds as ``2h 05m 30s`` (dropping leading zeros)."""
    try:
        total = float(value)
    except (TypeError, ValueError):
        return "?"
    if total < 0:
        return "?"
    total = int(round(total))
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if hours or minutes:
        parts.append(f"{minutes:02d}m" if hours else f"{minutes}m")
    parts.append(f"{seconds:02d}s" if (hours or minutes) else f"{seconds}s")
    return " ".join(parts)


def _get(mapping, key, default=None):
    """``mapping.get(key)`` that tolerates ``mapping`` not being a dict."""
    return mapping.get(key, default) if isinstance(mapping, dict) else default


def _table(headers: List[str], rows: List[List[str]]) -> List[str]:
    """Render a GitHub-flavoured Markdown table as a list of lines."""
    lines = ["| " + " | ".join(headers) + " |",
             "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return lines


def _header_section(plan: Dict) -> List[str]:
    snapshot = _get(plan, "snapshot", {})
    mode = _get(snapshot, "mode", "?")
    consistent = "no (frozen source)" if _get(snapshot, "not_consistent") else "yes"
    return [
        "# Migration plan summary",
        "",
        f"- **Run id:** {_get(plan, 'run_id', '?')}",
        f"- **Generated:** {_get(plan, 'timestamp', '?')}",
        f"- **Snapshot mode:** {mode} (consistent: {consistent})",
        f"- **Publication / slot:** "
        f"{_get(_get(plan, 'publication', {}), 'name', '?')} / "
        f"{_get(_get(plan, 'slot', {}), 'name', '?')}",
    ]


def _totals_section(plan: Dict) -> List[str]:
    totals = _get(plan, "totals", {})
    return [
        "## Totals",
        "",
        f"- **Tables:** {_get(totals, 'table_count', '?')} "
        f"across {_get(totals, 'shard_count', '?')} shard(s)",
        f"- **Heap:** {_human_bytes(_get(totals, 'heap_bytes'))} "
        f"(+ {_human_bytes(_get(totals, 'index_bytes'))} indexes)",
        f"- **Serial copy time (Σ per-stream):** "
        f"{_human_seconds(_get(totals, 'est_copy_seconds'))}",
    ]


def _makespan_section(plan: Dict) -> Optional[List[str]]:
    """Predicted makespan + feasibility verdict (present from stage 4 on)."""
    feasibility = _get(plan, "feasibility")
    splitting = _get(plan, "splitting", {})
    makespan = (_get(feasibility, "predicted_makespan_seconds")
                if feasibility else _get(splitting, "predicted_makespan_seconds"))
    if feasibility is None and makespan is None:
        return None

    lines = ["## Predicted makespan & feasibility", ""]
    if makespan is not None:
        lines.append(f"- **Predicted makespan:** {_human_seconds(makespan)} "
                     "(the slowest shard's wall-clock)")
    if feasibility is not None:
        status = _get(feasibility, "status", "?")
        badge = {"pass": "✅ pass", "warn": "⚠️ warn", "abort": "⛔ abort",
                 "not_evaluated": "— not evaluated"}.get(status, status)
        lines.append(f"- **Feasibility:** {badge}")
        source = _get(feasibility, "binding_deadline_source")
        deadline = _get(feasibility, "binding_deadline_seconds")
        if source and deadline is not None:
            margin = _get(feasibility, "margin_ratio")
            margin_str = f" (uses {margin:.0%} of it)" if isinstance(margin, (int, float)) else ""
            lines.append(f"- **Tightest deadline:** {source} "
                         f"{_human_seconds(deadline)}{margin_str}")
        for reason in _get(feasibility, "reasons", []) or []:
            lines.append(f"  - {reason}")
    return lines


def _bottleneck_section(plan: Dict) -> Optional[List[str]]:
    """Plain-language bottleneck + remediation (present from stage 5 on)."""
    bottleneck = _get(plan, "bottleneck")
    if not bottleneck:
        return None
    attribution = _get(plan, "attribution", {})
    lines = ["## Bottleneck", "", f"- **Binding constraint:** `{bottleneck}`"]
    raw = _get(attribution, "binding_term")
    if raw and raw != bottleneck:
        lines.append(f"- **Raw C\\* term:** `{raw}`")
    remediation = _get(attribution, "remediation")
    if remediation:
        lines.append(f"- **To go faster:** {remediation}")
    return lines


def _concurrency_section(plan: Dict) -> List[str]:
    conc = _get(plan, "concurrency", {})
    degraded = _get(conc, "degraded")
    lines = ["## Concurrency budget", ""]
    if degraded:
        lines.append("- **Source:** ⚠️ static fallback — no capacity data supplied; "
                     "shard count is the configured default, not a computed C\\*.")
    else:
        lines.append(f"- **C\\* (parallel COPY streams):** {_get(conc, 'c_star', '?')}, "
                     f"bound by `{_get(conc, 'binding_term', '?')}`")
    lines.append(f"- **Shape:** {_get(conc, 'shard_count', '?')} shard(s) × "
                 f"{_get(conc, 'table_jobs_per_shard', '?')} table_jobs")
    per_stream = _get(conc, "per_stream_bytes_per_sec")
    if per_stream is not None:
        lines.append(f"- **Per-stream throughput:** {_human_bytes(per_stream)}/s")
    terms = _get(conc, "terms", {})
    if terms:
        lines += ["", "**Ceiling terms** (C\\* is the smallest):", ""]
        binding = _get(conc, "binding_term")
        rows = [[f"`{name}`" + (" ⟵ binding" if name == binding else ""),
                 f"{value:.2f}" if isinstance(value, (int, float)) else value]
                for name, value in sorted(terms.items(), key=_term_sort_key)]
        lines += _table(["Term", "Streams"], rows)
    clamps = _get(conc, "clamps", [])
    if clamps:
        lines.append("")
        lines.append("**Applied clamps (constraints.json, tighten-only):** "
                     + ", ".join(f"`{c}`" for c in clamps))
    return lines


def _term_sort_key(item):
    """Sort ceiling terms by value ascending (binding first); non-numeric last."""
    _, value = item
    return (0, value) if isinstance(value, (int, float)) else (1, 0)


def _splitting_section(plan: Dict) -> Optional[List[str]]:
    splitting = _get(plan, "splitting")
    if not splitting or not _get(splitting, "enabled"):
        return None
    splits = _get(splitting, "splits", []) or []
    lines = ["## Table splitting", "",
             f"- **Ideal per-stream share:** "
             f"{_human_seconds(_get(splitting, 'ideal_share_seconds'))}"]
    without = _get(splitting, "makespan_without_splitting_seconds")
    with_ = _get(splitting, "predicted_makespan_seconds")
    if without is not None and with_ is not None:
        lines.append(f"- **Makespan floor:** {_human_seconds(without)} without "
                     f"splitting → {_human_seconds(with_)} with it")
    if not splits:
        lines.append("- No table exceeded the ideal share; nothing split.")
        return lines
    lines += [""]
    rows = [[f"`{_get(s, 'table', '?')}`", _get(s, "subtasks", "?"),
             _human_seconds(_get(s, "cost_seconds")), _get(s, "reason", "?")]
            for s in splits]
    lines += _table(["Table", "Subtasks", "Solo cost", "Reason"], rows)
    return lines


def _shard_layout_section(plan: Dict) -> List[str]:
    shards = _get(plan, "data_shards", []) or []
    lines = ["## Shard layout", ""]
    if not shards:
        lines.append("- (no shards)")
        return lines
    rows = [[_get(s, "name", "?"), _get(s, "table_count", "?"),
             _human_bytes(_get(s, "heap_bytes")),
             _human_seconds(_get(s, "est_copy_seconds")),
             _get(s, "table_jobs", "?"),
             _split_threshold(_get(s, "split_tables_larger_than"))]
            for s in shards]
    lines += _table(
        ["Shard", "Tables", "Heap", "Est copy", "table_jobs", "split >"], rows)
    return lines


def _split_threshold(value) -> str:
    """Render a shard's split threshold (an int byte count or a size string)."""
    if value is None:
        return "—"
    if isinstance(value, bool):  # guard: bool is an int subclass
        return str(value)
    if isinstance(value, (int, float)):
        return _human_bytes(value)
    return str(value)


def _cost_model_section(plan: Dict) -> Optional[List[str]]:
    cost_model = _get(plan, "cost_model")
    if not cost_model:
        return None
    constants = _get(cost_model, "constants", {})
    provenance = _get(cost_model, "provenance", {})
    lines = ["## Cost model", ""]
    bps = _get(constants, "per_stream_bytes_per_sec")
    rps = _get(constants, "per_stream_rows_per_sec")
    if bps is not None:
        src = _get(provenance, "per_stream_bytes_per_sec")
        lines.append(f"- **Bytes/stream:** {_human_bytes(bps)}/s"
                     + (f" _(source: {src})_" if src else ""))
    if rps is not None:
        src = _get(provenance, "per_stream_rows_per_sec")
        lines.append(f"- **Rows/stream:** {int(rps):,}/s"
                     + (f" _(source: {src})_" if src else ""))
    calibration = _get(cost_model, "calibration")
    if calibration:
        lines += _calibration_lines(calibration)
    return lines


def _calibration_lines(calibration: Dict) -> List[str]:
    source = _get(calibration, "source", "?")
    if source != "history":
        return ["- **Calibration:** none — using default/probe constants "
                "(no usable copy history)."]
    obs = _get(calibration, "observations", "?")
    lines = [f"- **Calibration:** fitted from {obs} prior copy observation(s)."]
    error = _get(calibration, "predicted_error_ratio", {})
    before, after = _get(error, "before"), _get(error, "after")
    if isinstance(before, (int, float)) and isinstance(after, (int, float)):
        lines.append(f"  - Predicted-duration error: {before:.0%} → {after:.0%} "
                     "after calibration.")
    return lines


def _resume_section(plan: Dict) -> Optional[List[str]]:
    resume = _get(plan, "resume")
    if not resume:
        return None
    if not _get(resume, "enabled", True):
        return ["## Resume / circuit-breaker", "", "- Disabled by constraints."]
    checkpoints = _get(resume, "checkpoints", []) or []
    lines = ["## Resume / circuit-breaker", "",
             f"- **Pause when source WAL disk >** "
             f"{_get(resume, 'wal_disk_pct_pause', '?')}%",
             f"- **Checkpoints:** {len(checkpoints)} (one per completed shard)"]
    lag = _get(resume, "max_lag_seconds")
    if lag is not None:
        lines.append(f"- **Max CDC lag:** {_human_seconds(lag)}")
    return lines


def _warnings_section(plan: Dict) -> Optional[List[str]]:
    """Collect the gates/warnings an operator must see, if any."""
    warnings: List[str] = []
    if _get(_get(plan, "concurrency", {}), "degraded"):
        warnings.append("⚠️ **Static fallback:** no capacity data — the shard count "
                        "is the configured default, not a computed budget.")
    if _get(_get(plan, "splitting", {}), "single_table_floor"):
        warnings.append("⚠️ **Un-splittable giant:** one table alone floors the "
                        "makespan; widening C\\* will not help (see Bottleneck).")
    feasibility = _get(plan, "feasibility", {})
    status = _get(feasibility, "status")
    if status == "warn":
        warnings.append("⚠️ **Feasibility warning:** the predicted makespan is close "
                        "to a deadline (see above).")
    elif status == "abort":
        warnings.append("⛔ **Feasibility abort:** the plan exceeds a hard deadline.")
    if not warnings:
        return None
    return ["## ⚠️ Warnings & gates", ""] + [f"- {w}" for w in warnings]


#: The ordered section renderers. Each returns lines, or ``None`` to skip when
#: its block is absent (so an early-stage plan renders without empty sections).
_SECTIONS = [
    _header_section,
    _warnings_section,
    _totals_section,
    _makespan_section,
    _bottleneck_section,
    _concurrency_section,
    _cost_model_section,
    _splitting_section,
    _shard_layout_section,
    _resume_section,
]


def render_plan_report(plan: Dict) -> str:
    """
    Render a ``plan.json`` dict as a reviewable Markdown report.

    Pure and defensive: reads every field with fallbacks so a plan carrying only
    the early-stage blocks still renders, and never raises -- an unexpected shape
    degrades to a short error note appended to whatever rendered. See also:
    pg_cyan/planner.py.
    """
    if not isinstance(plan, dict):
        return "# Migration plan summary\n\n_(no plan available to render)_\n"
    blocks: List[str] = []
    for section in _SECTIONS:
        try:
            lines = section(plan)
        except Exception as e:  # noqa: BLE001 - a bad section must not sink the report
            lines = [f"_(could not render {section.__name__}: {e})_"]
        if lines:
            blocks.append("\n".join(lines))
    return "\n\n".join(blocks) + "\n"
