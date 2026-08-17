"""
Unit tests for the human-readable plan report (`pg_cyan.plan.report`).

See also: pg_cyan/plan/report.py

DB-free by nature: the report is a pure renderer over a plan.json dict. Tests
exercise the full-plan rendering, the early-stage subset (no makespan/feasibility
yet), the degraded/gate warnings that must be visible, the byte/duration
humanizers, and the never-raises contract on malformed input.
"""
from pg_cyan.plan import report

MiB = 1024 * 1024


def _full_plan():
    """A plan carrying every block stages 1-5 produce."""
    return {
        "run_id": "run-42",
        "timestamp": "2026-08-11T00:00:00Z",
        "snapshot": {"mode": "coordinator-held", "not_consistent": False},
        "publication": {"name": "pgcyan_pub"},
        "slot": {"name": "pgcyan_slot"},
        "totals": {"table_count": 5, "shard_count": 2,
                   "heap_bytes": 3 * 1024 * MiB, "index_bytes": 200 * MiB,
                   "est_copy_seconds": 3720.0},
        "concurrency": {
            "c_star": 8, "binding_term": "target_write",
            "per_stream_bytes_per_sec": 50 * MiB,
            "shard_count": 2, "table_jobs_per_shard": 4,
            "terms": {"target_write": 8.0, "source_read": 20.0, "network_fill": 100.0},
            "clamps": ["max_shards=2"], "degraded": False,
        },
        "splitting": {
            "enabled": True, "ideal_share_seconds": 465.0,
            "predicted_makespan_seconds": 900.0,
            "makespan_without_splitting_seconds": 1800.0,
            "single_table_floor": False,
            "splits": [{"table": "public.events", "subtasks": 4,
                        "cost_seconds": 1800.0, "reason": "split"}],
        },
        "feasibility": {
            "status": "pass", "predicted_makespan_seconds": 900.0,
            "deadlines_seconds": {"wal_retention": 25600.0},
            "binding_deadline_seconds": 25600.0,
            "binding_deadline_source": "wal_retention",
            "margin_ratio": 0.035,
            "reasons": ["predicted makespan 900s is within all deadlines"],
        },
        "resume": {"enabled": True, "wal_disk_pct_pause": 85.0,
                   "checkpoints": ["shard-0", "shard-1"], "max_lag_seconds": 300.0},
        "bottleneck": "target-write",
        "attribution": {"bottleneck": "target-write", "binding_term": "target_write",
                        "term_value": 8.0, "remediation": "Provision more target IOPS."},
        "cost_model": {
            "constants": {"per_stream_bytes_per_sec": 50 * MiB,
                          "per_stream_rows_per_sec": 200000.0},
            "provenance": {"per_stream_bytes_per_sec": "history-calibrated",
                           "per_stream_rows_per_sec": "default"},
            "calibration": {"source": "history", "observations": 6,
                            "predicted_error_ratio": {"before": 0.52, "after": 0.40}},
        },
        "data_shards": [
            {"name": "shard-0", "table_count": 3, "heap_bytes": 2 * 1024 * MiB,
             "est_copy_seconds": 900.0, "table_jobs": 4,
             "split_tables_larger_than": 1500 * MiB},
            {"name": "shard-1", "table_count": 2, "heap_bytes": 1 * 1024 * MiB,
             "est_copy_seconds": 600.0, "table_jobs": 4,
             "split_tables_larger_than": "50GB"},
        ],
    }


def _minimal_plan():
    """A stage-1/2 plan: shard layout, cost, C* -- no makespan/feasibility/etc."""
    return {
        "run_id": "run-1",
        "snapshot": {"mode": "coordinator-held", "not_consistent": False},
        "publication": {"name": "pgcyan_pub"}, "slot": {"name": "pgcyan_slot"},
        "totals": {"table_count": 2, "shard_count": 2,
                   "heap_bytes": 100 * MiB, "index_bytes": 0, "est_copy_seconds": 12.0},
        "concurrency": {"c_star": 4, "binding_term": "source_read",
                        "per_stream_bytes_per_sec": 50 * MiB,
                        "shard_count": 2, "table_jobs_per_shard": 2,
                        "terms": {"source_read": 4.0}, "clamps": [], "degraded": False},
        "cost_model": {"constants": {"per_stream_bytes_per_sec": 50 * MiB,
                                     "per_stream_rows_per_sec": 200000.0}},
        "data_shards": [
            {"name": "shard-0", "table_count": 1, "heap_bytes": 60 * MiB,
             "est_copy_seconds": 7.0, "table_jobs": 2},
            {"name": "shard-1", "table_count": 1, "heap_bytes": 40 * MiB,
             "est_copy_seconds": 5.0, "table_jobs": 2},
        ],
    }


# --------------------------------------------------------------- full rendering

def test_full_plan_renders_every_section():
    md = report.render_plan_report(_full_plan())
    for heading in ["# Migration plan summary", "## Totals",
                    "## Predicted makespan & feasibility", "## Bottleneck",
                    "## Concurrency budget", "## Cost model", "## Table splitting",
                    "## Shard layout", "## Resume / circuit-breaker"]:
        assert heading in md
    # Human-friendly values, not raw bytes/seconds.
    assert "3.00 GiB" in md            # totals heap
    assert "15m 00s" in md             # 900s predicted makespan
    assert "target-write" in md and "Provision more target IOPS." in md
    # The binding term is flagged in the ceiling table.
    assert "⟵ binding" in md
    # Calibration provenance + error shrink surface.
    assert "history-calibrated" in md and "52% → 40%" in md
    # Shard layout shows both shards with humanized sizes.
    assert "shard-0" in md and "shard-1" in md and "2.00 GiB" in md


def test_full_plan_is_valid_markdown_tables():
    md = report.render_plan_report(_full_plan())
    # Each rendered table has a header separator row.
    assert md.count("| --- |") >= 3
    assert md.endswith("\n")


# ---------------------------------------------------------- early-stage subset

def test_minimal_plan_renders_without_later_blocks():
    md = report.render_plan_report(_minimal_plan())
    assert "## Totals" in md and "## Concurrency budget" in md
    assert "## Shard layout" in md and "## Cost model" in md
    # Blocks that don't exist yet are simply absent (no empty headings, no crash).
    assert "## Predicted makespan" not in md
    assert "## Bottleneck" not in md
    assert "## Table splitting" not in md
    assert "## Resume" not in md
    # A calibration-less cost model still renders its constants.
    assert "Bytes/stream" in md


# ------------------------------------------------------------ warnings & gates

def test_degraded_static_fallback_is_flagged():
    plan = _minimal_plan()
    plan["concurrency"]["degraded"] = True
    plan["concurrency"]["c_star"] = None
    plan["concurrency"]["binding_term"] = "none"
    md = report.render_plan_report(plan)
    assert "## ⚠️ Warnings & gates" in md
    assert "Static fallback" in md


def test_unsplittable_giant_and_abort_are_flagged():
    plan = _full_plan()
    plan["splitting"]["single_table_floor"] = True
    plan["feasibility"]["status"] = "abort"
    md = report.render_plan_report(plan)
    assert "Un-splittable giant" in md
    assert "Feasibility abort" in md


def test_feasibility_warn_is_flagged():
    plan = _full_plan()
    plan["feasibility"]["status"] = "warn"
    md = report.render_plan_report(plan)
    assert "Feasibility warning" in md


def test_no_warnings_section_when_all_clear():
    md = report.render_plan_report(_full_plan())
    assert "## ⚠️ Warnings & gates" not in md


# --------------------------------------------------------------- never raises

def test_non_dict_input_returns_a_string():
    for bad in [None, [], "nonsense", 42]:
        out = report.render_plan_report(bad)
        assert isinstance(out, str) and "Migration plan summary" in out


def test_malformed_blocks_degrade_without_raising():
    # Wrong types where dicts/lists are expected must not raise.
    plan = {"run_id": "x", "totals": "not-a-dict",
            "concurrency": {"terms": "not-a-dict", "degraded": False},
            "data_shards": "not-a-list", "cost_model": 5}
    out = report.render_plan_report(plan)
    assert isinstance(out, str) and out.endswith("\n")
    assert "# Migration plan summary" in out


def test_a_hostile_section_degrades_to_a_note_not_a_crash():
    # A block whose access itself raises must be caught per-section, so the rest
    # of the report still renders.
    class Boom(dict):
        def get(self, *a, **k):
            raise ValueError("hostile")

    out = report.render_plan_report({"run_id": "x", "concurrency": Boom()})
    assert out.startswith("# Migration plan summary")
    assert "could not render" in out


# ------------------------------------------------------------------- humanizers

def test_human_bytes():
    assert report._human_bytes(0) == "0 B"
    assert report._human_bytes(1023) == "1023 B"
    assert report._human_bytes(1024) == "1.00 KiB"
    assert report._human_bytes(1536 * MiB) == "1.50 GiB"
    assert report._human_bytes(None) == "?"
    assert report._human_bytes(-5) == "?"


def test_human_seconds():
    assert report._human_seconds(0) == "0s"
    assert report._human_seconds(45) == "45s"
    assert report._human_seconds(90) == "1m 30s"
    assert report._human_seconds(3661) == "1h 01m 01s"
    assert report._human_seconds(None) == "?"


def test_split_threshold_renders_ints_and_strings():
    assert report._split_threshold(1500 * MiB) == "1.46 GiB"
    assert report._split_threshold("50GB") == "50GB"
    assert report._split_threshold(None) == "—"
