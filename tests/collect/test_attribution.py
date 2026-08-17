"""
Unit tests for bottleneck attribution (`pg_cyan.collect.attribution`).

See also: pg_cyan/collect/attribution.py

DB-free by nature: attribution is a pure mapping from the concurrency budget's
binding term (and the split plan) to a canonical, actionable bottleneck label.
Tests exercise the canonical vocabulary, the single-table precedence rule, the
degraded (no-capacity) case, and that every result carries remediation.
"""
import pytest

from pg_cyan.collect import attribution
from pg_cyan.collect.capacity import Budget
from pg_cyan.collect.splitting import SplitPlan


def _budget(binding_term, terms=None, degraded=False):
    terms = terms if terms is not None else {binding_term: 4.0}
    return Budget(
        c_star=None if degraded else 4,
        binding_term=binding_term,
        per_stream_bytes_per_sec=50 * 1024 * 1024,
        shard_count=2, table_jobs=2, max_table_jobs=8,
        terms=terms, clamps=[], degraded=degraded,
    )


def _split_plan(single_table_floor=False, makespan=100.0):
    return SplitPlan(
        enabled=True, ideal_share_seconds=10.0,
        predicted_makespan_seconds=makespan,
        makespan_without_splitting_seconds=makespan,
        single_table_floor=single_table_floor,
        total_cost_seconds=200.0, decisions=[],
    )


@pytest.mark.parametrize("raw,canonical", [
    ("source_read", "source-read"),
    ("source_connections", "source-connections"),
    ("network_fill", "network"),
    ("target_write", "target-write"),
    ("target_connections", "target-connections"),
    ("k8s_cpu", "k8s-cpu"),
    ("constraints_max_concurrency", "operator-constraint"),
])
def test_binding_term_maps_to_canonical_label(raw, canonical):
    attr = attribution.attribute(_budget(raw), _split_plan())
    assert attr.bottleneck == canonical
    assert attr.binding_term == raw          # raw term preserved for auditing
    assert attr.term_value == pytest.approx(4.0)
    assert attr.remediation                  # every label has an operator action


def test_single_table_floor_takes_precedence_over_the_budget_term():
    # Even with a normal binding term, an un-splittable giant wins.
    attr = attribution.attribute(
        _budget("target_write"), _split_plan(single_table_floor=True, makespan=99.0))
    assert attr.bottleneck == "single-table"
    assert attr.binding_term == "single_table_floor"
    assert attr.term_value == pytest.approx(99.0)
    assert "won't help" in attr.remediation or "won’t help" in attr.remediation


def test_degraded_budget_has_no_bottleneck():
    attr = attribution.attribute(
        _budget("none", terms={}, degraded=True), _split_plan())
    assert attr.bottleneck == "none"
    assert attr.binding_term == "none"
    assert attr.term_value is None
    assert attr.remediation


def test_unknown_term_passes_through_with_a_generic_remediation():
    attr = attribution.attribute(_budget("some_future_term"), _split_plan())
    assert attr.bottleneck == "some_future_term"
    assert attr.remediation


def test_as_dict_is_a_self_describing_block():
    block = attribution.attribute(_budget("network_fill"), _split_plan()).as_dict()
    assert block["bottleneck"] == "network"
    assert block["binding_term"] == "network_fill"
    assert "remediation" in block and "note" in block
