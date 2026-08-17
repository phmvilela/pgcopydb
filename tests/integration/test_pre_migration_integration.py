"""
Manual integration tests for the pre-migration artifact stages: source-pg-metrics,
source-parameters, and source-pg-largeobjects.

Run against a real PostgreSQL server -- connection settings come from the
same ``PG_SOURCE_*`` / ``PG_OUTPUT_DIR`` environment variables the CLI reads
(see ``pg_cyan/collect/metrics.py::collect_metrics_from_env`` and
``pg_cyan/plan/parameters.py::export_parameters_from_env``). Excluded from the
default DB-free suite (see CLAUDE.md) via the ``integration`` marker; run
explicitly:

    PG_SOURCE_HOST=... PG_SOURCE_USER=... PG_SOURCE_PASSWORD=... \\
        python -m pytest tests/integration -m integration

or via the "Integration: pre-migration artifacts" GitHub Actions workflow
(workflow_dispatch), which also uploads the resulting JSON files as a build
artifact for human review.

Only the happy path is covered for each stage, and only the *shape* of the
output is checked -- presence of the documented fields, not their values.
New stages go here as another test function rather than a second integration
test file.
"""
import json
import os

import pytest

from pg_cyan.copy.lobs import PgCyanLobs
from pg_cyan.collect.metrics import PgCyanMetrics
from pg_cyan.plan.parameters import PgCyanParameters

pytestmark = pytest.mark.integration

_SOURCE_FIELDS = ("host", "port", "database")
_VERSION_FIELDS = ("version_string", "server_version", "server_version_num")
_DATABASE_SIZE_FIELDS = ("size_bytes", "size_pretty")
_OBJECT_COUNT_FIELDS = (
    "tables", "partitioned_tables", "partitions", "views",
    "materialized_views", "indexes", "sequences", "functions",
    "procedures", "triggers", "extensions",
)
_TABLE_FIELDS = (
    "schema", "name", "relkind",
    "heap_bytes", "heap_pretty", "index_bytes", "index_pretty",
    "row_estimate",
    # Cost-model inputs consumed by the planner. See also: pg_cyan/collect/cost_model.py
    "dead_tuple_ratio", "toast_bytes", "avg_row_width",
    # Split eligibility. See also: pg_cyan/collect/splitting.py
    "splittable_key",
)
_TABLE_TOTALS_FIELDS = ("heap_bytes", "index_bytes", "total_bytes")


def _connection_kwargs(output_dir_env="PG_OUTPUT_DIR"):
    return dict(
        host=os.getenv("PG_SOURCE_HOST", "localhost"),
        port=int(os.getenv("PG_SOURCE_PORT", "5432")),
        user=os.getenv("PG_SOURCE_USER", "postgres"),
        password=os.getenv("PG_SOURCE_PASSWORD", ""),
        database=os.getenv("PG_SOURCE_DATABASE", "postgres"),
        timeout=int(os.getenv("PREFLIGHT_TIMEOUT_SECONDS", "30")),
        output_dir=os.getenv(output_dir_env, "."),
    )


@pytest.fixture
def collector():
    instance = PgCyanMetrics(**_connection_kwargs())
    yield instance
    instance.close()


@pytest.fixture
def exporter():
    instance = PgCyanParameters(**_connection_kwargs())
    yield instance
    instance.close()


@pytest.fixture
def lobs():
    instance = PgCyanLobs(**_connection_kwargs())
    yield instance
    instance.close()


def test_collect_writes_source_pg_metrics_with_all_fields(collector):
    summary = collector.collect()

    assert summary["status"] == "success", collector.errors
    assert summary["output_file"]

    with open(summary["output_file"], encoding="utf-8") as fh:
        payload = json.load(fh)

    assert payload  # non-empty

    for field in ("timestamp", "component", "status", "source", "version",
                  "database_size", "object_counts", "tables", "table_totals"):
        assert field in payload, f"missing top-level field: {field}"

    for field in _SOURCE_FIELDS:
        assert field in payload["source"], f"missing source field: {field}"

    for field in _VERSION_FIELDS:
        assert field in payload["version"], f"missing version field: {field}"

    for field in _DATABASE_SIZE_FIELDS:
        assert field in payload["database_size"], f"missing database_size field: {field}"

    for field in _OBJECT_COUNT_FIELDS:
        assert field in payload["object_counts"], f"missing object_counts field: {field}"

    assert isinstance(payload["tables"], list)
    for table in payload["tables"]:
        for field in _TABLE_FIELDS:
            assert field in table, f"missing table field: {field}"

    for field in _TABLE_TOTALS_FIELDS:
        assert field in payload["table_totals"], f"missing table_totals field: {field}"
    # heap + index is reported as a distinct split that sums to the total.
    totals = payload["table_totals"]
    assert totals["heap_bytes"] + totals["index_bytes"] == totals["total_bytes"]


def test_export_writes_source_parameters_with_all_fields(exporter):
    summary = exporter.export()

    assert summary["status"] == "success", exporter.errors
    assert summary["output_file"]

    with open(summary["output_file"], encoding="utf-8") as fh:
        payload = json.load(fh)

    assert payload  # non-empty

    for field in ("timestamp", "component", "status", "source",
                  "parameter_count", "parameters"):
        assert field in payload, f"missing top-level field: {field}"

    for field in _SOURCE_FIELDS:
        assert field in payload["source"], f"missing source field: {field}"

    assert isinstance(payload["parameters"], list)
    assert payload["parameters"]  # pg_settings is never empty on a live server
    for parameter in payload["parameters"]:
        for field in PgCyanParameters._PARAM_COLUMNS:
            assert field in parameter, f"missing parameter field: {field}"


def test_collect_writes_source_lobs_with_all_fields(lobs):
    summary = lobs.collect()

    assert summary["status"] == "success", lobs.errors
    assert summary["output_file"]

    with open(summary["output_file"], encoding="utf-8") as fh:
        payload = json.load(fh)

    assert payload  # non-empty

    for field in ("timestamp", "component", "status", "source",
                  "lob_count"):
        assert field in payload, f"missing top-level field: {field}"

    for field in _SOURCE_FIELDS:
        assert field in payload["source"], f"missing source field: {field}"

    # A count is always present and non-negative (zero is valid).
    assert isinstance(payload["lob_count"], int)
    assert payload["lob_count"] >= 0
