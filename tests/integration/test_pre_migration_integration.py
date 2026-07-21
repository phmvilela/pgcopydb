"""
Manual integration tests for the pre-migration artifact stages: source-pg-metrics
(issue #19) and source-parameters (issue #4).

Run against a real PostgreSQL server -- connection settings come from the
same ``PG_SOURCE_*`` / ``PG_OUTPUT_DIR`` environment variables the CLI reads
(see ``pg_cyan/metrics.py::collect_metrics_from_env`` and
``pg_cyan/parameters.py::export_parameters_from_env``). Excluded from the
default DB-free suite (see CLAUDE.md) via the ``integration`` marker; run
explicitly:

    PG_SOURCE_HOST=... PG_SOURCE_USER=... PG_SOURCE_PASSWORD=... \\
        python -m pytest tests/integration -m integration

or via the "Integration: pre-migration artifacts" GitHub Actions workflow
(workflow_dispatch), which also uploads the resulting JSON files as a build
artifact for human review.

Only the happy path is covered for each stage, and only the *shape* of the
output is checked -- presence of the documented fields, not their values
(per issue #19, extended the same way to source-parameters rather than
adding a second integration test file).
"""
import json
import os

import pytest

from pg_cyan.metrics import PgCyanMetrics
from pg_cyan.parameters import PgCyanParameters

pytestmark = pytest.mark.integration

_SOURCE_FIELDS = ("host", "port", "database")
_VERSION_FIELDS = ("version_string", "server_version", "server_version_num")
_DATABASE_SIZE_FIELDS = ("size_bytes", "size_pretty")
_OBJECT_COUNT_FIELDS = (
    "tables", "partitioned_tables", "partitions", "views",
    "materialized_views", "indexes", "sequences", "functions",
    "procedures", "triggers", "extensions",
)
_TABLE_FIELDS = ("schema", "name", "relkind", "size_bytes", "size_pretty", "row_estimate")


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


def test_collect_writes_source_pg_metrics_with_all_fields(collector):
    summary = collector.collect()

    assert summary["status"] == "success", collector.errors
    assert summary["output_file"]

    with open(summary["output_file"], encoding="utf-8") as fh:
        payload = json.load(fh)

    assert payload  # non-empty (issue #3 acceptance criterion 1)

    for field in ("timestamp", "component", "status", "source", "version",
                  "database_size", "object_counts", "tables"):
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


def test_export_writes_source_parameters_with_all_fields(exporter):
    summary = exporter.export()

    assert summary["status"] == "success", exporter.errors
    assert summary["output_file"]

    with open(summary["output_file"], encoding="utf-8") as fh:
        payload = json.load(fh)

    assert payload  # non-empty (issue #4 acceptance criterion 1)

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
