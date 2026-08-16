"""
DB-free tests for the copylog-history producer (pg_cyan/copyloghistory.py).

Pins the single-stream normalization, byte enrichment from metrics, windowed
retention, and -- the point of the whole thing -- that #118's calibration
actually consumes the produced artifact and fits from it.
"""
import json

from pg_cyan import calibration
from pg_cyan.copyloghistory import PgCyanCopylogHistory


def _shard_log(shard="shard-0", duration=10.0, table_jobs=5, rows=1000, tables=None):
    return {"component": "data-shard", "status": "completed", "shard": shard,
            "duration_seconds": duration, "table_jobs": table_jobs,
            "row_count_total": rows, "tables": tables or {"public.a": rows}}


def test_observation_is_single_stream_normalized(tmp_path):
    p = PgCyanCopylogHistory(output_dir=str(tmp_path), run_id="r1")
    summary = p.produce([_shard_log(duration=10.0, table_jobs=5, rows=1000)])

    assert summary["status"] == "success" and summary["added_observations"] == 1
    hist = json.loads((tmp_path / "copylog-history.json").read_text())
    obs = hist["runs"][0]["observations"][0]
    assert obs["duration_seconds"] == 50.0        # wall 10 x table_jobs 5
    assert obs["raw_duration_seconds"] == 10.0
    assert obs["rows"] == 1000


def test_copylog_without_duration_is_skipped(tmp_path):
    p = PgCyanCopylogHistory(output_dir=str(tmp_path))
    log = _shard_log()
    del log["duration_seconds"]
    summary = p.produce([log])
    assert summary["added_observations"] == 0
    # No usable observation -> no run appended.
    assert json.loads((tmp_path / "copylog-history.json").read_text())["runs"] == []


def test_metrics_enrich_observation_with_live_bytes(tmp_path):
    metrics = {"tables": [
        {"schema": "public", "name": "a", "heap_bytes": 1000, "dead_tuple_ratio": 0.2},
        {"schema": "public", "name": "b", "heap_bytes": 500, "dead_tuple_ratio": 0.0},
    ]}
    log = _shard_log(rows=10, tables={"public.a": 6, "public.b": 4})
    p = PgCyanCopylogHistory(output_dir=str(tmp_path))
    p.produce([log], metrics=metrics)
    obs = json.loads((tmp_path / "copylog-history.json").read_text())["runs"][0]["observations"][0]
    assert obs["live_bytes"] == 1000 * 0.8 + 500      # 1300.0


def test_windowed_retention_keeps_recent_runs(tmp_path):
    for i in range(4):
        PgCyanCopylogHistory(output_dir=str(tmp_path), max_runs=2, run_id=f"r{i}") \
            .produce([_shard_log()])
    runs = json.loads((tmp_path / "copylog-history.json").read_text())["runs"]
    assert [r["run_id"] for r in runs] == ["r2", "r3"]   # oldest dropped


def test_no_observations_preserves_existing_history(tmp_path):
    PgCyanCopylogHistory(output_dir=str(tmp_path), run_id="r0").produce([_shard_log()])
    before = (tmp_path / "copylog-history.json").read_text()
    # A later run with unusable logs must not wipe the accumulated history.
    PgCyanCopylogHistory(output_dir=str(tmp_path), run_id="r1").produce([{"no": "duration"}])
    after = json.loads((tmp_path / "copylog-history.json").read_text())
    assert [r["run_id"] for r in after["runs"]] == ["r0"]


def test_load_copylogs_from_directory(tmp_path):
    (tmp_path / "migration-copy-log-0.json").write_text(json.dumps(_shard_log("shard-0")))
    (tmp_path / "migration-copy-log-1.json").write_text(json.dumps(_shard_log("shard-1")))
    (tmp_path / "unrelated.json").write_text(json.dumps({"hello": "world"}))
    logs = PgCyanCopylogHistory.load_copylogs(str(tmp_path))
    assert {l["shard"] for l in logs} == {"shard-0", "shard-1"}   # unrelated ignored


def test_expands_combined_log_shard_summaries(tmp_path):
    # The orchestrator's combined copylog carries per-shard timing under
    # shard_summaries; the producer expands it into one observation per shard.
    combined = {"component": "orchestrator", "status": "completed",
                "row_count_total": 30, "shard_summaries": [
                    {"shard": "shard-0", "duration_seconds": 4, "table_jobs": 2, "row_count_total": 10},
                    {"shard": "shard-1", "duration_seconds": 6, "table_jobs": 2, "row_count_total": 20},
                ]}
    summary = PgCyanCopylogHistory(output_dir=str(tmp_path), run_id="r1").produce([combined])
    assert summary["added_observations"] == 2
    obs = json.loads((tmp_path / "copylog-history.json").read_text())["runs"][0]["observations"]
    assert {o["shard"] for o in obs} == {"shard-0", "shard-1"}
    assert obs[0]["duration_seconds"] == 8.0   # 4 x 2


def test_calibration_consumes_the_produced_history(tmp_path):
    # The whole point: #118 calibration must fit from what we produce.
    p = PgCyanCopylogHistory(output_dir=str(tmp_path), run_id="r1")
    p.produce([_shard_log(duration=8.0, table_jobs=4, rows=2_000_000),
               _shard_log("shard-1", duration=9.0, table_jobs=4, rows=2_100_000)])
    history = json.loads((tmp_path / "copylog-history.json").read_text())

    result = calibration.calibrate(history)
    assert result.source == "history"
    assert result.observations == 2
    assert result.observed_rows_per_sec is not None
