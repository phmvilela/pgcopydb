"""
Copylog-history producer: turn completed copy runs into calibration history.

The planner's cost model (pg_cyan/cost_model.py) starts from conservative
throughput constants; #118's calibration (pg_cyan/calibration.py) refines them
from a ``copylog-history.json`` artifact of prior per-run copy durations, so each
migration's plan is fitted to what real runs achieved. That artifact is
*consumed* already but nothing *produces* it -- so on real deployments the
calibration is dormant and every plan uses the default constant. This is the
producer: it reads the copy step's logs (``migration-copy-log.json`` per shard,
which now carry ``duration_seconds`` + ``table_jobs``) and appends a run's
observations to ``copylog-history.json`` for the next run's planner.

**Observation model.** A shard copies its tables with ``table_jobs`` parallel
COPY streams, so its wall-clock is *aggregate*, not single-stream, throughput --
but the calibrated constant is per-stream. Each shard becomes one observation
normalized to a single-stream equivalent: ``duration_seconds = wall x table_jobs``
(volume unchanged), which under ideal intra-shard parallelism recovers the
single-stream rate ``rows / (wall x table_jobs)``. Imperfect parallelism makes
this *over*-estimate the duration (under-estimate the rate) -- a conservative
bias (the planner budgets more time), not an optimistic one. True per-table
durations from pgcopydb's own accounting are the precise future refinement.

Volume is ``rows`` from the copylog (ground truth); when a ``source-pg-metrics``
artifact is supplied, the shard's tables' ``heap_bytes`` are summed into a
``live_bytes`` volume too, so the bytes constant can be fitted as well.

Retention is windowed (the most recent ``max_runs`` runs) so history stays
representative without unbounded growth. It is a pure transform and never raises.

See also: pg_cyan/calibration.py (the consumer), pg_cyan/datashard.py (emits the
per-shard copylog), pg_cyan/capacity_contract.py.
"""
import glob
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

from pg_cyan import capacity_contract

logger = logging.getLogger(__name__)


def _pos(value) -> Optional[float]:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


class PgCyanCopylogHistory:
    """Appends a completed run's copy observations to ``copylog-history.json``."""

    OUTPUT_FILENAME = "copylog-history.json"
    DEFAULT_MAX_RUNS = 20

    def __init__(
        self,
        output_dir: str = ".",
        max_runs: int = DEFAULT_MAX_RUNS,
        run_id: Optional[str] = None,
    ):
        self.output_dir = output_dir
        self.max_runs = max(1, int(max_runs))
        self.run_id = run_id or "run-" + datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.errors: List[str] = []

    def _log_event(self, event_type, status, message, details=None) -> None:
        logger.info(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "copylog-history",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }))

    # ------------------------------------------------------------------ inputs

    @staticmethod
    def load_copylogs(path: str) -> List[Dict]:
        """Load copylog dict(s) from a file or a directory of ``*.json``."""
        files = []
        if os.path.isdir(path):
            files = sorted(glob.glob(os.path.join(path, "*.json")))
        elif os.path.isfile(path):
            files = [path]
        logs = []
        for f in files:
            try:
                with open(f, encoding="utf-8") as fh:
                    data = json.load(fh)
            except (OSError, ValueError):
                continue
            # A copylog is a dict carrying a copy result; ignore anything else
            # (e.g. an unrelated artifact that happens to share the directory).
            if isinstance(data, dict) and (
                    "duration_seconds" in data or "row_count_total" in data
                    or data.get("component") == "data-shard"):
                logs.append(data)
        return logs

    @staticmethod
    def _metrics_bytes(metrics: Optional[Dict]) -> Dict[str, Dict]:
        """Map ``"schema.table"`` -> its metrics row (for byte enrichment)."""
        if not isinstance(metrics, dict):
            return {}
        out = {}
        for t in metrics.get("tables") or []:
            if isinstance(t, dict) and t.get("schema") and t.get("name"):
                out[f"{t['schema']}.{t['name']}"] = t
        return out

    def _observation(self, copylog: Dict, by_name: Dict[str, Dict]) -> Optional[Dict]:
        """One single-stream-normalized observation from a shard copylog, or None."""
        wall = _pos(copylog.get("duration_seconds"))
        if wall is None:
            return None
        table_jobs = _pos(copylog.get("table_jobs")) or 1.0
        effective = wall * table_jobs

        rows = _pos(copylog.get("row_count_total")) or 0.0
        obs = {"duration_seconds": round(effective, 2),
               "shard": copylog.get("shard"),
               "raw_duration_seconds": round(wall, 2),
               "table_jobs": int(table_jobs)}
        if rows > 0:
            obs["rows"] = rows

        # Byte volume from metrics: sum the shard's tables' heap-derived live bytes.
        live = 0.0
        tables = copylog.get("tables")
        if by_name and isinstance(tables, dict):
            for name in tables:
                m = by_name.get(name)
                if not m:
                    continue
                heap = _pos(m.get("heap_bytes"))
                if heap is None:
                    continue
                dead = m.get("dead_tuple_ratio") or 0.0
                try:
                    dead = min(max(float(dead), 0.0), 0.99)
                except (TypeError, ValueError):
                    dead = 0.0
                live += heap * (1.0 - dead)
        if live > 0:
            obs["live_bytes"] = round(live, 2)

        if "rows" not in obs and "live_bytes" not in obs:
            return None  # no volume -> unusable for calibration
        return obs

    def _read_existing(self) -> Dict:
        path = os.path.join(self.output_dir, self.OUTPUT_FILENAME)
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _write_json(self, payload: Dict) -> Optional[str]:
        path = os.path.join(self.output_dir, self.OUTPUT_FILENAME)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            return path
        except OSError as e:
            self.errors.append(f"Failed to write {self.OUTPUT_FILENAME}: {e}")
            return None

    def produce(self, copylogs: List[Dict], metrics: Optional[Dict] = None,
                timestamp: Optional[str] = None) -> Dict:
        """
        Append this run's observations to copylog-history.json (windowed).

        Returns a summary dict. Never raises. When the run yields no usable
        observation, the existing history is preserved unchanged.
        """
        stamp = timestamp or (datetime.utcnow().isoformat() + "Z")
        by_name = self._metrics_bytes(metrics)
        # A combined (orchestrator) copylog carries per-shard timing under
        # shard_summaries; expand it so each shard is one observation. A per-shard
        # log is used as-is.
        shards: List[Dict] = []
        for c in copylogs:
            summaries = c.get("shard_summaries") if isinstance(c, dict) else None
            if isinstance(summaries, list) and summaries:
                shards.extend(summaries)
            else:
                shards.append(c)
        observations = [o for o in (self._observation(c, by_name) for c in shards)
                        if o is not None]

        history = self._read_existing()
        runs = history.get("runs")
        runs = list(runs) if isinstance(runs, list) else []

        if observations:
            runs.append({"run_id": self.run_id, "timestamp": stamp,
                         "shard_count": len(observations),
                         "observations": observations})
            runs = runs[-self.max_runs:]  # windowed retention
        else:
            self._log_event("produce", "warning",
                            "No usable copy observations (missing durations?); "
                            "history unchanged",
                            {"copylogs": len(copylogs)})

        payload = capacity_contract.stamp({
            "component": "copylog-history",
            "updated": stamp,
            "runs": runs,
            "note": ("Prior copy runs' per-shard observations, normalized to a "
                     "single-stream equivalent (duration x table_jobs). Consumed by "
                     "pg_cyan/calibration.py to fit the throughput constants. "
                     "Windowed to the most recent runs."),
        })
        output_path = self._write_json(payload)
        status = "success" if output_path else "failure"
        if output_path:
            self._log_event("produce", "success",
                            f"Wrote copylog-history with {len(runs)} run(s)",
                            {"added_observations": len(observations),
                             "total_runs": len(runs)})
        return {"status": status, "component": "copylog-history",
                "output_file": output_path,
                "added_observations": len(observations),
                "total_runs": len(runs), "errors": self.errors}


def produce_copylog_history_from_env(copylog_path: str, metrics_path: Optional[str],
                                     output_dir: str, max_runs: int,
                                     run_id: Optional[str]) -> int:
    """Run the producer from CLI/env settings. Returns 0 on success, 1 on write failure."""
    try:
        producer = PgCyanCopylogHistory(output_dir=output_dir, max_runs=max_runs, run_id=run_id)
        copylogs = producer.load_copylogs(copylog_path)
        metrics = None
        if metrics_path and os.path.isfile(metrics_path):
            try:
                with open(metrics_path, encoding="utf-8") as fh:
                    metrics = json.load(fh)
            except (OSError, ValueError):
                metrics = None
        summary = producer.produce(copylogs, metrics=metrics)
        print(json.dumps(summary, indent=2, default=str))
        return 0 if summary["status"] == "success" else 1
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Copylog-history production interrupted by user",
        }))
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(produce_copylog_history_from_env(
        os.getenv("PGCYAN_COPYLOG_PATH", "."), os.getenv("PGCYAN_METRICS_PATH"),
        os.getenv("PG_OUTPUT_DIR", "."), int(os.getenv("PGCYAN_HISTORY_MAX_RUNS", "20")),
        os.getenv("PGCYAN_RUN_ID")))
