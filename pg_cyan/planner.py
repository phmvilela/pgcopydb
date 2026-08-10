"""
Migration planner for the sharded pgcopydb topology.

This is the step ``-1`` node in ``deploy/k8s/`` (``30-job-planner.yaml``): a
pure, **offline** transform that reads the pre-collected source artifacts and
emits the migration *plan* every downstream Kubernetes step consumes --
``plan.json`` plus one ``shard-<n>.filters`` file per copy shard (pgcopydb
``--filters`` format with a disjoint ``[include-only-table]`` list). It never
connects to a database; it only reads the JSON already produced by the
pre-migration stages.

Inputs (read from ``--inputs-dir`` / ``$PG_INPUTS_DIR``):
  * ``source-pg-metrics.json``      -- per-table heap/index bytes plus the cost
    inputs (``row_estimate``, ``dead_tuple_ratio``, ``toast_bytes``); required.
    The extra cost fields are optional per row and degrade gracefully.
  * ``source-pg-largeobjects.json`` -- large-object count; required for the abort gate.
  * ``pg-source-parameters.json``   -- instance parameters; optional.
  * ``constraints.json``            -- operator-supplied ceilings/overrides; optional.
  * ``source-capacity.json`` / ``target-capacity.json`` / ``network.json`` /
    ``cluster-capacity.json`` -- capacity artifacts sizing the concurrency
    budget; all optional. See also: pg_cyan/capacity.py.

Responsibilities (this ticket, per the resolved scope):
  * **Bin-pack** tables into N disjoint shards by **estimated copy time** (LPT
    greedy) so the copy step's COPY is balanced by wall-clock, not raw bytes.
    The per-table weight is ``cost_model.CostModel.estimate_seconds`` -- roughly
    ``max(live_bytes / per_stream_bw, rows / per_stream_rows)`` with
    ``live_bytes ~= heap_bytes * (1 - dead_tuple_ratio)`` (see also
    ``pg_cyan/cost_model.py``). When the extended metrics fields are absent the
    cost collapses to ``heap_bytes`` and the assignment matches plain heap-bytes
    packing. Disjoint ``include-only-table`` lists are the whole safety basis of
    the shared-nothing shards -- no table appears in two shards, enforced and
    re-validated before anything is written.
  * **LOB abort gate:** if the source has any large objects (``lob_count > 0``),
    exit non-zero with a clear message and write *no plan* -- pgcopydb pgoutput
    CDC does not replicate large objects (see
    ``notes/PostgreSQL Logical Replication Limitations & Workarounds for LOBs.md``).
  * Record ``snapshot.mode``, publication name, and slot name for downstream steps.
  * **Concurrency budget:** when capacity artifacts are present, derive the
    parallel-stream budget ``C*`` and from it ``shard_count`` + per-shard
    ``table_jobs`` (see ``pg_cyan/capacity.py``); the per-stream throughput it
    finds also calibrates the copy-time cost model. With no capacity data the
    shard count falls back to ``shards`` (default 4, clamped to the table count).
  * **Adaptive splitting:** a table whose estimated copy time exceeds the ideal
    per-stream share (``total_cost / C*``) is marked for within-shard range
    splitting -- the shard's ``table_jobs`` and ``split_tables_larger_than`` are
    set so pgcopydb carves it into parallel ctid ranges. An un-splittable giant
    stays whole and is reported as the ``single-table`` bottleneck (see
    ``pg_cyan/splitting.py``).
  * **Feasibility gate:** the predicted makespan is compared to the
    WAL-retention deadline and maintenance window; an infeasible plan is refused
    like the LOB gate (no plan, reasons in the error report). A ``resume`` block
    of circuit-breaker thresholds + shard checkpoints is emitted for the
    controller (see ``pg_cyan/feasibility.py``).

Deferred to follow-up work:
  * **FK graph** is emitted as ``"status": "deferred"`` -- FK edges are not yet
    collected by any artifact, and foreign-keys applies all constraints in one
    non-sharded pass regardless. Real FK collection is a follow-up.
  * **Cross-shard range-splitting** (the same table's rows spread over several
    shards) needs per-range row filters the ``--filters`` format cannot express;
    splitting stays *within* a shard's job. Emitting explicit range boundaries
    (vs. the size threshold pgcopydb splits on) is likewise deferred.

Follows the ``PgCyanParameters`` / ``PgCyanMetrics`` orchestrator pattern (see
``CLAUDE.md``): structured JSON logging via ``_log_event`` and graceful
degradation -- ``plan()`` never raises. On success a non-empty ``plan.json`` and
the shard filter files are written; on any failure (missing/invalid input, LOB
gate, empty table set, or write error) a ``plan-errors.json`` with an error
message is written instead and no plan files are produced.
"""
import dataclasses
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

from pg_cyan import capacity, cost_model, feasibility, splitting

# Structured JSON logging (mirrors the other stages).
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s"
)
logger = logging.getLogger(__name__)


class PgCyanPlanner:
    """Turns the collected source artifacts into the sharded migration plan."""

    #: Written on the happy path (non-empty).
    OUTPUT_FILENAME = "plan.json"
    #: Written when anything goes wrong, with an error message.
    ERROR_FILENAME = "plan-errors.json"

    #: Input artifact filenames (produced by the pre-migration stages).
    METRICS_FILENAME = "source-pg-metrics.json"
    LOBS_FILENAME = "source-pg-largeobjects.json"
    PARAMETERS_FILENAME = "pg-source-parameters.json"
    CONSTRAINTS_FILENAME = "constraints.json"
    #: Optional capacity artifacts consumed to size the concurrency budget.
    #: See also: pg_cyan/capacity.py.
    SOURCE_CAPACITY_FILENAME = "source-capacity.json"
    TARGET_CAPACITY_FILENAME = "target-capacity.json"
    NETWORK_FILENAME = "network.json"
    CLUSTER_CAPACITY_FILENAME = "cluster-capacity.json"

    #: Fixed default; smarter sizing is deferred.
    DEFAULT_SHARDS = 4
    #: pgcopydb --table-jobs ceiling per shard.
    DEFAULT_TABLE_JOBS = 8
    #: pgcopydb --index-jobs per index shard (target-side; kept thin).
    DEFAULT_INDEX_JOBS = 4
    DEFAULT_SPLIT_THRESHOLD = "50GB"
    DEFAULT_SNAPSHOT_MODE = "coordinator-held"
    DEFAULT_PUBLICATION = "pgcyan_pub"
    DEFAULT_SLOT = "pgcyan_slot"
    #: Per-shard pod resources. the copy step's COPY is network-bound, so requests are
    #: kept small enough to co-schedule with the controller on a single small
    #: node (``*_request``), while limits stay generous so the copy is not CPU-
    #: throttled. ``cpu``/``memory`` are the limits; ``cpu_request``/
    #: ``memory_request`` the requests (see ``orchestrator._data_shard_substitutions``).
    DEFAULT_SHARD_RESOURCES = {
        "cpu": "1", "cpu_request": "100m",
        "memory": "1Gi", "memory_request": "256Mi",
    }

    def __init__(
        self,
        inputs_dir: str = ".",
        output_dir: str = ".",
        shards: int = DEFAULT_SHARDS,
        split_tables_larger_than: str = DEFAULT_SPLIT_THRESHOLD,
        snapshot_mode: str = DEFAULT_SNAPSHOT_MODE,
        publication_name: str = DEFAULT_PUBLICATION,
        slot_name: str = DEFAULT_SLOT,
        run_id: Optional[str] = None,
        cost_model_: Optional[cost_model.CostModel] = None,
    ):
        """
        Initialize the planner.

        Args:
            inputs_dir: Directory holding the collected source artifacts.
            output_dir: Directory the plan/filters (or error) files are written to.
            shards: Desired number of copy shards (clamped to the table count).
            split_tables_larger_than: pgcopydb within-job split threshold recorded per shard.
            snapshot_mode: Snapshot strategy (e.g. ``coordinator-held``/``frozen-source``).
            publication_name: Logical-replication publication name for downstream steps.
            slot_name: Replication slot name for downstream steps.
            run_id: Opaque run identifier; generated from the clock if omitted.
            cost_model_: Time-based per-table cost model used to weight the
                bin-packer; defaults to ``cost_model.DEFAULT``.
        """
        self.inputs_dir = inputs_dir
        self.output_dir = output_dir
        self.shards = shards
        self.cost_model = cost_model_ or cost_model.DEFAULT
        self.split_tables_larger_than = split_tables_larger_than
        self.snapshot_mode = snapshot_mode
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.run_id = run_id or "pgcyan-" + datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.errors: List[str] = []

    def _log_event(
        self,
        event_type: str,
        status: str,
        message: str,
        details: Optional[Dict] = None,
    ) -> None:
        """Log a structured JSON event (mirrors the other stages)."""
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "planner",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }
        logger.info(json.dumps(event))

    # ------------------------------------------------------------------ inputs

    def _read_json(self, filename: str, required: bool) -> Optional[Dict]:
        """
        Load a JSON artifact from ``inputs_dir``.

        Returns the parsed object, or ``None`` if it is missing/invalid. A
        ``required`` input that cannot be read records an error; an optional one
        is skipped silently. Never raises to the caller.
        """
        path = os.path.join(self.inputs_dir, filename)
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except FileNotFoundError:
            if required:
                self._log_event(
                    "read_input",
                    "failure",
                    f"Required input {filename} not found",
                    {"path": path, "remediation": "Run the pre-migration stage that produces it first"},
                )
                self.errors.append(f"Missing required input: {filename}")
            return None
        except (OSError, ValueError) as e:
            # ValueError covers json.JSONDecodeError (malformed artifact).
            if required:
                self._log_event(
                    "read_input",
                    "failure",
                    f"Could not read input {filename}",
                    {"path": path, "error": str(e)},
                )
                self.errors.append(f"Invalid input {filename}: {str(e)}")
            return None

    def check_lob_gate(self, lobs: Dict) -> bool:
        """
        LOB abort gate: fail if the source has any large objects.

        pgcopydb's pgoutput CDC does not replicate large objects, so a source
        with any is unsafe for this pipeline until the dedicated LOB ticket
        lands. Returns ``True`` only when the count is a definite zero.
        """
        count = lobs.get("lob_count")
        if count is None:
            self._log_event(
                "lob_gate",
                "failure",
                "Large-object count missing from source-pg-largeobjects.json",
                {"remediation": "Re-run `pg_cyan.cli source-largeobjects` to regenerate the artifact"},
            )
            self.errors.append("Cannot verify LOB safety: lob_count missing from input")
            return False
        if count > 0:
            self._log_event(
                "lob_gate",
                "failure",
                f"Source has {count} large object(s); aborting -- LOBs are not replicated by pgcopydb CDC",
                {
                    "lob_count": count,
                    "remediation": (
                        "Logical replication does not replicate large objects "
                        "(pg_largeobject / lo_*). Migrate them via the separate "
                        "LOB pipeline before planning; see notes/PostgreSQL "
                        "Logical Replication Limitations & Workarounds for LOBs.md"
                    ),
                },
            )
            self.errors.append(
                f"LOB abort gate: source has {count} large object(s); "
                "pgcopydb CDC does not replicate large objects"
            )
            return False
        self._log_event(
            "lob_gate",
            "success",
            "No large objects on source; safe to plan",
            {"lob_count": count},
        )
        return True

    # ------------------------------------------------------------- bin-packing

    @staticmethod
    def _table_key(table: Dict) -> str:
        """Schema-qualified table name -- the disjointness key and filter entry."""
        return f'{table["schema"]}.{table["name"]}'

    @staticmethod
    def _heap_bytes(table: Dict) -> int:
        return int(table.get("heap_bytes") or 0)

    def _cost(self, table: Dict) -> float:
        """Estimated single-stream copy time (s) -- the bin-packing weight."""
        return self.cost_model.estimate_seconds(table)

    def bin_pack(self, tables: List[Dict], n: int) -> List[List[Dict]]:
        """
        LPT-greedy bin-pack ``tables`` into ``n`` shards balanced by copy time.

        The weight is each table's estimated single-stream copy time
        (``cost_model``), not raw heap bytes -- a bloated or narrow-hot table no
        longer distorts the balance. Costliest table first, each assigned to the
        currently-lightest shard. Every table lands in exactly one shard, so the
        shards partition the table set. When the extended metrics fields are
        absent the cost is proportional to ``heap_bytes``, so the packing matches
        plain heap-bytes balancing.
        """
        shards: List[List[Dict]] = [[] for _ in range(n)]
        loads = [0.0] * n
        for table in sorted(tables, key=self._cost, reverse=True):
            lightest = loads.index(min(loads))
            shards[lightest].append(table)
            loads[lightest] += self._cost(table)
        return shards

    def _validate_disjoint(self, shards: List[List[Dict]], tables: List[Dict]) -> bool:
        """
        Prove the shards are a partition of the table set.

        No table key may appear twice (disjoint), and the union must cover every
        input table (complete). This is the safety basis of shared-nothing
        shards, so it is re-checked defensively before anything is written.
        """
        seen: set = set()
        for shard in shards:
            for table in shard:
                key = self._table_key(table)
                if key in seen:
                    self._log_event(
                        "validate_shards",
                        "failure",
                        f"Table {key} assigned to more than one shard",
                        {"table": key},
                    )
                    self.errors.append(f"Shard overlap: {key} appears in multiple shards")
                    return False
                seen.add(key)

        expected = {self._table_key(t) for t in tables}
        if seen != expected:
            missing = expected - seen
            self._log_event(
                "validate_shards",
                "failure",
                "Shard assignment does not cover every table",
                {"missing_count": len(missing), "missing_sample": sorted(missing)[:5]},
            )
            self.errors.append(f"Shard coverage incomplete: {len(missing)} table(s) unassigned")
            return False
        return True

    # --------------------------------------------------------------- assembly

    def _filters_body(self, shard_tables: List[Dict]) -> str:
        """Render one pgcopydb ``--filters`` file body for a shard."""
        lines = ["[include-only-table]"]
        lines.extend(sorted(self._table_key(t) for t in shard_tables))
        return "\n".join(lines) + "\n"

    def _apply_splits(
        self, shard_tables: List[Dict], subtasks_by_table: Dict[str, int],
        table_jobs: int, max_table_jobs: int,
    ) -> tuple:
        """
        Refine a shard's ``table_jobs`` + split threshold for its split tables.

        For shards holding a table the planner decided to split, raise
        ``table_jobs`` enough to run its ranges in parallel and lower
        ``split_tables_larger_than`` below the split tables so pgcopydb carves
        them. Shards with no split table keep the configured defaults. See also:
        pg_cyan/splitting.py.
        """
        subtasks = [subtasks_by_table[k]
                    for t in shard_tables
                    if (k := self._table_key(t)) in subtasks_by_table]
        if not subtasks:
            return table_jobs, self.split_tables_larger_than

        jobs = min(max_table_jobs, max(table_jobs, max(subtasks)))
        # Size-triggered ctid split needs the threshold below the split tables.
        # Only positive heaps can be size-split; a zero-heap (row-bound) table
        # keeps the default -- pgcopydb cannot size-split it.
        heaps = [self._heap_bytes(t) for t in shard_tables
                 if self._table_key(t) in subtasks_by_table and self._heap_bytes(t) > 0]
        threshold = max(1, int(min(heaps) * 0.9)) if heaps else self.split_tables_larger_than
        return jobs, threshold

    def build_plan(
        self, shards: List[List[Dict]], budget: capacity.Budget,
        split_plan: splitting.SplitPlan, assessment: feasibility.Assessment,
    ) -> Dict:
        """Assemble the ``plan.json`` payload from the bin-packed shards."""
        not_consistent = self.snapshot_mode != self.DEFAULT_SNAPSHOT_MODE
        subtasks_by_table = split_plan.subtasks_by_table()

        data_shards = []
        index_shards = []
        for i, shard_tables in enumerate(shards):
            filter_file = f"shard-{i}.filters"
            heap_bytes = sum(self._heap_bytes(t) for t in shard_tables)
            index_bytes = sum(int(t.get("index_bytes") or 0) for t in shard_tables)
            est_copy_seconds = sum(self._cost(t) for t in shard_tables)
            # A capacity budget sets table_jobs from C*; with no capacity data
            # keep the static one-worker-per-table (capped) sizing.
            if budget.degraded:
                table_jobs = max(1, min(len(shard_tables), self.DEFAULT_TABLE_JOBS))
            else:
                table_jobs = budget.table_jobs
            table_jobs, split_threshold = self._apply_splits(
                shard_tables, subtasks_by_table, table_jobs, budget.max_table_jobs)
            data_shards.append({
                "name": f"shard-{i}",
                "filter_file": filter_file,
                "table_count": len(shard_tables),
                "heap_bytes": heap_bytes,
                "index_bytes": index_bytes,
                # Estimated serial single-stream copy time for this shard's
                # tables -- the balancing weight, not this shard's makespan
                # (splitting + concurrency shorten the wall-clock; see splitting).
                "est_copy_seconds": round(est_copy_seconds, 2),
                "table_jobs": table_jobs,
                "split_tables_larger_than": split_threshold,
                "resources": dict(self.DEFAULT_SHARD_RESOURCES),
            })
            index_shards.append({
                "name": f"idx-{i}",
                "filter_file": filter_file,
                "index_jobs": self.DEFAULT_INDEX_JOBS,
            })

        total_heap = sum(s["heap_bytes"] for s in data_shards)
        total_index = sum(s["index_bytes"] for s in data_shards)
        total_cost = sum(s["est_copy_seconds"] for s in data_shards)
        # An indivisible giant that floors the makespan is the bottleneck the
        # operator must act on; otherwise it is whatever bound the stream budget.
        if split_plan.single_table_floor:
            bottleneck = "single-table"
        elif budget.degraded:
            bottleneck = "none"
        else:
            bottleneck = budget.binding_term
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "planner",
            "status": "success",
            "run_id": self.run_id,
            "snapshot": {"mode": self.snapshot_mode, "not_consistent": not_consistent},
            "publication": {"name": self.publication_name},
            "slot": {"name": self.slot_name},
            "totals": {
                "table_count": sum(s["table_count"] for s in data_shards),
                "shard_count": len(data_shards),
                "heap_bytes": total_heap,
                "index_bytes": total_index,
                "est_copy_seconds": round(total_cost, 2),
            },
            "concurrency": budget.as_plan_block(),
            "splitting": split_plan.as_plan_block(),
            "feasibility": assessment.feasibility_block(),
            "resume": assessment.resume_block(),
            "bottleneck": bottleneck,
            "cost_model": {
                "weight": "estimated_copy_seconds",
                "constants": self.cost_model.as_dict(),
                "note": (
                    "Shards balanced by estimated single-stream copy time: "
                    "t(table) = max(live_bytes/per_stream_bytes_per_sec, "
                    "rows/per_stream_rows_per_sec), live_bytes = "
                    "heap_bytes*(1-dead_tuple_ratio). Constants are calibratable; "
                    "when the extended metrics fields are absent the weight "
                    "degrades to heap_bytes. Concurrency budget and makespan "
                    "prediction are not modelled yet."
                ),
            },
            "data_shards": data_shards,
            "index_shards": index_shards,
            "fk_graph": {
                "status": "deferred",
                "edges": [],
                "note": (
                    "FK edge collection is not yet implemented. "
                    "Step-3b applies all constraints in one "
                    "non-sharded pass regardless of this graph."
                ),
            },
            "notes": (
                "Shards bin-packed by estimated copy time (LPT). A table costlier "
                "than the ideal per-stream share is split within its shard's job "
                "via split_tables_larger_than (see splitting); cross-shard "
                "range-splitting is out of scope. Index/FK compute runs "
                "target-side, so index/fk pods are thin -- scale target "
                "index_jobs + maintenance_work_mem instead."
            ),
        }

    def _build_error(self) -> Dict:
        """Assemble the payload written to ``plan-errors.json``."""
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "planner",
            "status": "failure",
            "message": "Failed to produce a migration plan",
            "errors": self.errors or ["Unknown error"],
            "remediation": "Check the inputs directory, the LOB abort gate, and the messages above",
        }

    def _write_text(self, filename: str, content: str) -> Optional[str]:
        """Write ``content`` to ``output_dir/filename``; return path or ``None``."""
        path = os.path.join(self.output_dir, filename)
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(content)
            return path
        except OSError as e:
            self._log_event(
                "write_output",
                "failure",
                f"Failed to write {filename}",
                {"path": path, "error": str(e)},
            )
            self.errors.append(f"Failed to write {filename}: {str(e)}")
            return None

    def _write_json(self, filename: str, payload: Dict) -> Optional[str]:
        """Write ``payload`` as pretty JSON to ``output_dir/filename``."""
        return self._write_text(filename, json.dumps(payload, indent=2, default=str) + "\n")

    # ------------------------------------------------------------- orchestrate

    def _resolve_budget(self, capacity_inputs: Dict, table_count: int) -> capacity.Budget:
        """
        Compute the concurrency budget and calibrate the cost model from it.

        A network probe gives a real per-stream throughput, so when one is
        present it replaces the cost model's default -- shard balance and the
        stream budget then rest on the same number. See also: pg_cyan/capacity.py.
        """
        budget = capacity.compute_budget(
            constraints=capacity_inputs["constraints"],
            source_capacity=capacity_inputs["source_capacity"],
            target_capacity=capacity_inputs["target_capacity"],
            network=capacity_inputs["network"],
            cluster_capacity=capacity_inputs["cluster_capacity"],
            table_count=table_count,
            fallback_shards=self.shards,
            fallback_table_jobs=self.DEFAULT_TABLE_JOBS,
        )
        if capacity_inputs["network"]:
            self.cost_model = dataclasses.replace(
                self.cost_model,
                per_stream_bytes_per_sec=budget.per_stream_bytes_per_sec,
            )
        self._log_event(
            "concurrency_budget",
            "warning" if budget.degraded else "success",
            f"C*={budget.c_star} bound by {budget.binding_term}; "
            f"{budget.shard_count} shard(s) x {budget.table_jobs} table_jobs",
            budget.as_plan_block(),
        )
        return budget

    def _assess_feasibility(
        self, split_plan: splitting.SplitPlan, capacity_inputs: Dict,
        shards: List[List[Dict]],
    ) -> feasibility.Assessment:
        """
        Gate the predicted makespan against the WAL/window deadlines.

        On ``abort`` the reasons are recorded to ``self.errors`` so the failure
        path writes them (with the numbers) and no plan is produced -- the same
        refusal contract as the LOB gate. See also: pg_cyan/feasibility.py.
        """
        assessment = feasibility.assess(
            split_plan.predicted_makespan_seconds,
            source_capacity=capacity_inputs["source_capacity"],
            constraints=capacity_inputs["constraints"],
            shard_names=[f"shard-{i}" for i in range(len(shards))],
        )
        status_map = {"abort": "failure", "warn": "warning"}
        self._log_event(
            "feasibility",
            status_map.get(assessment.status, "success"),
            f"Feasibility {assessment.status}: " + "; ".join(assessment.reasons),
            assessment.feasibility_block(),
        )
        if assessment.aborted:
            for reason in assessment.reasons:
                self.errors.append(f"Feasibility abort: {reason}")
        return assessment

    def plan(self) -> Dict:
        """
        Load inputs, run the LOB gate + bin-packer, and write plan/filters.

        Never raises. Returns a summary dict:
            {
              "status": "success" | "failure",
              "output_file": <plan.json path or None>,
              "error_file": <plan-errors.json path or None>,
              "shard_files": [<paths>],
              "shard_count": <int>,
              "table_count": <int>,
              "errors": [...],
            }
        """
        summary = {
            "status": "failure",
            "output_file": None,
            "error_file": None,
            "shard_files": [],
            "shard_count": 0,
            "table_count": 0,
            "errors": self.errors,
        }

        try:
            metrics = self._read_json(self.METRICS_FILENAME, required=True)
            lobs = self._read_json(self.LOBS_FILENAME, required=True)
            constraints = self._read_json(self.CONSTRAINTS_FILENAME, required=False)
            self._read_json(self.PARAMETERS_FILENAME, required=False)
            capacity_inputs = {
                "constraints": constraints,
                "source_capacity": self._read_json(self.SOURCE_CAPACITY_FILENAME, required=False),
                "target_capacity": self._read_json(self.TARGET_CAPACITY_FILENAME, required=False),
                "network": self._read_json(self.NETWORK_FILENAME, required=False),
                "cluster_capacity": self._read_json(self.CLUSTER_CAPACITY_FILENAME, required=False),
            }

            if metrics is not None and lobs is not None:
                # LOB abort gate runs before any planning work.
                if self.check_lob_gate(lobs):
                    tables = metrics.get("tables") or []
                    if not tables:
                        self._log_event(
                            "plan",
                            "failure",
                            "No tables in source-pg-metrics.json to plan",
                            {"remediation": "Confirm the metrics artifact covers user tables"},
                        )
                        self.errors.append("No tables to plan")
                    else:
                        budget = self._resolve_budget(capacity_inputs, len(tables))
                        split_plan = splitting.plan_splits(
                            tables,
                            c_star=budget.c_star,
                            max_table_jobs=budget.max_table_jobs,
                            cost_model=self.cost_model,
                        )
                        shards = self.bin_pack(tables, budget.shard_count)
                        if self._validate_disjoint(shards, tables):
                            assessment = self._assess_feasibility(
                                split_plan, capacity_inputs, shards)
                            # An infeasible plan is refused like the LOB gate: no
                            # plan written, reasons recorded to the error report.
                            if not assessment.aborted:
                                ok = self._write_plan_files(
                                    shards, summary, len(tables), budget,
                                    split_plan, assessment)
                                if ok:
                                    return summary
        except Exception as e:
            # Malformed artifact (bad types, missing keys) -> degrade, never raise.
            self._log_event(
                "plan",
                "failure",
                "Unexpected error while planning",
                {"error": str(e), "remediation": "Check the input artifacts are well-formed"},
            )
            self.errors.append(f"Planning error: {str(e)}")

        # Failure path: missing input, LOB gate, empty tables, or write failure.
        error_path = self._write_json(self.ERROR_FILENAME, self._build_error())
        summary.update(status="failure", error_file=error_path)
        self._log_event(
            "plan",
            "failure",
            f"Wrote error report to {self.ERROR_FILENAME}",
            {"path": error_path, "errors": self.errors},
        )
        return summary

    def _write_plan_files(
        self, shards: List[List[Dict]], summary: Dict, table_count: int,
        budget: capacity.Budget, split_plan: splitting.SplitPlan,
        assessment: feasibility.Assessment,
    ) -> bool:
        """Write plan.json + every shard-<n>.filters; update ``summary`` in place."""
        plan_path = self._write_json(
            self.OUTPUT_FILENAME, self.build_plan(shards, budget, split_plan, assessment))
        if not plan_path:
            return False

        shard_files = []
        for i, shard_tables in enumerate(shards):
            shard_path = self._write_text(f"shard-{i}.filters", self._filters_body(shard_tables))
            if not shard_path:
                return False
            shard_files.append(shard_path)

        summary.update(
            status="success",
            output_file=plan_path,
            shard_files=shard_files,
            shard_count=len(shards),
            table_count=table_count,
        )
        self._log_event(
            "plan",
            "success",
            f"Wrote {self.OUTPUT_FILENAME} and {len(shard_files)} shard filter file(s)",
            {"path": plan_path, "shard_count": len(shards), "table_count": table_count},
        )
        return True


def plan_from_env() -> int:
    """
    Produce a migration plan using environment-driven defaults.

    Returns:
        0 if the plan was written, 1 otherwise.
    """
    try:
        planner = PgCyanPlanner(
            inputs_dir=os.getenv("PG_INPUTS_DIR", "."),
            output_dir=os.getenv("PG_OUTPUT_DIR", "."),
            shards=int(os.getenv("PGCYAN_SHARDS", str(PgCyanPlanner.DEFAULT_SHARDS))),
            split_tables_larger_than=os.getenv(
                "PGCYAN_SPLIT_TABLES_LARGER_THAN", PgCyanPlanner.DEFAULT_SPLIT_THRESHOLD),
            snapshot_mode=os.getenv("PGCYAN_SNAPSHOT_MODE", PgCyanPlanner.DEFAULT_SNAPSHOT_MODE),
            publication_name=os.getenv("PGCYAN_PUBLICATION", PgCyanPlanner.DEFAULT_PUBLICATION),
            slot_name=os.getenv("PGCYAN_SLOT", PgCyanPlanner.DEFAULT_SLOT),
            run_id=os.getenv("PGCYAN_RUN_ID"),
        )
        summary = planner.plan()
        print(json.dumps(summary, indent=2, default=str))
        return 0 if summary["status"] == "success" else 1
    except KeyboardInterrupt:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "interrupted",
            "message": "Planning interrupted by user",
        }))
        return 2
    except Exception as e:
        logger.error(json.dumps({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "failure",
            "message": "Fatal error during planning",
            "error": str(e),
        }))
        return 1


if __name__ == "__main__":
    sys.exit(plan_from_env())
