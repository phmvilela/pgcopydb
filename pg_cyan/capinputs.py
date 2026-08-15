"""
Static capacity-input provider: turn an operator-declared bundle into planner artifacts.

Before any automated collector exists, an operator can still drive the
capacity-aware planner by *declaring* their environment. This stage reads a single
declared bundle (``capacity-declared.json`` by default) -- one JSON object keyed by
``source_capacity`` / ``target_capacity`` / ``network`` / ``cluster_capacity`` /
``copylog_history`` / ``constraints`` -- validates each sub-object against the
shared contract (``pg_cyan/capacity_contract.py``), and writes the valid ones out
as the canonical artifact files the planner reads from its inputs dir. A later
collector automates any one input by writing the same artifact; the control-plane
API accepts the same bundle over ``POST /migrations`` and validates it with the
same contract, so the three routes cannot diverge.

The bundle *is* the operator-facing wire format; this provider is its file-based
transport. Absent or invalid inputs degrade -- a missing bundle is a no-op (the
planner keeps its static fallbacks), an invalid sub-object is skipped with its
errors recorded while the valid siblings are still written. ``provide()`` follows
the success-vs-error two-file convention and never raises.

See also: pg_cyan/capacity_contract.py (the schemas + validator),
pg_cyan/planner.py (consumes the artifacts), pg_cyan/capacity.py.
"""
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from pg_cyan import capacity_contract

logger = logging.getLogger(__name__)


class PgCyanCapacityInputs:
    """Explodes an operator-declared capacity bundle into validated planner artifacts."""

    #: The operator-authored bundle read from ``inputs_dir``.
    DECLARED_FILENAME = "capacity-declared.json"
    #: Manifest of what was written (success) or why nothing could be (failure).
    OUTPUT_FILENAME = "capacity-inputs.json"
    ERROR_FILENAME = "capacity-inputs-errors.json"

    def __init__(
        self,
        inputs_dir: str = ".",
        output_dir: str = ".",
        declared_filename: str = DECLARED_FILENAME,
    ):
        """
        Args:
            inputs_dir: Directory holding the declared bundle.
            output_dir: Directory the artifact files (or the error file) are written to.
            declared_filename: The declared-bundle filename to read.
        """
        self.inputs_dir = inputs_dir
        self.output_dir = output_dir
        self.declared_filename = declared_filename
        self.errors: List[str] = []

    def _log_event(
        self, event_type: str, status: str, message: str,
        details: Optional[Dict] = None,
    ) -> None:
        """Log a structured JSON event (mirrors the other stages)."""
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "capacity-inputs",
            "event_type": event_type,
            "status": status,
            "message": message,
            "details": details or {},
        }
        logger.info(json.dumps(event))

    def _write_json(self, filename: str, payload: Dict) -> str:
        """Write ``payload`` as JSON into ``output_dir``; return the path."""
        path = os.path.join(self.output_dir, filename)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        return path

    def _read_declared(self) -> tuple:
        """
        Load the declared bundle.

        Returns ``(bundle, present)``: ``bundle`` is the parsed object (``None`` on
        a missing or malformed file), ``present`` is whether the file existed at
        all (so a missing file can degrade quietly while a malformed one fails).
        """
        path = os.path.join(self.inputs_dir, self.declared_filename)
        if not os.path.exists(path):
            return None, False
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh), True
        except (OSError, ValueError) as e:
            self._log_event(
                "read_declared", "failure",
                f"Declared bundle {self.declared_filename} could not be read",
                {"path": path, "error": str(e),
                 "remediation": "Ensure the bundle is a well-formed JSON object"},
            )
            self.errors.append(f"Unreadable declared bundle: {e}")
            return None, True

    def _error_summary(self) -> Dict:
        """Assemble the payload written to ``capacity-inputs-errors.json``."""
        error_path = self._write_json(self.ERROR_FILENAME, {
            "component": "capacity-inputs",
            "status": "failure",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "errors": self.errors,
        })
        return {
            "status": "failure",
            "component": "capacity-inputs",
            "written": [],
            "skipped": {},
            "output_file": None,
            "error_file": error_path,
            "errors": list(self.errors),
        }

    def provide(self) -> Dict:
        """
        Read, validate, and explode the declared bundle into planner artifacts.

        Returns a summary dict (``status`` ∈ ``success``/``warning``/``failure``).
        Writes each valid artifact into ``output_dir``, plus a manifest
        (``capacity-inputs.json``) or, on hard failure, ``capacity-inputs-errors.json``.
        Never raises.
        """
        bundle, present = self._read_declared()

        if not present:
            # No declared inputs at all: a clean no-op. The planner falls back to
            # its static defaults; this is not a failure.
            self._log_event(
                "provide", "success",
                "No declared capacity bundle; planner will use static fallbacks",
                {"declared": self.declared_filename},
            )
            output_path = self._write_json(self.OUTPUT_FILENAME, {
                "component": "capacity-inputs",
                "status": "success",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "contract_version": capacity_contract.CONTRACT_VERSION,
                "written": [],
                "skipped": {},
                "note": "No declared bundle found; no capacity artifacts written.",
            })
            return {
                "status": "success", "component": "capacity-inputs",
                "written": [], "skipped": {},
                "output_file": output_path, "error_file": None, "errors": [],
            }

        if bundle is None:
            return self._error_summary()

        if not isinstance(bundle, dict):
            self._log_event(
                "validate", "failure",
                "Declared bundle is not a JSON object keyed by artifact",
                {"remediation": "Provide an object like {\"source_capacity\": {...}}"},
            )
            self.errors.append("Declared bundle must be a JSON object keyed by artifact")
            return self._error_summary()

        results = capacity_contract.validate_bundle(bundle)
        written: List[str] = []
        skipped: Dict[str, List[str]] = {}
        for key, value in bundle.items():
            filename = capacity_contract.BUNDLE_KEY_TO_FILENAME.get(key)
            if filename is None:
                skipped[key] = [f"unknown bundle key: {key}"]
                continue
            errs = results.get(filename, [])
            if errs:
                skipped[filename] = errs
                self._log_event(
                    "validate", "warning",
                    f"Skipped invalid declared artifact {filename}",
                    {"errors": errs},
                )
                continue
            self._write_json(filename, capacity_contract.stamp(value)
                             if isinstance(value, dict) else value)
            written.append(filename)

        # Nothing usable came out of a non-empty declaration -> a hard failure, so
        # the operator is not misled into thinking the plan is capacity-aware.
        if not written and skipped:
            self.errors.append(
                "No declared capacity artifact was valid: " + "; ".join(
                    f"{name}: {', '.join(errs)}" for name, errs in skipped.items()))
            return self._error_summary()

        status = "warning" if skipped else "success"
        self._log_event(
            "provide", status,
            f"Wrote {len(written)} capacity artifact(s) from the declared bundle",
            {"written": written, "skipped": sorted(skipped)},
        )
        output_path = self._write_json(self.OUTPUT_FILENAME, {
            "component": "capacity-inputs",
            "status": status,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "contract_version": capacity_contract.CONTRACT_VERSION,
            "written": written,
            "skipped": skipped,
            "note": ("Valid declared artifacts written for the planner; any skipped "
                     "entry failed contract validation and was left out so the "
                     "planner degrades that term rather than trusting bad input."),
        })
        return {
            "status": status, "component": "capacity-inputs",
            "written": written, "skipped": skipped,
            "output_file": output_path, "error_file": None,
            "errors": list(self.errors),
        }
