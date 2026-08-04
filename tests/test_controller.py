"""
Unit tests for the folded migration controller (`pg_cyan.cli run-migration`).

See also: pg_cyan/controller.py

Cluster-free and process-free: the snapshot sub-component and the orchestrator
are both injected as fakes, so nothing spawns pgcopydb or touches a cluster.
Tests exercise the compose wiring (establish -> resolve owner -> walk with the id
injected + owned + health-checked), the always-release-the-snapshot teardown, and
the graceful-degradation branches (establish failure, no StatefulSet owner).
"""
import pytest
from kubernetes.client.rest import ApiException

from pg_cyan.controller import PgCyanController


# ------------------------------------------------------------------- fakes

class _Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FakeProc:
    def __init__(self, alive=True):
        self._alive = alive

    def poll(self):
        return None if self._alive else 0


class _FakeHolder:
    def __init__(self, status="success", snapshot="SNAP-1", proc_alive=True):
        self._summary = {"status": status, "snapshot_name": snapshot,
                         "slot": "pgcyan_slot", "publication": "pgcyan_pub"}
        self.proc = _FakeProc(proc_alive)
        # The controller hands these to the orchestrator for the CDC phase.
        self.slot_name = "pgcyan_slot"
        self.plugin = "pgoutput"
        self.established = False
        self.closed = False

    def establish(self):
        self.established = True
        return self._summary

    def close(self):
        self.closed = True


class _FakeOrchestrator:
    def __init__(self, result=None, **kwargs):
        self.kwargs = kwargs
        self._result = result or {"status": "success",
                                  "phases": [{"phase": "2", "success": True}], "errors": []}

    def run(self):
        return self._result


class _FakeCore:
    """Fake CoreV1Api: owner-ref resolution + the run-state ConfigMap (claim/patch)."""

    def __init__(self, owner_kind="StatefulSet", state_exists=False, state_status="running"):
        self._owner_kind = owner_kind
        # The pgcyan-migration-state marker: seed it to simulate a prior run
        # (restart) that the fresh pod must refuse to resume.
        self.state = {"status": state_status} if state_exists else None

    def read_namespaced_pod(self, name, namespace):
        ref = _Obj(api_version="apps/v1", kind=self._owner_kind,
                   name="pgcyan-controller", uid="u-1", controller=True)
        return _Obj(metadata=_Obj(owner_references=[ref]))

    def create_namespaced_config_map(self, namespace, body):
        if self.state is not None:
            raise ApiException(status=409, reason="AlreadyExists")   # already claimed
        self.state = dict(body.data)

    def read_namespaced_config_map(self, name, namespace):
        if self.state is None:
            raise ApiException(status=404, reason="NotFound")
        return _Obj(data=self.state)

    def patch_namespaced_config_map(self, name, namespace, body):
        self.state = {**(self.state or {}), **body["data"]}


class _FakeFollow:
    """Records kwargs; start() succeeds unless slot is the 'bad' sentinel."""
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.closed = False

    def start(self):
        self.started = True
        ok = self.kwargs.get("slot_name") != "bad-slot"
        return {"status": "success" if ok else "failure",
                "slot": self.kwargs.get("slot_name"), "errors": [] if ok else ["boom"]}

    def supervise(self, stop):
        return 0

    def close(self):
        self.closed = True


def _make_controller(holder=None, core=None, orch_result=None, capture=None,
                     ready_file=None, follow_cdc=False, api_port=0):
    holder = holder or _FakeHolder()

    def factory(**kwargs):
        orch = _FakeOrchestrator(result=orch_result, **kwargs)
        if capture is not None:
            capture["orch"] = orch
        return orch

    def follow_factory(**kwargs):
        cdc = _FakeFollow(**kwargs)
        if capture is not None:
            capture["cdc"] = cdc
        return cdc

    return PgCyanController(
        holder=holder, namespace="pgcyan-migration", pod_name="pgcyan-controller-0",
        core_api=core or _FakeCore(), orchestrator_factory=factory,
        follow_factory=follow_factory, follow_cdc=follow_cdc, api_port=api_port,
        ready_file=ready_file or "/tmp/pgcyan-test-ready-unused",
    )


# --------------------------------------------------------------------- tests

def test_run_happy_path_establishes_walks_and_releases(tmp_path):
    holder = _FakeHolder()
    capture = {}
    core = _FakeCore()
    ready = tmp_path / "ready"
    ctl = _make_controller(holder=holder, core=core, capture=capture, ready_file=str(ready))
    summary = ctl.run()

    assert summary["status"] == "success"
    assert summary["ready"] is True
    assert summary["snapshot"] == "SNAP-1"
    assert summary["owned"] is True
    assert holder.established is True
    assert holder.closed is True                      # released in finally
    assert ready.exists()                             # readiness probe passes
    assert core.state["status"] == "succeeded"        # run-state marker updated
    # the orchestrator was built with the snapshot injected + owned + health-checked
    kw = capture["orch"].kwargs
    assert kw["snapshot_id"] == "SNAP-1"
    assert kw["owner_reference"]["kind"] == "StatefulSet"
    assert kw["owner_reference"]["uid"] == "u-1"
    assert callable(kw["health_check"])


def test_refuse_to_resume_when_state_marker_exists(tmp_path):
    # A prior run already claimed the state ConfigMap -> a restart must NOT
    # re-snapshot; it parks not-ready.
    holder = _FakeHolder()
    capture = {}
    core = _FakeCore(state_exists=True, state_status="running")
    ready = tmp_path / "ready"
    ready.write_text("stale")                         # a leftover ready file...
    ctl = _make_controller(holder=holder, core=core, capture=capture, ready_file=str(ready))
    summary = ctl.run()

    assert summary["status"] == "halted"
    assert summary["ready"] is False
    assert holder.established is False                 # never established a new snapshot
    assert "orch" not in capture                       # never walked
    assert not ready.exists()                          # ...removed -> not ready
    assert any("Refused to resume" in e for e in summary["errors"])


def test_walk_failure_clears_readiness(tmp_path):
    core = _FakeCore()
    ready = tmp_path / "ready"
    ctl = _make_controller(core=core, ready_file=str(ready),
                           orch_result={"status": "failure", "phases": [], "errors": ["boom"]})
    summary = ctl.run()
    assert summary["status"] == "failure"
    assert summary["ready"] is False
    assert not ready.exists()                          # failure -> not ready
    assert core.state["status"] == "failed"


def test_health_check_reflects_the_snapshot_child():
    holder = _FakeHolder(proc_alive=True)
    capture = {}
    ctl = _make_controller(holder=holder, capture=capture)
    ctl.run()
    health = capture["orch"].kwargs["health_check"]
    assert health() is True
    holder.proc = _FakeProc(alive=False)              # child dies
    assert health() is False


def test_establish_failure_aborts_before_walking():
    holder = _FakeHolder(status="failure")
    capture = {}
    ctl = _make_controller(holder=holder, capture=capture)
    summary = ctl.run()
    assert summary["status"] == "failure"
    assert "orch" not in capture                      # orchestrator never built
    assert holder.closed is True                      # child still released


def test_no_statefulset_owner_runs_unowned():
    ctl = _make_controller(core=_FakeCore(owner_kind="ReplicaSet"))
    summary = ctl.run()
    assert summary["owned"] is False
    assert summary["status"] == "success"             # still runs, just no cascade


def test_walk_failure_propagates_and_releases():
    holder = _FakeHolder()
    ctl = _make_controller(holder=holder,
                           orch_result={"status": "failure", "phases": [], "errors": ["boom"]})
    summary = ctl.run()
    assert summary["status"] == "failure"
    assert "boom" in summary["errors"]
    assert holder.closed is True


def test_stop_after_flows_to_orchestrator():
    capture = {}
    ctl = _make_controller(capture=capture)
    ctl.stop_after = "2"
    ctl.run()
    assert capture["orch"].kwargs["stop_after"] == "2"


def test_no_cdc_by_default_parks_after_walk():
    # follow_cdc defaults off: the controller parks Ready after the walk, no CDC.
    capture = {}
    ctl = _make_controller(capture=capture)   # follow_cdc=False
    summary = ctl.run()
    assert summary["status"] == "success" and summary["ready"] is True
    assert ctl.cdc is None
    assert "cdc" not in summary


def test_full_walk_starts_cdc_on_controller_dir(monkeypatch):
    # After a full successful walk, the controller releases the snapshot and
    # becomes the CDC consumer (streaming on its own --dir).
    capture = {}
    ctl = _make_controller(capture=capture, follow_cdc=True)
    summary = ctl.run()
    assert summary["status"] == "success" and summary["ready"] is True
    assert summary["cdc"] == "streaming"
    assert capture["cdc"].started is True
    # the slot the holder created is handed to the CDC follow (not the orchestrator)
    assert capture["cdc"].kwargs["slot_name"] == "pgcyan_slot"
    assert capture["cdc"].kwargs["plugin"] == "pgoutput"
    # the snapshot was released before CDC (freeing the slot)
    assert ctl._released is True
    # the orchestrator does not receive slot/release wiring
    assert "slot_name" not in capture["orch"].kwargs
    assert "release_snapshot" not in capture["orch"].kwargs


def test_cdc_start_failure_marks_not_ready(monkeypatch):
    # If CDC fails to attach, the controller is not Ready and the run is a failure.
    holder = _FakeHolder()
    holder.slot_name = "bad-slot"     # _FakeFollow.start() fails for this slot
    ctl = _make_controller(holder=holder, follow_cdc=True)
    summary = ctl.run()
    assert summary["status"] == "failure" and summary["ready"] is False
    assert summary["cdc"] == "failed"


def test_cdc_skipped_when_walk_stops_before_postdata():
    # Stopping the walk before post-data (stopped_after != last phase) => no CDC.
    capture = {}
    ctl = _make_controller(capture=capture, follow_cdc=True,
                           orch_result={"status": "success", "stopped_after": "2",
                                        "phases": [], "errors": []})
    summary = ctl.run()
    assert summary["status"] == "success"
    assert ctl.cdc is None                 # CDC not started (post-data not reached)


def test_release_snapshot_closes_holder_and_marks_healthy():
    # Releasing the snapshot ahead of CDC closes the holder, and from then on the
    # health-check reports healthy (its absence is by design, not failure).
    ctl = _make_controller()
    holder = ctl.holder
    assert ctl._snapshot_alive() is True          # child alive pre-release
    ctl._release_snapshot()
    assert holder.closed is True                   # holder torn down
    assert ctl._released is True
    assert ctl._snapshot_alive() is True           # released => healthy, not aborting
    # idempotent: a second call is a no-op
    holder.closed = False
    ctl._release_snapshot()
    assert holder.closed is False


def test_resolve_owner_reference_shape():
    ctl = _make_controller()
    ref = ctl.resolve_owner_reference()
    assert ref == {
        "apiVersion": "apps/v1", "kind": "StatefulSet", "name": "pgcyan-controller",
        "uid": "u-1", "controller": True, "blockOwnerDeletion": True,
    }


# --------------------------------------------------- wait-to-start

def test_handle_api_status_health_and_unknown_routes(tmp_path):
    ctl = _make_controller(ready_file=str(tmp_path / "ready"))
    ctl.state = "waiting-to-start"

    code, body = ctl.handle_api("GET", "/status")
    assert code == 200 and body["state"] == "waiting-to-start" and body["ready"] is False

    code, body = ctl.handle_api("GET", "/health")
    assert code == 200 and body["status"] == "ok"

    # Trailing slash + query string are normalized; unknown route/verb -> 404.
    assert ctl.handle_api("GET", "/status/?x=1")[0] == 200
    assert ctl.handle_api("GET", "/nope")[0] == 404
    assert ctl.handle_api("DELETE", "/status")[0] == 404


def test_post_start_is_idempotent(tmp_path):
    ctl = _make_controller(ready_file=str(tmp_path / "ready"))
    ctl.state = "waiting-to-start"
    assert ctl._start_requested.is_set() is False

    code, _ = ctl.handle_api("POST", "/start")
    assert code == 202 and ctl._start_requested.is_set()          # first call arms it

    code, _ = ctl.handle_api("POST", "/start")
    assert code == 200                                            # idempotent no-op

    ctl.state = "succeeded"                                       # already finished
    code, body = ctl.handle_api("POST", "/start")
    assert code == 200 and "finished" in body["message"]


def test_wait_to_start_holds_snapshot_until_start_signal(tmp_path):
    """The snapshot is established only AFTER POST /start, not at launch."""
    import threading
    import time

    holder = _FakeHolder()
    ctl = _make_controller(holder=holder, ready_file=str(tmp_path / "ready"), api_port=0)
    ctl.park = lambda: None                     # don't block after the walk in this test

    result = {}
    t = threading.Thread(target=lambda: result.__setitem__("summary", ctl.serve_and_run()),
                         daemon=True)
    t.start()
    try:
        # Reaches the pre-snapshot wait; the snapshot is NOT taken yet.
        for _ in range(100):
            if ctl.state == "waiting-to-start":
                break
            time.sleep(0.02)
        assert ctl.state == "waiting-to-start"
        assert holder.established is False
        assert ctl.handle_api("GET", "/status")[1]["state"] == "waiting-to-start"

        # Signal start -> the run establishes the snapshot and walks.
        assert ctl.handle_api("POST", "/start")[0] == 202
        t.join(timeout=5)
        assert not t.is_alive()
    finally:
        ctl._start_requested.set()              # ensure the thread can't hang on failure
        t.join(timeout=2)

    assert holder.established is True                     # snapshot taken AFTER start
    assert result["summary"]["status"] == "success"
    code, body = ctl.handle_api("GET", "/status")
    assert body["state"] == "succeeded" and body["snapshot"] == "SNAP-1"
