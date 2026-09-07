"""Teardown-logic tests, exercised against fake k8s + controller clients."""
from app.controller_client import ControllerUnreachable
from app.provisioner import node_reader_binding_name
from app.teardown import (
    CONTROLLER_POD_SELECTOR,
    CONTROLLER_STATEFULSET,
    Teardown,
    TeardownError,
)


class FakeK8s:
    """Records teardown ops; returns a scripted sequence of pod counts."""

    def __init__(self, pod_counts, events=None, boom_on_binding_delete=False):
        self.pod_counts = list(pod_counts)
        self.scaled = []
        self.deleted = []
        self.deleted_bindings = []
        self.polls = 0
        self._events = events
        self._boom_on_binding_delete = boom_on_binding_delete

    def scale_statefulset(self, namespace, name, replicas):
        self.scaled.append((namespace, name, replicas))
        if self._events is not None:
            self._events.append("scale")

    def count_pods(self, namespace, label_selector):
        self.polls += 1
        # last value repeats once the script is exhausted
        return self.pod_counts.pop(0) if len(self.pod_counts) > 1 else self.pod_counts[0]

    def delete_namespace(self, namespace):
        self.deleted.append(namespace)
        if self._events is not None:
            self._events.append("delete")

    def delete_cluster_role_binding(self, name):
        if self._boom_on_binding_delete:
            raise RuntimeError("not found")  # e.g. a pre-#158 migration never had one
        self.deleted_bindings.append(name)
        if self._events is not None:
            self._events.append("delete-binding")


class FakeController:
    """Records /release calls; returns a scripted response or raises unreachable."""

    def __init__(self, response=(200, {"released": True}), unreachable=False, events=None):
        self.response = response
        self.unreachable = unreachable
        self.released = []
        self._events = events

    def release(self, namespace):
        self.released.append(namespace)
        if self._events is not None:
            self._events.append("release")
        if self.unreachable:
            raise ControllerUnreachable(f"controller in {namespace} is unreachable")
        return self.response


def _teardown(k8s, controller=None, **kw):
    # no real sleeping in tests
    return Teardown(k8s, controller or FakeController(),
                    poll_interval=0.0, sleep=lambda *_: None, **kw)


def test_teardown_releases_then_scales_waits_then_deletes():
    events = []
    k8s = FakeK8s(pod_counts=[1, 1, 0], events=events)   # pod lingers, then gone
    controller = FakeController(events=events)
    res = _teardown(k8s, controller).teardown("pgcyan-mig-abc")

    assert res == {"namespace": "pgcyan-mig-abc", "graceful": True, "released": True}
    # released the source FIRST (while the controller is still alive)...
    assert controller.released == ["pgcyan-mig-abc"]
    # ...then scaled to 0, then deleted -- strictly in that order.
    assert events[0] == "release"
    assert events.index("release") < events.index("scale") < events.index("delete")
    assert k8s.scaled == [("pgcyan-mig-abc", CONTROLLER_STATEFULSET, 0)]
    assert k8s.polls >= 3
    assert k8s.deleted == ["pgcyan-mig-abc"]


def test_teardown_uses_the_controller_pod_selector():
    k8s = FakeK8s(pod_counts=[0])
    seen = {}
    orig = k8s.count_pods

    def spy(ns, sel):
        seen["sel"] = sel
        return orig(ns, sel)

    k8s.count_pods = spy
    _teardown(k8s).teardown("ns")
    assert seen["sel"] == CONTROLLER_POD_SELECTOR


def test_teardown_deletes_even_if_pod_lingers_but_flags_not_graceful():
    k8s = FakeK8s(pod_counts=[1])          # never reaches 0
    res = _teardown(k8s, grace_timeout=0.0).teardown("ns")
    assert res["graceful"] is False
    assert k8s.deleted == ["ns"]            # teardown still completes


def test_teardown_wraps_k8s_errors():
    class Boom(FakeK8s):
        def delete_namespace(self, namespace):
            raise RuntimeError("api exploded")

    k8s = Boom(pod_counts=[0])
    try:
        _teardown(k8s).teardown("ns")
        assert False, "expected TeardownError"
    except TeardownError as exc:
        assert "ns" in str(exc)


def test_teardown_proceeds_when_controller_unreachable_for_release():
    # A crashed controller can't release the source; teardown must still delete
    # the namespace (the leaked slot is left for the reaper), flagging released=False.
    k8s = FakeK8s(pod_counts=[0])
    res = _teardown(k8s, FakeController(unreachable=True)).teardown("ns")
    assert res["released"] is False
    assert k8s.deleted == ["ns"]


def test_teardown_flags_released_false_on_controller_error_response():
    # Controller reachable but the release failed (500) -> released False, still deletes.
    k8s = FakeK8s(pod_counts=[0])
    controller = FakeController(response=(500, {"released": False, "errors": ["boom"]}))
    res = _teardown(k8s, controller).teardown("ns")
    assert res["released"] is False
    assert k8s.deleted == ["ns"]


def test_teardown_deletes_the_node_reader_binding():
    k8s = FakeK8s(pod_counts=[0])
    _teardown(k8s).teardown("pgcyan-mig-abc")
    assert k8s.deleted_bindings == [node_reader_binding_name("pgcyan-mig-abc")]


def test_teardown_binding_cleanup_is_best_effort_and_never_blocks():
    # No binding to delete (pre-#158 migration, or provisioning failed before
    # the stamp) -- teardown must still succeed and report normally.
    k8s = FakeK8s(pod_counts=[0], boom_on_binding_delete=True)
    res = _teardown(k8s).teardown("ns")
    assert res == {"namespace": "ns", "graceful": True, "released": True}
    assert k8s.deleted == ["ns"]           # the namespace delete still happened
    assert k8s.deleted_bindings == []      # the binding delete failed silently
