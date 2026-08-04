"""Teardown-logic tests, exercised against a fake k8s applier."""
from app.teardown import (
    CONTROLLER_POD_SELECTOR,
    CONTROLLER_STATEFULSET,
    Teardown,
    TeardownError,
)


class FakeK8s:
    """Records teardown ops; returns a scripted sequence of pod counts."""

    def __init__(self, pod_counts):
        self.pod_counts = list(pod_counts)
        self.scaled = []
        self.deleted = []
        self.polls = 0

    def scale_statefulset(self, namespace, name, replicas):
        self.scaled.append((namespace, name, replicas))

    def count_pods(self, namespace, label_selector):
        self.polls += 1
        # last value repeats once the script is exhausted
        return self.pod_counts.pop(0) if len(self.pod_counts) > 1 else self.pod_counts[0]

    def delete_namespace(self, namespace):
        self.deleted.append(namespace)


def _teardown(k8s, **kw):
    # no real sleeping in tests
    return Teardown(k8s, poll_interval=0.0, sleep=lambda *_: None, **kw)


def test_teardown_scales_down_waits_then_deletes():
    k8s = FakeK8s(pod_counts=[1, 1, 0])   # pod lingers, then gone
    res = _teardown(k8s).teardown("pgcyan-mig-abc")

    assert res == {"namespace": "pgcyan-mig-abc", "graceful": True}
    # scaled the controller to 0 first...
    assert k8s.scaled == [("pgcyan-mig-abc", CONTROLLER_STATEFULSET, 0)]
    # ...polled with the controller selector until the pod was gone...
    assert k8s.polls >= 3
    # ...and only then deleted the namespace.
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
