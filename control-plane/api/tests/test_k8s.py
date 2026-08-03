"""K8sApplier I/O tests (issue #90 regression).

Guards the one behaviour that RBAC depends on: scaling the controller must patch
the StatefulSet *resource*, never the ``statefulsets/scale`` subresource (a
separate RBAC resource the control-plane ClusterRole does not grant). This was a
real teardown failure caught only on-cluster -- unit tests use a fake applier, so
this test pins the real applier's call.
"""


def test_scale_statefulset_patches_resource_not_scale_subresource():
    from kubernetes import client

    from app.k8s import K8sApplier

    applier = K8sApplier(api_client=client.ApiClient())  # no cluster call on construct

    calls = []

    class RecordingApps:
        def patch_namespaced_stateful_set(self, name, namespace, body):
            calls.append(("resource", name, namespace, body))

        def patch_namespaced_stateful_set_scale(self, *a, **k):
            calls.append(("scale-subresource",))  # must NOT be used (needs extra RBAC)

    applier._apps = RecordingApps()
    applier.scale_statefulset("pgcyan-mig-abc", "pgcyan-controller", 0)

    assert calls == [("resource", "pgcyan-controller", "pgcyan-mig-abc",
                      {"spec": {"replicas": 0}})]
