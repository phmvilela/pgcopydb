"""K8sApplier I/O tests.

Guards the behaviours that RBAC depends on: scaling the controller must patch
the StatefulSet *resource*, never the ``statefulsets/scale`` subresource (a
separate RBAC resource the control-plane ClusterRole does not grant); deleting
the per-namespace node-reader binding (#158) must go through the RBAC client,
not Core/Apps. Unit tests elsewhere use a fake applier, so these pin the real
applier's calls.

See also: control-plane/api/app/k8s.py.
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


def test_delete_cluster_role_binding_uses_the_rbac_client():
    from kubernetes import client

    from app.k8s import K8sApplier

    applier = K8sApplier(api_client=client.ApiClient())

    calls = []

    class RecordingRbac:
        def delete_cluster_role_binding(self, name):
            calls.append(name)

    applier._rbac = RecordingRbac()
    applier.delete_cluster_role_binding("pgcyan-node-reader-pgcyan-mig-abc")

    assert calls == ["pgcyan-node-reader-pgcyan-mig-abc"]
