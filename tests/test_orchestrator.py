"""
Unit tests for the migration DAG orchestrator (`pg_cyan.cli orchestrate`, issue #40).

Cluster-free by nature: every Kubernetes API client (batch/apps/policy/core) is
injected as a fake, so nothing here needs a real cluster. Tests exercise
``${...}`` template substitution, manifest loading against the REAL
``deploy/k8s`` files (so a drift between the templates and the substitution
keys is caught), apply idempotency, Job/Deployment barrier outcomes, and the
end-to-end phase walk (fail-fast + successful run).
"""
import json

import pytest
import yaml
from kubernetes.client.rest import ApiException

from pg_cyan.datashard import PgCyanDataShard
from pg_cyan.orchestrator import OrchestratorError, PgCyanOrchestrator


# --------------------------------------------------------------------- fakes

class _Obj:
    """Minimal attribute bag standing in for a Kubernetes API model.

    The real ``kubernetes.client`` model classes (``V1Job``, ``V1Deployment``,
    ...) enforce required-field validation unrelated to what the orchestrator
    reads (e.g. ``V1JobSpec`` demands a ``template``) -- these fakes carry only
    the attributes ``pg_cyan.orchestrator`` actually looks at.
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeBatchApi:
    def __init__(self, job_outcomes=None, create_conflicts=None):
        self.created = []
        self.status_calls = {}
        self._job_outcomes = job_outcomes or {}
        self._create_conflicts = create_conflicts or set()

    def create_namespaced_job(self, namespace, body):
        name = body["metadata"]["name"]
        if name in self._create_conflicts:
            raise ApiException(status=409, reason="AlreadyExists")
        self.created.append((namespace, name, body))

    def read_namespaced_job_status(self, name, namespace):
        self.status_calls[name] = self.status_calls.get(name, 0) + 1
        sequence = self._job_outcomes.get(name, ["complete"])
        idx = min(self.status_calls[name] - 1, len(sequence) - 1)
        state = sequence[idx]
        if state == "running":
            status = _Obj(succeeded=None, failed=None, conditions=None)
        elif state == "complete":
            status = _Obj(succeeded=1, failed=None, conditions=[_Obj(type="Complete", status="True", message=None)])
        elif state == "failed":
            status = _Obj(
                succeeded=None, failed=1,
                conditions=[_Obj(type="Failed", status="True", message="boom")],
            )
        else:
            raise AssertionError(f"unknown state {state}")
        return _Obj(spec=_Obj(backoff_limit=3), status=status)


class FakeAppsApi:
    def __init__(self, deployment_outcomes=None, create_conflicts=None):
        self.created = []
        self._outcomes = deployment_outcomes or {}
        self._create_conflicts = create_conflicts or set()

    def create_namespaced_deployment(self, namespace, body):
        name = body["metadata"]["name"]
        if name in self._create_conflicts:
            raise ApiException(status=409, reason="AlreadyExists")
        self.created.append((namespace, name, body))

    def read_namespaced_deployment_status(self, name, namespace):
        ready = self._outcomes.get(name, 1)
        return _Obj(
            spec=_Obj(replicas=1),
            status=_Obj(ready_replicas=ready, conditions=[]),
        )


class FakePolicyApi:
    def __init__(self):
        self.created = []

    def create_namespaced_pod_disruption_budget(self, namespace, body):
        self.created.append((namespace, body["metadata"]["name"], body))


#: The image every test expects phase pods to be stamped with. An autouse
#: fixture below exports it as PGCYAN_IMAGE so `_resolve_image` uses the
#: override and never has to read a pod -- keeping the existing fakes pod-free.
TEST_IMAGE = "test-registry.example.io/pgcyan:testsha"


@pytest.fixture(autouse=True)
def _pgcyan_image_env(monkeypatch):
    monkeypatch.setenv("PGCYAN_IMAGE", TEST_IMAGE)


class FakeCoreApi:
    def __init__(self, plan=None, missing=False, pod_containers=None, pvc_conflicts=None,
                 shard_copylogs=None, configmap_conflicts=None):
        self._plan = plan
        self._missing = missing
        #: list of (name, image) tuples for read_namespaced_pod, used by the
        #: image-resolution tests that clear the PGCYAN_IMAGE override.
        self._pod_containers = pod_containers or [("orchestrator", TEST_IMAGE)]
        #: PVCs the orchestrator creates (issue #58 per-shard RWO work disks).
        self.created_pvcs = []
        self._pvc_conflicts = pvc_conflicts or set()
        #: per-shard copy-log ConfigMaps the shards published: {shard_name: payload}.
        #: Returned (labelled copylog=shard) by list_namespaced_config_map so the
        #: orchestrator can aggregate them (issue #25).
        self._shard_copylogs = shard_copylogs or {}
        #: ConfigMaps the orchestrator create/replaces (name -> data dict).
        self.configmaps = {}
        self._configmap_conflicts = configmap_conflicts or set()

    def read_namespaced_config_map(self, name, namespace):
        if self._missing:
            raise ApiException(status=404, reason="NotFound")
        return _Obj(data={"plan.json": json.dumps(self._plan)})

    def list_namespaced_config_map(self, namespace, label_selector=None):
        items = [
            _Obj(metadata=_Obj(name=f"pgcyan-copylog-{shard}"),
                 data={PgCyanDataShard.COPY_LOG_CONFIGMAP_KEY: json.dumps(payload)})
            for shard, payload in self._shard_copylogs.items()
        ]
        return _Obj(items=items)

    def read_namespaced_pod(self, name, namespace):
        containers = [_Obj(name=n, image=img) for n, img in self._pod_containers]
        return _Obj(spec=_Obj(containers=containers))

    def create_namespaced_persistent_volume_claim(self, namespace, body):
        name = body["metadata"]["name"]
        if name in self._pvc_conflicts:
            raise ApiException(status=409, reason="AlreadyExists")
        self.created_pvcs.append((namespace, name, body))

    def create_namespaced_config_map(self, namespace, body):
        name = body.metadata.name
        if name in self._configmap_conflicts:
            raise ApiException(status=409, reason="AlreadyExists")
        self.configmaps[name] = body.data

    def replace_namespaced_config_map(self, name, namespace, body):
        self.configmaps[name] = body.data


SAMPLE_PLAN = {
    "run_id": "pgcyan-test-run",
    "slot": {"name": "pgcyan_slot"},
    "publication": {"name": "pgcyan_pub"},
    "data_shards": [
        {
            "name": "shard-0", "filter_file": "shard-0.filters", "table_jobs": 4,
            "split_tables_larger_than": "50GB", "resources": {"cpu": "2", "memory": "4Gi"},
        },
        {
            "name": "shard-1", "filter_file": "shard-1.filters", "table_jobs": 8,
            "split_tables_larger_than": "50GB", "resources": {"cpu": "4", "memory": "8Gi"},
        },
    ],
    "index_shards": [
        {"name": "idx-0", "filter_file": "shard-0.filters", "index_jobs": 4},
        {"name": "idx-1", "filter_file": "shard-1.filters", "index_jobs": 4},
    ],
}


def make_orchestrator(**kwargs):
    defaults = dict(
        batch_api=FakeBatchApi(),
        apps_api=FakeAppsApi(),
        policy_api=FakePolicyApi(),
        core_api=FakeCoreApi(plan=SAMPLE_PLAN),
        poll_interval=0,
    )
    defaults.update(kwargs)
    return PgCyanOrchestrator(**defaults)


# ------------------------------------------------------------- ${...} render

def test_render_yaml_substitutes_every_placeholder():
    orch = make_orchestrator()
    template = "name: pgcyan-${THING}-x\nvalue: \"${THING}\"\n"
    docs = orch._render_yaml(template, {"THING": "widget"})
    assert docs == [{"name": "pgcyan-widget-x", "value": "widget"}]


def test_render_yaml_renders_every_document_in_a_multi_doc_template():
    orch = make_orchestrator()
    template = (
        "kind: PersistentVolumeClaim\n"
        "metadata:\n  name: pvc-${N}\n"
        "---\n"
        "kind: Job\n"
        "metadata:\n  name: job-${N}\n"
    )
    docs = orch._render_yaml(template, {"N": "0"})
    assert [d["kind"] for d in docs] == ["PersistentVolumeClaim", "Job"]
    assert docs[0]["metadata"]["name"] == "pvc-0"
    assert docs[1]["metadata"]["name"] == "job-0"


def test_render_yaml_raises_on_unknown_placeholder():
    orch = make_orchestrator()
    with pytest.raises(OrchestratorError, match="UNKNOWN"):
        orch._render_yaml("value: ${UNKNOWN}\n", {})


# --------------------------------------------------------- manifest loading

def test_load_phase_manifests_static_phase_reads_real_files():
    orch = make_orchestrator()
    phase = {"id": "0", "name": "pre-data", "manifest": "40-job-phase0-predata.yaml"}
    docs = orch._load_phase_manifests(phase, plan=None)
    assert len(docs) == 1
    assert docs[0]["kind"] == "Job"
    assert docs[0]["metadata"]["name"] == "pgcyan-phase0-predata"


def test_load_phase_manifests_static_phase_multi_doc():
    orch = make_orchestrator()
    phase = {"id": "1", "name": "snapshot-holder", "manifest": "50-deployment-snapshot-holder.yaml"}
    docs = orch._load_phase_manifests(phase, plan=None)
    kinds = {d["kind"] for d in docs}
    assert kinds == {"Deployment", "PodDisruptionBudget"}


def test_load_phase_manifests_templated_phase_renders_pvc_and_job_per_shard():
    orch = make_orchestrator()
    phase = dict(PgCyanOrchestrator.PHASES[3])  # data-shards
    assert phase["id"] == "2"
    docs = orch._load_phase_manifests(phase, plan=SAMPLE_PLAN)
    # one RWO work-dir PVC + one Job per shard (issue #58), PVC first so it is
    # applied before the Job that mounts it.
    jobs = [d for d in docs if d["kind"] == "Job"]
    pvcs = [d for d in docs if d["kind"] == "PersistentVolumeClaim"]
    assert [d["metadata"]["name"] for d in jobs] == [
        "pgcyan-datashard-shard-0", "pgcyan-datashard-shard-1",
    ]
    assert [d["metadata"]["name"] for d in pvcs] == [
        "pgcyan-workdir-shard-0", "pgcyan-workdir-shard-1",
    ]
    assert docs[0]["kind"] == "PersistentVolumeClaim"  # PVC precedes its Job
    # the shard Job mounts its own per-shard PVC and reads the snapshot ConfigMap
    shard0 = jobs[0]["spec"]["template"]["spec"]
    assert shard0["volumes"][0]["persistentVolumeClaim"]["claimName"] == "pgcyan-workdir-shard-0"
    # no leftover placeholders anywhere in the rendered docs
    assert "${" not in yaml.safe_dump(docs)


def test_load_phase_manifests_index_template_renders_pvc_and_job_per_index_shard():
    orch = make_orchestrator()
    phase = dict(PgCyanOrchestrator.PHASES[4])  # index
    assert phase["id"] == "3a"
    docs = orch._load_phase_manifests(phase, plan=SAMPLE_PLAN)
    jobs = [d["metadata"]["name"] for d in docs if d["kind"] == "Job"]
    pvcs = [d["metadata"]["name"] for d in docs if d["kind"] == "PersistentVolumeClaim"]
    assert jobs == ["pgcyan-index-idx-0", "pgcyan-index-idx-1"]
    assert pvcs == ["pgcyan-workdir-idx-0", "pgcyan-workdir-idx-1"]


def test_load_phase_manifests_templated_phase_requires_plan_entries():
    orch = make_orchestrator()
    phase = dict(PgCyanOrchestrator.PHASES[3])
    with pytest.raises(OrchestratorError, match="data_shards"):
        orch._load_phase_manifests(phase, plan={"data_shards": []})


# ----------------------------------------------------------- image handling

def _container_image(doc):
    return doc["spec"]["template"]["spec"]["containers"][0]["image"]


def test_static_manifest_image_placeholder_filled_runtime_vars_preserved():
    """${PGCYAN_IMAGE} is stamped with the resolved image; ${RESTORE_JOBS} and
    $(SNAPSHOT_ID) -- runtime shell/env refs -- must survive untouched."""
    orch = make_orchestrator()
    fk = orch._load_phase_manifests(
        {"id": "3b", "name": "fk", "manifest": "71-job-phase3-fk.yaml"}, plan=None
    )[0]
    assert _container_image(fk) == TEST_IMAGE
    assert "${RESTORE_JOBS}" in yaml.safe_dump(fk)  # runtime shell var preserved

    predata = orch._load_phase_manifests(
        {"id": "0", "name": "pre-data", "manifest": "40-job-phase0-predata.yaml"}, plan=None
    )[0]
    assert _container_image(predata) == TEST_IMAGE
    # the snapshot id is sourced from the holder's ConfigMap via configMapKeyRef
    # (issue #24) -- image-only substitution must leave that ref intact
    assert "pgcyan-snapshot" in yaml.safe_dump(predata)


def test_templated_manifest_image_placeholder_filled():
    orch = make_orchestrator()
    phase = dict(PgCyanOrchestrator.PHASES[3])  # data-shards
    docs = orch._load_phase_manifests(phase, plan=SAMPLE_PLAN)
    # Templated phases are multi-doc now (a per-shard PVC precedes each Job,
    # issue #58); the image placeholder lives on the Job containers.
    jobs = [d for d in docs if d["kind"] == "Job"]
    assert jobs
    for job in jobs:
        assert _container_image(job) == TEST_IMAGE
    assert "${" not in yaml.safe_dump(docs)  # no leftover placeholders anywhere


def test_resolve_image_env_override_wins():
    orch = make_orchestrator()
    assert orch._resolve_image() == TEST_IMAGE  # from the autouse fixture


def test_resolve_image_reads_own_pod(monkeypatch):
    monkeypatch.delenv("PGCYAN_IMAGE", raising=False)
    monkeypatch.setattr("pg_cyan.orchestrator.socket.gethostname", lambda: "pgcyan-orch-xyz")
    core_api = FakeCoreApi(pod_containers=[("orchestrator", "acr.io/pgcyan:deadbee")])
    orch = make_orchestrator(core_api=core_api)
    assert orch._resolve_image() == "acr.io/pgcyan:deadbee"


def test_resolve_image_pod_read_failure_raises(monkeypatch):
    monkeypatch.delenv("PGCYAN_IMAGE", raising=False)

    class ExplodingPodCoreApi(FakeCoreApi):
        def read_namespaced_pod(self, name, namespace):
            raise ApiException(status=404, reason="NotFound")

    orch = make_orchestrator(core_api=ExplodingPodCoreApi())
    with pytest.raises(OrchestratorError, match="resolve the image"):
        orch._resolve_image()


# ------------------------------------------------------------------- apply

def test_apply_creates_job():
    batch_api = FakeBatchApi()
    orch = make_orchestrator(batch_api=batch_api)
    orch._apply({"kind": "Job", "metadata": {"name": "pgcyan-phase0-predata"}})
    assert batch_api.created == [("pgcyan-migration", "pgcyan-phase0-predata",
                                   {"kind": "Job", "metadata": {"name": "pgcyan-phase0-predata",
                                                                 "namespace": "pgcyan-migration"}})]


def test_apply_treats_conflict_as_idempotent_noop():
    batch_api = FakeBatchApi(create_conflicts={"pgcyan-phase0-predata"})
    orch = make_orchestrator(batch_api=batch_api)
    orch._apply({"kind": "Job", "metadata": {"name": "pgcyan-phase0-predata"}})  # must not raise
    assert batch_api.created == []


def test_apply_unsupported_kind_raises():
    orch = make_orchestrator()
    with pytest.raises(OrchestratorError, match="ConfigMap"):
        orch._apply({"kind": "ConfigMap", "metadata": {"name": "x"}})


def test_apply_creates_persistent_volume_claim():
    core_api = FakeCoreApi(plan=SAMPLE_PLAN)
    orch = make_orchestrator(core_api=core_api)
    orch._apply({"kind": "PersistentVolumeClaim", "metadata": {"name": "pgcyan-workdir-shard-0"}})
    assert [n for _, n, _ in core_api.created_pvcs] == ["pgcyan-workdir-shard-0"]


def test_apply_pvc_conflict_is_idempotent_noop():
    core_api = FakeCoreApi(plan=SAMPLE_PLAN, pvc_conflicts={"pgcyan-workdir-shard-0"})
    orch = make_orchestrator(core_api=core_api)
    orch._apply({"kind": "PersistentVolumeClaim", "metadata": {"name": "pgcyan-workdir-shard-0"}})  # no raise
    assert core_api.created_pvcs == []


# -------------------------------------------------------------------- wait

def test_wait_for_job_success_first_poll():
    orch = make_orchestrator(batch_api=FakeBatchApi(job_outcomes={"j1": ["complete"]}))
    success, message = orch._wait_for("Job", "j1", timeout=10)
    assert success is True
    assert message is None


def test_wait_for_job_polls_until_complete():
    orch = make_orchestrator(batch_api=FakeBatchApi(job_outcomes={"j1": ["running", "running", "complete"]}))
    success, message = orch._wait_for("Job", "j1", timeout=10)
    assert success is True


def test_wait_for_job_failure():
    orch = make_orchestrator(batch_api=FakeBatchApi(job_outcomes={"j1": ["failed"]}))
    success, message = orch._wait_for("Job", "j1", timeout=10)
    assert success is False
    assert "boom" in message


def test_wait_for_job_times_out(monkeypatch):
    from pg_cyan import orchestrator as orchestrator_module

    clock = iter([0, 0, 100, 100])  # jumps well past a timeout=10 on the 2nd check
    monkeypatch.setattr(orchestrator_module.time, "monotonic", lambda: next(clock))
    orch = make_orchestrator(batch_api=FakeBatchApi(job_outcomes={"j1": ["running"]}))
    success, message = orch._wait_for("Job", "j1", timeout=10)
    assert success is False
    assert "Timed out" in message


def test_wait_for_deployment_ready():
    orch = make_orchestrator(apps_api=FakeAppsApi(deployment_outcomes={"d1": 1}))
    success, message = orch._wait_for("Deployment", "d1", timeout=10)
    assert success is True
    assert message is None


def test_wait_for_deployment_not_ready_times_out(monkeypatch):
    from pg_cyan import orchestrator as orchestrator_module

    clock = iter([0, 0, 100, 100])
    monkeypatch.setattr(orchestrator_module.time, "monotonic", lambda: next(clock))
    orch = make_orchestrator(apps_api=FakeAppsApi(deployment_outcomes={"d1": 0}))
    success, message = orch._wait_for("Deployment", "d1", timeout=10)
    assert success is False
    assert "Timed out" in message


# -------------------------------------------------------------- plan i/o

def test_read_plan_configmap_parses_json():
    orch = make_orchestrator(core_api=FakeCoreApi(plan=SAMPLE_PLAN))
    plan = orch._read_plan_configmap()
    assert plan == SAMPLE_PLAN


def test_read_plan_configmap_missing_raises():
    orch = make_orchestrator(core_api=FakeCoreApi(missing=True))
    with pytest.raises(OrchestratorError):
        orch._read_plan_configmap()


def test_read_plan_configmap_bad_json_raises():
    class BadCoreApi:
        def read_namespaced_config_map(self, name, namespace):
            return _Obj(data={"plan.json": "{not json"})

    orch = make_orchestrator(core_api=BadCoreApi())
    with pytest.raises(OrchestratorError, match="not valid JSON"):
        orch._read_plan_configmap()


# --------------------------------------------------------------------- run

def _all_jobs_complete_batch_api():
    return FakeBatchApi(job_outcomes={
        "pgcyan-planner": ["complete"],
        "pgcyan-phase0-predata": ["complete"],
        "pgcyan-datashard-shard-0": ["complete"],
        "pgcyan-datashard-shard-1": ["complete"],
        "pgcyan-index-idx-0": ["complete"],
        "pgcyan-index-idx-1": ["complete"],
        "pgcyan-phase3-fk": ["complete"],
    })


def _all_deployments_ready_apps_api():
    return FakeAppsApi(deployment_outcomes={"pgcyan-snapshot-holder": 1, "pgcyan-follow-cdc": 1})


def test_run_happy_path_applies_all_seven_phases_in_order():
    batch_api = _all_jobs_complete_batch_api()
    apps_api = _all_deployments_ready_apps_api()
    core_api = FakeCoreApi(plan=SAMPLE_PLAN)
    orch = make_orchestrator(batch_api=batch_api, apps_api=apps_api, core_api=core_api)
    summary = orch.run()

    assert summary["status"] == "success"
    assert summary["run_id"] == "pgcyan-test-run"
    # holder (1) runs BEFORE pre-data (0): pre-data pins to the holder's snapshot
    assert [p["phase"] for p in summary["phases"]] == ["-1", "1", "0", "2", "3a", "3b", "4"]
    assert all(p["success"] for p in summary["phases"])
    # each data + index shard got its own RWO work-dir PVC stamped (issue #58)
    assert {n for _, n, _ in core_api.created_pvcs} == {
        "pgcyan-workdir-shard-0", "pgcyan-workdir-shard-1",
        "pgcyan-workdir-idx-0", "pgcyan-workdir-idx-1",
    }
    # data + index shards + fk + pre-data + planner Jobs were all created
    created_job_names = {name for _, name, _ in batch_api.created}
    assert created_job_names == {
        "pgcyan-planner", "pgcyan-phase0-predata", "pgcyan-datashard-shard-0",
        "pgcyan-datashard-shard-1", "pgcyan-index-idx-0", "pgcyan-index-idx-1",
        "pgcyan-phase3-fk",
    }
    created_deployment_names = {name for _, name, _ in apps_api.created}
    assert created_deployment_names == {"pgcyan-snapshot-holder", "pgcyan-follow-cdc"}


def test_run_fails_fast_and_stops_the_dag():
    batch_api = FakeBatchApi(job_outcomes={
        "pgcyan-planner": ["complete"],
        "pgcyan-phase0-predata": ["failed"],
    })
    apps_api = _all_deployments_ready_apps_api()
    orch = make_orchestrator(batch_api=batch_api, apps_api=apps_api,
                              core_api=FakeCoreApi(plan=SAMPLE_PLAN))
    summary = orch.run()

    assert summary["status"] == "failure"
    # planner (-1) + holder (1) ran, pre-data (0) failed, nothing after attempted
    assert [p["phase"] for p in summary["phases"]] == ["-1", "1", "0"]
    assert summary["phases"][0]["success"] is True   # planner
    assert summary["phases"][1]["success"] is True   # snapshot-holder
    assert summary["phases"][2]["success"] is False  # pre-data
    # the failing pre-data phase halts the DAG: no data shards, no follow-cdc
    assert "pgcyan-datashard-shard-0" not in {n for _, n, _ in batch_api.created}
    assert "pgcyan-follow-cdc" not in {n for _, n, _ in apps_api.created}


def test_run_stops_before_reading_plan_if_earlier_phase_fails():
    """The plan ConfigMap must not even be read if phase -1/0/1 already failed."""
    batch_api = FakeBatchApi(job_outcomes={"pgcyan-planner": ["failed"]})

    class ExplodingCoreApi:
        def read_namespaced_config_map(self, name, namespace):
            raise AssertionError("plan should not be read before phase 2")

    orch = make_orchestrator(batch_api=batch_api, core_api=ExplodingCoreApi())
    summary = orch.run()
    assert summary["status"] == "failure"
    assert [p["phase"] for p in summary["phases"]] == ["-1"]


def test_run_fails_fast_when_one_of_two_data_shards_fails():
    batch_api = FakeBatchApi(job_outcomes={
        "pgcyan-planner": ["complete"],
        "pgcyan-phase0-predata": ["complete"],
        "pgcyan-datashard-shard-0": ["complete"],
        "pgcyan-datashard-shard-1": ["failed"],
    })
    apps_api = _all_deployments_ready_apps_api()
    orch = make_orchestrator(batch_api=batch_api, apps_api=apps_api,
                              core_api=FakeCoreApi(plan=SAMPLE_PLAN))
    summary = orch.run()

    assert summary["status"] == "failure"
    assert [p["phase"] for p in summary["phases"]] == ["-1", "1", "0", "2"]
    data_phase = summary["phases"][-1]
    assert data_phase["success"] is False
    targets_by_name = {t["name"]: t for t in data_phase["targets"]}
    assert targets_by_name["pgcyan-datashard-shard-0"]["success"] is True
    assert targets_by_name["pgcyan-datashard-shard-1"]["success"] is False


# ------------------------------------------------ slot/publication in shard env

def _shard_env(job_doc):
    """Return the container env of a rendered data-shard Job as a name->value dict."""
    container = job_doc["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: e.get("value") for e in container["env"]}


def test_data_shard_env_carries_plan_slot_and_publication():
    orch = make_orchestrator()
    phase = dict(PgCyanOrchestrator.PHASES[3])  # data-shards
    docs = orch._load_phase_manifests(phase, plan=SAMPLE_PLAN)
    job = next(d for d in docs if d["kind"] == "Job")
    env = _shard_env(job)
    # slot/publication are plan-level and flow into the shard log via env.
    assert env["SLOT_NAME"] == "pgcyan_slot"
    assert env["PUBLICATION_NAME"] == "pgcyan_pub"
    assert env["SHARD_NAME"] == "shard-0"
    # the shard runs the Python wrapper, not a raw pgcopydb clone.
    assert job["spec"]["template"]["spec"]["containers"][0]["command"] == [
        "python", "-m", "pg_cyan.cli", "copy-shard",
    ]


# ----------------------------------------------------- copy-log aggregation

def _shard_log(shard, tables, slot="pgcyan_slot", pub="pgcyan_pub", snap="0000000A-1"):
    """Build a per-shard copy-log payload (as a shard would publish to its CM)."""
    return {
        "component": "data-shard", "status": "completed", "shard": shard,
        "slot": slot, "publication": pub, "snapshot": snap, "tables": tables,
    }


def test_merge_copy_logs_unions_disjoint_tables_and_sums_rows():
    orch = make_orchestrator()
    logs = [
        {"shard": "shard-0", "slot": "pgcyan_slot", "publication": "pgcyan_pub",
         "snapshot": "0000000A-1", "tables": {"public.orders": 10, "public.customers": 20}},
        {"shard": "shard-1", "slot": "pgcyan_slot", "publication": "pgcyan_pub",
         "snapshot": "0000000A-1", "tables": {"shop.items": 30}},
    ]
    merged = orch._merge_copy_logs(logs)
    assert merged["status"] == "completed"
    assert merged["slot"] == "pgcyan_slot"
    assert merged["publication"] == "pgcyan_pub"
    assert merged["snapshot"] == "0000000A-1"
    assert merged["shard_count"] == 2
    assert merged["table_count"] == 3
    assert merged["row_count_total"] == 60
    assert merged["tables"]["shop.items"] == 30
    assert merged["warnings"] == []


def test_merge_copy_logs_flags_snapshot_disagreement():
    orch = make_orchestrator()
    logs = [
        {"shard": "shard-0", "snapshot": "0000000A-1", "tables": {"a.b": 1}},
        {"shard": "shard-1", "snapshot": "0000000B-2", "tables": {"c.d": 2}},
    ]
    merged = orch._merge_copy_logs(logs)
    assert any("disagree on snapshot" in w for w in merged["warnings"])


def test_aggregate_copy_logs_publishes_combined_configmap():
    core = FakeCoreApi(
        plan=SAMPLE_PLAN,
        shard_copylogs={
            "shard-0": _shard_log("shard-0", {"public.orders": 10, "public.customers": 20}),
            "shard-1": _shard_log("shard-1", {"shop.items": 30}),
        },
    )
    orch = make_orchestrator(core_api=core)
    combined = orch._aggregate_copy_logs()

    assert combined["row_count_total"] == 60
    assert combined["table_count"] == 3
    assert combined["published"] is True
    # published into the pgcyan-copylog ConfigMap under the expected key
    published = core.configmaps[PgCyanOrchestrator.COPY_LOG_CONFIGMAP]
    payload = json.loads(published[PgCyanOrchestrator.COPY_LOG_KEY])
    assert payload["row_count_total"] == 60
    assert payload["tables"]["public.orders"] == 10


def test_aggregate_copy_logs_empty_when_no_shard_logs():
    core = FakeCoreApi(plan=SAMPLE_PLAN, shard_copylogs={})
    orch = make_orchestrator(core_api=core)
    combined = orch._aggregate_copy_logs()
    assert combined["shard_count"] == 0
    assert combined["row_count_total"] == 0
    assert any("No shard copy logs" in w for w in combined["warnings"])


def test_full_run_aggregates_copy_logs_on_phase_two_success():
    batch_api = FakeBatchApi(job_outcomes={
        "pgcyan-planner": ["complete"],
        "pgcyan-phase0-predata": ["complete"],
        "pgcyan-datashard-shard-0": ["complete"],
        "pgcyan-datashard-shard-1": ["complete"],
        "pgcyan-index-idx-0": ["complete"],
        "pgcyan-index-idx-1": ["complete"],
        "pgcyan-phase3-fk": ["complete"],
    })
    apps_api = _all_deployments_ready_apps_api()
    core = FakeCoreApi(
        plan=SAMPLE_PLAN,
        shard_copylogs={
            "shard-0": _shard_log("shard-0", {"public.orders": 10, "public.customers": 20}),
            "shard-1": _shard_log("shard-1", {"shop.items": 30}),
        },
    )
    orch = make_orchestrator(batch_api=batch_api, apps_api=apps_api, core_api=core)
    summary = orch.run()

    assert summary["status"] == "success"
    data_phase = next(p for p in summary["phases"] if p["phase"] == "2")
    assert data_phase["copy_log"]["row_count_total"] == 60
    assert PgCyanOrchestrator.COPY_LOG_CONFIGMAP in core.configmaps
