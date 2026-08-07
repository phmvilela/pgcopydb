# Lessons learned bringing the schema step (pre-data) green on AKS + Azure Postgres

Captured while getting `schema` (issue #24) to pass its cluster-backed
integration test end-to-end. Every fix below took several failed runs to
isolate, and **most of them will recur verbatim in the steps that follow**,
because they share the exact same shape: a non-root pgcopydb pod, mounting an
Azure managed-disk PVC as `--dir`, running against Azure Database for PostgreSQL
(Flexible Server), pinned to the snapshot holder's snapshot.

Directly relevant to:

- **#25 — Sharded Initial Copy Pinned To Snapshot** (the copy step, `pgcopydb clone`, N PVC pods)
- **#26 — Start Change Data Capture** (the follow step, `pgcopydb follow`, PVC pod)
- **#28 — Build Secondary Indexes And FK Constraints** (the post-data steps, post-data restore, PVC pods)
- (#27 lag monitor and #29 cutover touch the cluster less, but the image/CI lessons still apply.)

Read this before wiring up any of those — it will save the same multi-run debugging loop.

---

## 1. Non-root image + Azure managed-disk PVC ⇒ "Permission denied" on `--dir`

**What happened:** the image runs as `USER migration` (uid/gid 1000; `Dockerfile`
`useradd -u 1000`). A freshly-provisioned Azure managed disk mounts its root
`root:root 0755`, so uid 1000 cannot write it. pgcopydb aborted at
`copydb.c FATAL Failed to create directory "/work/schema": Permission denied`
(exit code 12). The snapshot holder never hit this only because its `--dir` is a
node-local `emptyDir`, not a PVC.

**Fix (already applied to 40/60/70/71/80):** a pod-level `securityContext`:

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000            # 'migration' from the image
  runAsGroup: 1000
  fsGroup: 1000              # CSI group-owns the disk so uid 1000 can write
  fsGroupChangePolicy: OnRootMismatch   # don't re-chown the whole catalog every restart
```

**For #25/#26/#28:** every one of these already carries the block — **do not
drop it**, and if you add any *new* pod that mounts a PVC, it needs the same
block. `fsGroup` must be the numeric gid (1000); k8s does not resolve the name
`migration`. `OnRootMismatch` matters most for the **resumable** shards
(#25) and index shards (#28): their catalogs can be large, and without it the
CSI driver re-chowns the entire volume on every pod restart.

## 2. Azure blocks non-allow-listed extensions ⇒ pg_restore aborts

**What happened:** the source schema dump includes Azure's platform-managed
extensions (`azure`, `pgaadauth`). On the target, `azure_pg_admin` cannot
`CREATE`/`DROP` them — Azure gates extensions behind the `azure.extensions`
server parameter. pg_restore emitted 6 `extension "..." is not allow-listed`
errors and returned exit 1, which pgcopydb surfaces as rc=7, failing the whole
restore.

**Fix (the schema step):** pass `--skip-extensions` (manifest-level, in
`40-job-schema.yaml`). On Azure this is the *correct* behaviour
regardless: extensions must be pre-provisioned via `azure.extensions` +
`CREATE EXTENSION` by an allowed role, not carried in a schema restore.

**⚠️ This WILL bite #25 and #28 — check it before you run them:**

- **#25 (data shards):** `60-job-copy.template.yaml` runs
  `pgcopydb clone …` with **no `--skip-extensions`** today. `clone` processes
  the schema sections, so unless the shards are made data-only (see item 6) or
  given `--skip-extensions`, they will hit the identical extension wall.
- **#28 (post-data):** the post-data restore processes the rest of the dump; it
  must also `--skip-extensions` (or the extensions must already exist on the
  target).

Related, lower-severity: the `…azuresu` `Failed to parse Archive TOC comment or
acl` lines are **non-fatal** — pgcopydb skipping pg_catalog ACL entries owned by
Azure's superuser. They did not stop the run. But if a later step's restore
*does* fail on ACL/COMMENT TOC entries, the next levers (all already CLI flags on
`schema`, so no rebuild) are `--no-acl` and `--no-comments`. We already
run `--no-owner` because target roles are pre-created (Terraform).

## 3. The image is pinned by commit short SHA — you must publish one per commit

**What happened:** the biggest time-sink. `integration-tests.yml` resolves the
image as `pgcyan:$(git rev-parse --short HEAD)`, and `build.yml`'s **publish job
only runs on push-to-`main` or manual dispatch — NOT on feature-branch pushes**.
So a branch commit has *no* image until you build one for it. Running the test
first gives either a stale image (old behaviour, confusing) or `ImagePullBackOff`
(tag doesn't exist).

**Workflow for every cluster test on a branch:**

1. Push your commit.
2. `gh workflow run build.yml --ref <branch>` → publishes `pgcyan:<sha>`. Wait
   for the `publish` job.
3. `gh workflow run integration-tests.yml --ref <branch>`.
4. Verify the tag exists first: `az acr repository show --name pgcyanacr --image pgcyan:<sha> --query digest -o tsv`.

**Even a YAML-only commit changes HEAD's SHA**, so the test still needs a
matching image tag. When the code is unchanged you can skip the rebuild and just
retag the previous image (fast):

```bash
az acr import --name pgcyanacr \
  --source pgcyanacr.azurecr.io/pgcyan:<prev-sha> \
  --image pgcyan:<new-sha> --force
```

Consider adding a preflight guard to the integration jobs that fails with a
clear "image `pgcyan:<sha>` not found — run build.yml first" instead of a cryptic
`ImagePullBackOff`. (Left as a possible follow-up.)

## 4. Two deploy layers with very different latencies — know which one you're touching

- **Manifests (`deploy/k8s/*.yaml`)** are applied live by the workflow via
  `kubectl apply` from the checked-out branch → changes take effect on the next
  run with **no rebuild**. (fsGroup, `--skip-extensions`, command/env, `needs:`.)
- **Python / Dockerfile** is **baked into the image** → needs a rebuild + a new
  SHA before it can run (item 3). (The `--snapshot`-on-restore fix was here.)

Before re-running, ask: "is my fix in a manifest or in the image?" It tells you
whether you need to rebuild. Confirm which code actually ran by reading the
command string the wrapper logs (e.g. the presence of `--snapshot` / 
`--skip-extensions` in the `restore-pre-data` "Running" event).

## 5. pgcopydb shares one SQLite catalog under `--dir`; snapshot must match across sub-commands

**What happened:** the wrapper runs `pgcopydb dump schema --snapshot X` then
`pgcopydb restore pre-data` sharing the same `--dir`. pgcopydb stamps the
snapshot id into `<dir>/schema/source.db` and refuses to reuse the catalog unless
the *current* invocation's snapshot matches:
`Catalogs ... have been setup for snapshot "X" and current snapshot is ""` (rc=7).

**Fix:** pass the **same `--snapshot`** to *every* pgcopydb command that shares a
`--dir` (or `--not-consistent` to waive it). **For #28:** if the post-data build
is split into separate pgcopydb invocations over a shared work dir, they must all
carry the same snapshot id (the holder's, from the `pgcyan-snapshot` ConfigMap),
exactly like the schema step does.

## 6. `pgcopydb clone` does schema by default — the shards must be DATA-ONLY (design check for #25)

Not a bug we hit, but the reason items 1/2/5 all matter for #25: the sharded
topology assumes the schema step does pre-data, the copy step copies **data only**, the post-data steps
does post-data. But `pgcopydb clone` (what
`60-job-copy.template.yaml` invokes) runs the full pipeline —
dump/restore pre-data *and* post-data around the COPY — by default. Before #25 is
called done, confirm the shard command is actually restricted to table data
(e.g. the appropriate data-only invocation / section flags), otherwise each shard
will re-run schema DDL, collide with the schema step/3, and re-trip the extension and
ownership walls above.

## 7. CI: singleton holder + shared cluster objects ⇒ serialize the jobs

**What happened:** the `pre-data` integration job and the `snapshot-holder` job
both stand up the **singleton** `pgcyan-snapshot-holder` Deployment and publish
the **same** `pgcyan-snapshot` ConfigMap in the same namespace. GitHub runs jobs
in parallel by default, so the two holders raced and one job's ConfigMap write
landed under the other's pod (mismatched snapshot ids).

**Fix:** `needs: snapshot-holder` on the `pre-data` job. **For #25/#26:** any new
integration job that also needs the holder running (or touches `pgcyan-snapshot`
/ `pgcyan-db-uris` / the holder Deployment) must be chained with `needs:` so it
never overlaps another holder-using job. `planner` stays parallel only because it
touches disjoint objects.

## 8. Target-DB config + reachability (applies to every step that writes the target)

the schema step was the first step to touch the **target**, which needed new
`PG_TARGET_*` GitHub secrets/variables and an in-cluster `psql` check Job (the
target isn't reachable from the GitHub runner — only from inside the cluster).
#25 and #28 write the target too and rely on the same `pgcyan-db-uris`
Secret (`PGCOPYDB_TARGET_PGURI`) and the same "verify from an in-cluster pod, not
the runner" pattern. Target roles must be pre-created (we run `--no-owner`).

## 9. Least-privilege SA + `*/status` subresources ⇒ barrier read Forbidden

**What happened (bringing the folded controller #70 green):** the orchestrator
polls each step barrier with `read_namespaced_job_status` /
`read_namespaced_deployment_status` — i.e. the **`jobs/status` and
`deployments/status` subresources**. Kubernetes RBAC does **not** fold a status
subresource into a grant on its parent: a Role that grants `jobs` (create/get/…)
still gets `Forbidden` on `jobs/status`. So `create` worked (the planner Job ran
and completed on its own) but the orchestrator's barrier read was denied, halting
the DAG before pre-data.

**Why it only surfaced now:** this is the least-privilege-SA fidelity gap. The old
integration jobs drove the cluster with the *runner's* (near-admin) kubeconfig, so
the orchestrator SA's own reads were never exercised until the controller ran
in-pod under `pgcyan-orchestrator`. **Fix:** add `jobs/status` +
`deployments/status` to the Role in `01-rbac.yaml` (a unit test,
`test_orchestrator_rbac_grants_status_subresources`, locks it). **For #26/#28:**
any new in-pod API read of a `*/status` (or a whole new resource kind) needs its
own grant — a create grant is not enough.

## 10. pgcopydb flags are NOT portable across sub-commands

**What happened (first cluster run of the copy step):** every shard crash-looped with
`table-data: option '--restart' doesn't allow an argument` (rc=1). The command
built `pgcopydb copy table-data ... --resume --restart=false`. On `copy
table-data`, `--restart` is a **boolean flag** — it takes no value. The
`--restart=false` was carried over verbatim from the pre-#25 `pgcopydb clone`
manifest (which was never run on a cluster). "Resume, don't wipe" is served by
`--resume` alone (restart defaults off and, when set, WIPES temp files). **Lesson:
verify every flag against `pgcopydb <subcommand> --help`** — options and their
arg-shapes differ between `clone`, `copy table-data`, `snapshot`, `restore`.

## 11. Under the fold, "done" is the run-state marker, not pod readiness

**What happened:** the controller's readiness probe flips Ready as soon as the
snapshot is *established* — well before the DAG walk finishes. Watching pod
readiness to decide "migration complete" is wrong; the completion signal is the
`pgcyan-migration-state` ConfigMap going `succeeded`/`failed`. Also: a leftover
`pgcyan-migration-state` from a prior run makes a fresh controller **refuse to
resume** (parks not-ready) — clear it (and the plan/copylog CMs) between runs. The
`run-integration-test` skill (`.claude/skills/`) has the full watch/clear runbook.

---

## 12. pgcopydb setup steps are non-idempotent across separate pods (#28, #26)

**The recurring pattern.** pgcopydb sub-commands assume they own a single shared
`--dir` catalog for the whole clone. When we split the DAG across shared-nothing
pods -- each with its OWN `--dir` -- a command re-runs a setup step it "already
did" in another pod's state, and that step is often **non-idempotent**:

- **#28:** `pgcopydb copy indexes`/`copy constraints` silently built nothing in a
  standalone per-shard pod (they no-op without the clone's shared catalog) -> we
  switched the indexes step to custom SQL.
- **#26:** `pgcopydb follow` in a separate CDC pod (fresh `--dir`) re-runs the full
  slot setup, aborting first on the publication then on the slot itself
  (`replication slot "<slot>" already exists`, rc=6) -- and you CANNOT drop the slot
  to fix it (it retains WAL from the copy's snapshot; dropping loses changes). Root
  cause (verified in the pgcopydb source): `follow`'s only reuse path needs the
  slot's `lsn`+`snapshot` already present in the `--dir` `source.db` catalog, and
  in this fork the **sentinel** (resume coordinates) + CDC stores live in `--dir`
  SQLite -- so a fresh `--dir` structurally cannot resume. **Resolution (Option A):
  the controller itself streams CDC**, running `follow` on the SAME `--dir` its
  `snapshot --follow` populated -- pgcopydb's native `clone --follow` lineage, where
  the slot record is already local. No separate pod, no cross-pod handoff.
  See `context/cdc-snapshot-slot-mvcc-qa.md` + `k8s-sharded-migration-topology.md`.
- **#26 (follow-up):** and run that `follow` WITHOUT `--resume`. Two source facts:
  slot reuse fires on the slot record being in `--dir` `source.db` (NOT gated on
  `--resume`), while the `pgcopydb.sentinel` row is created ONLY when NOT `--resume`
  (`--resume` skips it, expecting it to exist). `snapshot --follow` writes the slot
  record but NOT the sentinel, so `follow --resume` reuses the slot then aborts
  `sentinel ... 0 rows` (rc=12). Plain `follow` reuses the slot AND creates the
  sentinel (startpos = slot LSN = the copy's snapshot point). `--resume` is only for
  a genuine restart where the sentinel already exists.
- **#26 (leftover origin):** plain `follow` also CREATES the target replication
  origin, and accepts a pre-existing one only if its LSN == this run's startpos
  (`acceptTrackedLSN = resume || lsn == startpos`). A prior/failed/**cancelled** run
  leaves the origin behind at a STALE LSN (target-side; NOT cleaned by the source
  slot/publication drops or the k8s teardown), so `follow` aborts `origin "<name>"
  already exists` (rc=12). Fix: `PgCyanFollow.drop_stale_origin` drops it on the
  target before follow (mirrors `drop_stale_slot`), so follow recreates a fresh
  origin at this run's slot LSN. Full leftover-cleanup set now: slot + publication
  (source, snapshot.py), origin (target, follow.py), sentinel/--dir (fresh PVC),
  run-state marker (k8s teardown).
- **#26 (apply must be enabled):** even fully attached, `follow` only PREFETCHES --
  its catchup process replays to the target only once the sentinel `apply` flag is
  set (`ld_apply.c`: "Waiting until the pgcopydb sentinel apply is enabled"). `clone
  --follow` flips it after its base copy (`sentinel_update_apply(true)`); our copy is
  a separate step, so nothing did -> `write_lsn`/`flush_lsn` advance but `replay_lsn
  0/0` and nothing lands on the target. Fix: `PgCyanFollow.enable_apply` runs
  `pgcopydb stream sentinel set apply --dir <same dir>` once attached. Symptom to
  recognise: CDC "streaming" (slot active) but the target never changes + `replay_lsn
  0/0` in the follow log.
- **#26 -> #78 (the real root cause):** even with apply enabled, the CDC round-trip
  probe still did not land -- and `--debug` showed apply COMMITs the probe's txn, yet
  a target `SELECT ... WHERE email=<probe>` finds nothing. Root cause: the sharded
  COPY loads rows with explicit ids and NEVER advances the target's identity/serial
  **sequences** (`pg_sequence_last_value` = NULL). pgcopydb's CDC transform sets
  `GENERATED ALWAYS AS IDENTITY` columns to DEFAULT, so the replicated INSERT draws
  `id` from the sequence -> gets **1** -> collides with the copied row id=1; the
  conflict is swallowed on commit -> silent drop. `pgcopydb clone` avoids this with
  its "STEP 9: reset sequences"; our custom copy skipped it. Fix: a **sequence-sync
  step (2s, #78)** -- custom SQL, source `pg_sequences` -> target `setval`, after
  the copy. **Symptom to recognise:** CDC "committed" a change (COMMIT in the apply
  log) but the row is absent, on a table with a `GENERATED ALWAYS AS IDENTITY` PK ->
  check `SELECT pg_sequence_last_value(pg_get_serial_sequence('t','id'))` on the
  target (NULL = never advanced = collision).

**Rule of thumb for new steps:** any pgcopydb command that runs in a pod OTHER
than the one that created the slot/snapshot/catalog will re-do publication/slot/
schema setup against a fresh `--dir` (non-idempotently), and cannot resume (the
sentinel + slot record are `--dir`-local). Either **keep the whole lifecycle in one
pod on one persistent `--dir`** (what CDC does), or use custom SQL (what the post-data steps
does). `rc=6` == a source-side setup step (publication
/ slot) it could not (re)create.

## Quick pgcopydb exit-code cheat sheet (from this saga)

- **rc=12** — work-dir / filesystem problem (couldn't create `--dir/schema`): permissions → item 1.
- **rc=7**  — restore/catalog failure: snapshot mismatch (item 5) OR pg_restore returned non-zero (extensions/ACL → item 2).
- **rc=6**  — source-side setup step (publication/slot) it could not (re)create: non-idempotent create against a fresh `--dir` → item 12.

Always read the pgcopydb `FATAL`/`ERROR` lines in the pod log, not just the rc —
the rc is the same for several distinct root causes.
