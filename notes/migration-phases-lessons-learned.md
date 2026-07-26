# Lessons learned bringing phase 0 (pre-data) green on AKS + Azure Postgres

Captured while getting `restore-predata` (issue #24) to pass its cluster-backed
integration test end-to-end. Every fix below took several failed runs to
isolate, and **most of them will recur verbatim in the phases that follow**,
because they share the exact same shape: a non-root pgcopydb pod, mounting an
Azure managed-disk PVC as `--dir`, running against Azure Database for PostgreSQL
(Flexible Server), pinned to the phase-1 holder's snapshot.

Directly relevant to:

- **#25 — Sharded Initial Copy Pinned To Snapshot** (phase 2, `pgcopydb clone`, N PVC pods)
- **#26 — Start Change Data Capture** (phase 4, `pgcopydb follow`, PVC pod)
- **#28 — Build Secondary Indexes And FK Constraints** (phase 3, post-data restore, PVC pods)
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

**Fix (phase 0):** pass `--skip-extensions` (manifest-level, in
`40-job-phase0-predata.yaml`). On Azure this is the *correct* behaviour
regardless: extensions must be pre-provisioned via `azure.extensions` +
`CREATE EXTENSION` by an allowed role, not carried in a schema restore.

**⚠️ This WILL bite #25 and #28 — check it before you run them:**

- **#25 (data shards):** `60-job-phase2-datashard.template.yaml` runs
  `pgcopydb clone …` with **no `--skip-extensions`** today. `clone` processes
  the schema sections, so unless the shards are made data-only (see item 6) or
  given `--skip-extensions`, they will hit the identical extension wall.
- **#28 (post-data):** the post-data restore processes the rest of the dump; it
  must also `--skip-extensions` (or the extensions must already exist on the
  target).

Related, lower-severity: the `…azuresu` `Failed to parse Archive TOC comment or
acl` lines are **non-fatal** — pgcopydb skipping pg_catalog ACL entries owned by
Azure's superuser. They did not stop the run. But if a later phase's restore
*does* fail on ACL/COMMENT TOC entries, the next levers (all already CLI flags on
`restore-predata`, so no rebuild) are `--no-acl` and `--no-comments`. We already
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
exactly like phase 0 does.

## 6. `pgcopydb clone` does schema by default — the shards must be DATA-ONLY (design check for #25)

Not a bug we hit, but the reason items 1/2/5 all matter for #25: the sharded
topology assumes phase 0 does pre-data, phase 2 copies **data only**, phase 3
does post-data. But `pgcopydb clone` (what
`60-job-phase2-datashard.template.yaml` invokes) runs the full pipeline —
dump/restore pre-data *and* post-data around the COPY — by default. Before #25 is
called done, confirm the shard command is actually restricted to table data
(e.g. the appropriate data-only invocation / section flags), otherwise each shard
will re-run schema DDL, collide with phase 0/3, and re-trip the extension and
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

## 8. Target-DB config + reachability (applies to every phase that writes the target)

Phase 0 was the first phase to touch the **target**, which needed new
`PG_TARGET_*` GitHub secrets/variables and an in-cluster `psql` check Job (the
target isn't reachable from the GitHub runner — only from inside the cluster).
#25 and #28 write the target too and rely on the same `pgcyan-db-uris`
Secret (`PGCOPYDB_TARGET_PGURI`) and the same "verify from an in-cluster pod, not
the runner" pattern. Target roles must be pre-created (we run `--no-owner`).

---

## Quick pgcopydb exit-code cheat sheet (from this saga)

- **rc=12** — work-dir / filesystem problem (couldn't create `--dir/schema`): permissions → item 1.
- **rc=7**  — restore/catalog failure: snapshot mismatch (item 5) OR pg_restore returned non-zero (extensions/ACL → item 2).

Always read the pgcopydb `FATAL`/`ERROR` lines in the pod log, not just the rc —
the rc is the same for several distinct root causes.
