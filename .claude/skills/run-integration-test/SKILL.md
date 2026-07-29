---
name: run-integration-test
description: Use when running, watching, or debugging the pgcyan cluster-backed integration test (.github/workflows/integration-tests.yml) on AKS — dispatching the workflow, waiting for the run, reading pod logs from the uploaded artifact, watching live cluster state, and mapping the known failure modes (image-missing, RBAC subresources, pgcopydb flags) to fixes.
---

# Run & debug the pgcyan integration test

The integration test (`.github/workflows/integration-tests.yml`, `workflow_dispatch`
only) stands up the folded controller (`run-migration`, #70) on the real AKS
cluster and walks the whole implemented DAG (planner → pre-data → data shards)
against real source+target Postgres, asserting the combined `pgcyan-copylog`
ConfigMap (#25's AC) and the data-only target schema shape. One `migration` job,
no parallelism.

## Dispatching (needs the image first)

The phase pods run `pgcyanacr.azurecr.io/pgcyan:<branch-HEAD-short-sha>`, and
`build.yml`'s publish job only runs on push-to-main / manual dispatch — so a
feature-branch commit has **no image** until one is built. The workflow's first
real step ("Ensure the commit's image is published") handles this: if the tag is
absent it dispatches `build.yml` on the branch and polls the ACR. So the only
prerequisite is that **the commit is pushed**.

```bash
git push                                            # the run tests HEAD's short SHA
gh workflow run integration-tests.yml --ref <branch>
gh run list --workflow=integration-tests.yml --limit 1 \
  --json databaseId,status,headSha --jq '.[0]'      # confirm sha == HEAD short SHA
```

Pushing needs a token with `workflow` scope (the file lives under `.github/`); if
yours lacks it, ask the human to push + dispatch.

## Watching a run (live)

GitHub serves **full step logs only after a job finishes**, so watch the *cluster*
for progress in the meantime (the read-only `pgcyan-viewer` kubeconfig context can
`get pods/jobs/configmaps` in `pgcyan-migration`). Run a background poller that
prints only on transitions:

```bash
NS=pgcyan-migration; RUN=<id>; last=""
for i in $(seq 1 140); do
  rs=$(gh run view $RUN --json status --jq .status 2>/dev/null || echo '?')
  state=$(kubectl get cm pgcyan-migration-state -n $NS -o jsonpath='{.data.status}' 2>/dev/null || echo -)
  shpods=$(kubectl get pods -l pgcyan.dev/phase=data -n $NS --no-headers 2>/dev/null | wc -l)
  copylog=$(kubectl get cm pgcyan-copylog -n $NS -o jsonpath='{.metadata.name}' 2>/dev/null || echo -)
  line="run=$rs state=$state shardpods=$shpods copylog=$copylog"
  [ "$line" != "$last" ] && { echo "[$(date +%H:%M:%S)] $line"; last="$line"; }
  [ "$rs" = completed ] && { echo DONE; break; }
  sleep 20
done
```

Read the milestones like this:
- **`state` (run-state ConfigMap)** is the real completion signal — `running` →
  `succeeded`/`failed`. Pod *readiness* flips Ready as soon as the snapshot is
  established (before the walk finishes), so it is NOT the signal.
- **`shardpods` climbing far past the shard count** (e.g. 4 → 28) = shards
  **crash-looping** through their `backoffLimit`, not progressing. A healthy
  phase 2 keeps it near the shard count with the shard Jobs going `succeeded`.
- **`copylog` appearing** = the orchestrator aggregated phase-2 successfully.

## Reading logs after it finishes

The workflow uploads a `pgcyan-migration-<runid>` artifact with per-component pod
logs — this is the primary way to see *why* something failed (the controller
dumps `kubectl logs -l pgcyan.dev/component=controller` on failure, and each
phase's pod log is captured):

```bash
gh run download <runid> -D /tmp/art
A=/tmp/art/pgcyan-migration-<runid>
cat "$A/controller.log"      # the walk: establish, phase applies, the failing barrier
cat "$A/data.log"            # phase-2 shard pods (pgcopydb copy table-data output)
cat "$A/pre-data.log" "$A/planner.log"
python3 -m json.tool < "$A/migration-copy-log.json"   # the #25 result, if it got that far
grep -i status "$A/run-state.yaml"
```

**Firewall caveat:** `gh run download` and `gh run view --log` fetch from Azure
blob storage (`*.blob.core.windows.net`), which the dev-container firewall may
block (`no route to host`) — it allowlists GitHub's API ranges, not the blob
host. If blocked, ask the human to unblock/relay, or fall back to **cluster
events** (they survive teardown ~1h): `kubectl get events -n pgcyan-migration
--sort-by=.lastTimestamp`. Events reveal Job Complete/Failed, ImagePullBackOff,
and probe failures even after the pods are gone.

## Known failure modes (each cost a run to isolate)

These are the real defects the DB-free unit tests structurally cannot catch —
they only surface when the code runs under the least-privilege SA against real
pgcopydb/Postgres. If a run fails, check these first:

1. **`ImagePullBackOff` on `pgcyan:<sha>`** — the commit's image was never built
   (feature branch). The ensure-image step should prevent it; if it fails, the
   OIDC identity may lack **AcrPull** to query the tag. Check the ensure-image
   step's output.
2. **Both jobs hang on the collect-inputs barrier to a 900s timeout** — a
   job-collision race: two jobs managing the same `pgcyan-collect-inputs` Job /
   `pgcyan-plan` ConfigMap in one namespace. Fixed by keeping the workflow a
   **single job**; do not re-introduce a parallel job that also runs phase -2.
3. **`Could not read status of Job/...: Forbidden`** at a barrier — the
   orchestrator SA is missing a **`*/status` subresource** grant (`jobs/status`,
   `deployments/status`). RBAC does not fold `jobs/status` into a grant on
   `jobs`, so create works but the barrier read is denied. Grant it in
   `deploy/k8s/01-rbac.yaml` (a unit test locks this — see
   `test_orchestrator_rbac_grants_status_subresources`).
4. **Every shard crash-loops with a pgcopydb usage/arg error** (e.g.
   `option '--restart' doesn't allow an argument`) — a wrong `pgcopydb` flag in
   `datashard.build_copy_command`. Flags carried over from `clone` are NOT always
   valid on `copy table-data`; verify against `pgcopydb copy table-data --help`.
5. **Fresh run halts immediately, `state=halted`, pod not-ready** — refuse-to-
   resume: a leftover `pgcyan-migration-state` ConfigMap from a prior run. The
   workflow's "Clear prior" step deletes it; a manual re-run needs
   `kubectl delete cm pgcyan-migration-state -n pgcyan-migration` first.

## Success looks like

`state=succeeded`, run conclusion `success`, and `migration-copy-log.json` shows
`status: completed`, the slot name, `shard_count` matching the plan, and a
non-empty per-table `tables` map with `warnings: []` (disjoint shards, no
overlap). The target has tables + data but no secondary indexes / FK / triggers
(data-only held; those are phase 3).
