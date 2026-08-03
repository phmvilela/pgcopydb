# Control-plane deployment (`pgcyan-system`)

The fixed, long-lived namespace that hosts the pgcyan **control-plane API**
(issue #86, design #84). Distinct from `deploy/k8s/` (the per-migration
`pgcyan-migration` namespace): the control plane is created **once** and uses
its cluster-scoped RBAC to provision a **separate, disposable namespace per
migration** (later tickets #87–#90).

## Contents

| File | What |
|---|---|
| `00-namespace.yaml` | the `pgcyan-system` namespace |
| `01-rbac.yaml` | `pgcyan-control-plane` ServiceAccount + ClusterRole + ClusterRoleBinding (create/delete namespaces and the resources inside per-migration namespaces) |
| `10-pvc-registry.yaml` | RWO managed-disk PVC for the SQLite registry, mounted at `/data` |
| `20-deployment-api.yaml` | the FastAPI Deployment (single replica, `Recreate`, non-root) — `GET /health` |
| `30-service-api.yaml` | ClusterIP Service (`:80` → container `:8000`) |

The API source lives in [`../../control-plane/api/`](../../control-plane/api/).

## Apply (manual for now — automated deploy is #91)

The Deployment carries two `envsubst` placeholders, filled at apply time (same
convention as `deploy/k8s`):
- `${PGCYAN_CONTROL_PLANE_IMAGE}` — the control-plane API image itself;
- `${PGCYAN_MIGRATION_IMAGE}` — the pgcyan migration image the API stamps into
  each provisioned controller (#87). Leave it and `POST /migrations` returns 503
  until set.

```bash
export PGCYAN_CONTROL_PLANE_IMAGE=<acr>.azurecr.io/pgcyan-control-plane:<tag>
export PGCYAN_MIGRATION_IMAGE=<acr>.azurecr.io/pgcyan:<short-sha>
kubectl kustomize deploy/control-plane \
  | envsubst '${PGCYAN_CONTROL_PLANE_IMAGE} ${PGCYAN_MIGRATION_IMAGE}' \
  | kubectl apply -f -
```

## Manual verification (issue #86 acceptance)

```bash
# API pod healthy
kubectl get pods -n pgcyan-system
kubectl port-forward -n pgcyan-system svc/pgcyan-control-plane-api 8080:80 &
curl -fsS localhost:8080/health          # -> {"status":"ok",...}

# SA can provision namespaces + namespaced resources
SA=system:serviceaccount:pgcyan-system:pgcyan-control-plane
kubectl auth can-i create namespaces --as="$SA"                               # yes
for r in serviceaccounts secrets configmaps persistentvolumeclaims services \
         statefulsets.apps jobs.batch roles.rbac.authorization.k8s.io \
         rolebindings.rbac.authorization.k8s.io; do
  echo "$r: $(kubectl auth can-i create "$r" -n pgcyan-migration --as="$SA")"
done

# Registry PVC bound + persistent across a pod restart
kubectl get pvc -n pgcyan-system pgcyan-control-plane-registry           # Bound
POD=$(kubectl get pod -n pgcyan-system -l app.kubernetes.io/component=control-plane -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n pgcyan-system "$POD" -- sh -c 'echo hi > /data/canary'
kubectl delete pod -n pgcyan-system "$POD"
# after the new pod is Running:
NEW=$(kubectl get pod -n pgcyan-system -l app.kubernetes.io/component=control-plane -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n pgcyan-system "$NEW" -- cat /data/canary                 # -> hi
```

### Provisioning a migration (issue #87)

```bash
kubectl port-forward -n pgcyan-system svc/pgcyan-control-plane-api 8080:80 &
curl -fsS -XPOST localhost:8080/migrations -H 'content-type: application/json' -d '{
  "source": {"host": "SRC_HOST", "user": "U", "password": "P", "database": "DB"},
  "target": {"host": "TGT_HOST", "user": "U", "password": "P", "database": "DB"}
}'                                          # -> 201 {"id": "...", "namespace": "pgcyan-mig-..."}

NS=pgcyan-mig-<id>
kubectl get ns "$NS"
kubectl get pods -n "$NS"                   # controller Running...
POD=$(kubectl get pod -n "$NS" -l pgcyan.dev/component=controller -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$POD" -n "$NS" -- python -c \
  "import urllib.request;print(urllib.request.urlopen('http://localhost:9999/status').read().decode())"
#   -> {"state":"waiting-to-start", "snapshot": null, ...}   (no slot on the source yet)
```

> **Concurrency caveat:** provisioned controllers use the default `pgcyan_slot` /
> `pgcyan_pub` names (kept identical to the integration test — no drift). Two
> migrations against the **same source** would collide on the slot **at start**
> (not at provision). Per-migration slot naming / clash detection is #88.

