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

The Deployment's image is a `${PGCYAN_CONTROL_PLANE_IMAGE}` placeholder, filled
with `envsubst` at apply time (same convention as `deploy/k8s`):

```bash
export PGCYAN_CONTROL_PLANE_IMAGE=<acr>.azurecr.io/pgcyan-control-plane:<tag>
kubectl kustomize deploy/control-plane \
  | envsubst '${PGCYAN_CONTROL_PLANE_IMAGE}' \
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
