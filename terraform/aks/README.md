# pg_cyan AKS Terraform (`terraform/aks`)

Provisions the **Azure Kubernetes Service** cluster that runs the sharded
pgcopydb migration defined in [`deploy/k8s`](../../deploy/k8s/). This module is
**separate from** the worker-VM module in [`../`](../) — it has its own state
(a separate blob in the shared remote backend; see [Remote state](#remote-state))
and is applied independently.

## What it creates

| Resource | Why the `deploy/k8s` manifests need it |
|---|---|
| **AKS cluster** with `oidc_issuer_enabled` + `workload_identity_enabled` | `01-rbac.yaml` binds the worker/orchestrator ServiceAccounts to Azure Workload Identity so pods get short-lived Entra tokens instead of static DB passwords. |
| **2 user-assigned managed identities** (`-orchestrator`, `-worker`) + **federated credentials** | One per ServiceAccount (`pgcyan-orchestrator`, `pgcyan-worker` in `pgcyan-migration`). The federated credential's `subject` matches `system:serviceaccount:<ns>:<sa>` exactly. |
| **Azure Files CSI driver** (`file_driver_enabled`) | Provides the `azurefile-csi` **ReadWriteMany** StorageClass that `20-pvc-workdir.yaml` claims for the shared `/work` dir. |
| **Azure Container Registry** + **AcrPull** to the kubelet identity | Hosts the `REPLACE_ME/pgcyan` image so every Job/Deployment can pull it without imagePullSecrets. |
| **Log Analytics** + **Container Insights** (`oms_agent`, managed-identity auth) | Cluster/pod observability for the migration run. |
| **Azure CNI Overlay** VNet/subnet | Pod IPs come from an overlay CIDR, so heavy concurrent shard fan-out never exhausts the VNet. |
| **GitHub Actions OIDC identity** (UAMI + federated credential) | Passwordless Azure login for `.github/workflows/build.yml`'s `publish` (ACR push) and `integration-tests.yml`'s cluster jobs — no static credentials in GitHub. |

## Prerequisites

- Azure CLI authenticated: `az login`
- Terraform ≥ 1.0, `kubectl`
- An `az aks get-versions --location <region>`-valid `kubernetes_version`

## Deploy

```bash
cd terraform/aks
cp terraform.tfvars.example terraform.tfvars   # edit: acr_name must be globally unique

# One-time: create the shared remote-state backend (see "Remote state" below).
# Writes a gitignored backend.hcl into this module.
../bootstrap-backend.sh

terraform init -backend-config=backend.hcl
terraform plan -out=tfplan
terraform apply tfplan
```

### Deploy from Azure Cloud Shell (no local install)

These `.tf` files are **not** consumable by the Azure Portal UI — Terraform is
not ARM/Bicep, so there is no "upload template" path for them. The
browser-based equivalent is **Azure Cloud Shell** (the `>_` icon in the portal),
which ships Terraform and is already authenticated as your Azure user (no
`az login`):

```bash
# In Cloud Shell — get the module (clone the repo or upload the terraform/aks/ folder)
git clone https://github.com/berenguel/pgcyan.git
cd pgcyan/terraform/aks

cp terraform.tfvars.example terraform.tfvars   # edit: acr_name must be globally unique
../bootstrap-backend.sh                        # recommended: durable remote state
terraform init -backend-config=backend.hcl     # or just `terraform init` for throwaway local state
terraform apply
```

This runs the module exactly as written (Workload Identity, the managed
identities + federated credentials, ACR, etc.), so `terraform apply` re-runs and
`terraform destroy` behave as expected. Use the remote backend
(`bootstrap-backend.sh` + `-backend-config=backend.hcl`) if you want the state to
persist beyond the Cloud Shell session. Building
the same resources by hand in the portal is possible but error-prone (the
federated-credential `subject` and the `azurefile-csi` RWX class must match the
manifests exactly) and leaves no state — prefer Cloud Shell.

## After apply — wire up and deploy the manifests

1. **Get kubeconfig:**
   ```bash
   $(terraform output -raw get_credentials_command)
   ```

2. **Apply the base objects** (namespace + RBAC ServiceAccounts):
   ```bash
   kubectl apply -f ../../deploy/k8s/00-namespace.yaml
   kubectl apply -f ../../deploy/k8s/01-rbac.yaml
   ```

3. **Annotate the ServiceAccounts with the managed-identity client-ids.**
   `01-rbac.yaml` leaves `azure.workload.identity/client-id` as a TODO because the
   ids only exist post-apply. Run the generated helper (it uses the real ids):
   ```bash
   ./generated/apply-workload-identity.sh
   ```
   or annotate manually:
   ```bash
   kubectl annotate sa pgcyan-orchestrator -n pgcyan-migration \
     azure.workload.identity/client-id=$(terraform output -raw orchestrator_identity_client_id) --overwrite
   kubectl annotate sa pgcyan-worker -n pgcyan-migration \
     azure.workload.identity/client-id=$(terraform output -raw worker_identity_client_id) --overwrite
   ```

4. **Point the image at the ACR** — replace `REPLACE_ME/pgcyan` in the manifests
   with `$(terraform output -raw acr_login_server)/pgcyan`, and push your image
   there (`az acr login --name <acr>` then `docker push`).

5. **Grant the managed identities Postgres login.** Terraform creates the
   identities but cannot grant DB-side logins. Using the principal ids
   (`terraform output worker_identity_principal_id`), create Entra-authenticated
   roles on **both** source and target Postgres so the worker can authenticate
   with the token it exchanges. Until then, fall back to the Secret in
   `10-secrets.example.yaml`.

6. **Configure GitHub Actions OIDC.** Set the printed values as GitHub repo
   secrets (Settings → Secrets and variables → Actions):
   ```bash
   terraform output github_actions_client_id
   terraform output github_actions_tenant_id
   terraform output github_actions_subscription_id
   ```
   → `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_SUBSCRIPTION_ID`. Also set
   the repo variables `AKS_RESOURCE_GROUP` / `AKS_CLUSTER_NAME` to
   `terraform output resource_group_name` / `cluster_name` (and `ACR_NAME` if
   it differs from the workflow's `pgcyanacr` default). The federated
   credential trusts exactly `var.github_repo` (default `berenguel/pgcyan`) —
   change that variable and re-apply if you fork the repo.

## Remote state

This module's state is a blob (`key = "aks.tfstate"`) in the shared Azure Blob
backend created by [`../bootstrap-backend.sh`](../bootstrap-backend.sh) — the
same container the worker-VM module uses (`key = "worker-vm.tfstate"`). Run the
script once (`az login` first), then `terraform init -backend-config=backend.hcl`.
The generated `backend.hcl` is gitignored; only the per-module `key` is
committed. See [`../README.md`](../README.md#remote-state) for the full rationale
and the key-less (AAD) option.

## Defaults track production

The defaults in [`variables.tf`](variables.tf) are the values the **deployed**
cluster actually runs, not aspirational ones, and `terraform.tfvars.example`
mirrors them. A `terraform plan` with no `terraform.tfvars` at all should
therefore be empty.

This matters because `terraform.tfvars` is gitignored. The file used to create
this cluster was lost with the machine it lived on, and the example file that
replaced it disagreed with reality on six values — so a plan proposed destroying
the container registry (and every image in it), downgrading Kubernetes, and
switching the cluster to the paid SKU tier. Keeping the defaults honest means
the repo alone reproduces production, and tfvars is only for genuine
per-environment overrides.

Consequences worth knowing:

- **`acr_name` and `cluster_name` force replacement.** Changing `acr_name`
  destroys the registry and all images; the manifests' image references and
  `publish-image.yml` break with it.
- **`kubernetes_version` cannot go backwards.** AKS rejects downgrades, so a
  stale default makes `apply` fail — bump it after an AKS upgrade lands.
- **`node_vm_size` changes need `temporary_name_for_rotation`** set on the node
  pool, or the provider refuses to resize in place.
- **`sku_tier = "Standard"` is billed** per cluster-hour. The default is `Free`.

If the cluster drifts again (a portal change, an auto-upgrade), regenerate a
tfvars from live state rather than guessing:

```bash
terraform apply -refresh-only        # adopt reality into state; touches state only
./reconstruct-tfvars.sh > terraform.tfvars
terraform plan                       # should be empty
```

Then fold any values it reports into the `variables.tf` defaults and delete the
generated file, so the next person does not need it.

## Notes

- `service_cidr` must not overlap `vnet_address_space` or `pod_cidr`.
- The generated `apply-workload-identity.sh` embeds cluster-specific ids and is
  gitignored; it is re-rendered on every `terraform apply`.
- `admin_enabled = false` on the ACR — pulls go through AAD/AcrPull, no admin
  user+password (per the repo's no-static-credentials convention).
- Destroy with `terraform destroy` (tears down the whole resource group scope).
