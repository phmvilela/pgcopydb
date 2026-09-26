# pg_cyan Terraform

One root module lives here:

- [**`aks/`**](aks/) — the **AKS cluster** that runs the sharded pgcopydb
  migration in [`deploy/k8s`](../deploy/k8s/): OIDC issuer + Workload Identity,
  the managed identities and federated credentials the ServiceAccounts bind to,
  an ACR, Log Analytics/Container Insights, and Azure CNI Overlay networking.
  See [`aks/README.md`](aks/README.md) for prerequisites, deploy steps and the
  post-apply wiring.

This directory holds only the shared state-backend bootstrap
([`bootstrap-backend.sh`](./bootstrap-backend.sh)); it is not itself a Terraform
module.

## Remote state

`terraform/aks/` stores its state as a blob (`key = "aks.tfstate"`) in an Azure
Storage container created by [`bootstrap-backend.sh`](./bootstrap-backend.sh):

```bash
az login
./bootstrap-backend.sh          # idempotent: creates the RG + storage account +
                                # container and writes aks/backend.hcl
cd aks && terraform init -backend-config=backend.hcl
```

- The storage account name is derived from your subscription id, so the script
  is safe to re-run anywhere and always targets the same state. That is what
  keeps a rebuilt dev container or a fresh clone from orphaning Azure resources.
- `backend.hcl` is **gitignored** (env-specific, like `*.tfvars`); only the
  state `key` is committed, in the module's `backend "azurerm"` block.
  [`backend.hcl.example`](./backend.hcl.example) documents the shape.
- The container has blob versioning and 30-day soft delete enabled, so a botched
  apply cannot irrecoverably clobber state history.
- Migrating existing local state up to the backend: add `-migrate-state` to the
  first `terraform init -backend-config=backend.hcl`.
- The backend uses the storage account key, fetched via ARM at runtime — it never
  touches the repo. To go key-less, set `use_azuread_auth = true` in the backend
  block and grant yourself **Storage Blob Data Contributor** on the account.

## Applying safely

The module's committed defaults are the values the deployed cluster actually
runs, so `terraform plan` with no `terraform.tfvars` should report no changes.
Do not copy `terraform.tfvars.example` expecting to need it, and read
[`aks/README.md`](aks/README.md#defaults-track-production) before changing a
default — some of them force replacement of the cluster or the registry.

## History

A second root module used to live here, provisioning a standalone **worker VM**
(resource group, VNet, NSG, public IP, VM, cloud-init). It was removed in favour
of the AKS module: the migration runs as Kubernetes Jobs, and that module had
never been applied — its `azurerm_network_security_group` used `ingress_rule` /
`egress_rule` blocks, which is `aws_security_group` syntax, so it failed to parse.
Its state key (`worker-vm.tfstate`) is unused.
