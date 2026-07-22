# pg_cyan Azure Terraform Deployment Guide

## Prerequisites

1. **Azure CLI**: Install and authenticate
   ```bash
   az login
   ```

2. **Terraform**: Install v1.0+
   ```bash
   terraform version
   ```

3. **SSH Key**: Generate a key pair for VM access
   ```bash
   ssh-keygen -t rsa -b 4096 -f ~/.ssh/pg_cyan_rsa
   ```

## Setup

1. **Copy and customize variables**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars with your specifics
   ```

2. **Bootstrap the remote state backend** (one-time per subscription; creates
   the shared Azure Blob backend — see [Remote state](#remote-state)). It writes
   a gitignored `backend.hcl` into this module:
   ```bash
   ./bootstrap-backend.sh
   ```

3. **Initialize Terraform** against that backend:
   ```bash
   terraform init -backend-config=backend.hcl
   ```

4. **Review deployment plan**
   ```bash
   terraform plan -out=tfplan
   ```

## Deployment

```bash
# Apply configuration
terraform apply tfplan

# Terraform will output:
# - vm_public_ip: Public IP of the worker VM
# - ssh_command: SSH connection command
# - PostgreSQL version installed
```

## Post-Deployment

1. **Access the VM**
   ```bash
   ssh azureuser@<VM_PUBLIC_IP>
   ```

2. **Verify PostgreSQL installation**
   ```bash
   psql --version
   sudo systemctl status postgresql
   ```

3. **Test PostgreSQL connection** (if server mode):
   ```bash
   psql -h localhost -U postgres -c "SELECT version();"
   ```

4. **Deploy pg_cyan application**
   ```bash
   # Clone repo or deploy Docker container
   docker pull <your-registry>/pg_cyan:latest
   docker run -e PG_SOURCE_HOST=... -e PG_TARGET_HOST=... pg_cyan preflight
   ```

## Cleanup

```bash
# Destroy all resources
terraform destroy
```

## Security Notes

⚠️ **Production Checklist:**
- [ ] Restrict `allowed_ssh_cidr` to specific IPs (not `*`)
- [ ] Use Azure Key Vault for secrets (not `.tfvars`)
- [ ] Enable SSH host verification
- [ ] Set up Azure Monitor/Log Analytics
- [ ] Configure backup policies for the VM
- [ ] Use managed identities for Azure API access
- [ ] Review NSG rules for PostgreSQL access

## Idempotency & Safety

This Terraform template is idempotent:
- Running `terraform apply` multiple times is safe
- Resource dependencies are handled automatically
- State lives in the shared remote backend (see below), **not** a local
  `terraform.tfstate` — so a rebuilt dev container or a fresh clone never
  orphans Azure resources

## Remote state

Both root modules (`terraform/` and `terraform/aks/`) store state as separate
blobs in **one Azure Storage container**, created by
[`bootstrap-backend.sh`](./bootstrap-backend.sh):

```bash
az login
./bootstrap-backend.sh          # idempotent: creates the RG + storage account +
                                # container and writes backend.hcl into each module
terraform init -backend-config=backend.hcl
```

- The storage account name is derived from your subscription id, so the script
  is safe to re-run anywhere and always targets the same state.
- `backend.hcl` is **gitignored** (env-specific, like `*.tfvars`); only the
  per-module state `key` is committed, in each module's `backend "azurerm"`
  block. `backend.hcl.example` documents the shape.
- Migrating existing local state up to the backend: add `-migrate-state` to the
  first `terraform init -backend-config=backend.hcl`.
- The backend uses the storage account key, fetched via ARM at runtime — it
  never touches the repo. To go key-less, set `use_azuread_auth = true` in the
  backend block and grant yourself **Storage Blob Data Contributor** on the
  account.

## Scaling

After initial provisioning, scale the VM:
```bash
# Update terraform.tfvars
vm_size = "Standard_B4ms"  # or Standard_D2s_v3, etc.

# Apply the change
terraform apply
```

---

For issues, check Azure Resource Group logs and VM serial console output.
