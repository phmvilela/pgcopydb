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

2. **Initialize Terraform**
   ```bash
   terraform init
   ```

3. **Review deployment plan**
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
- State is tracked in `terraform.tfstate` (commit to version control with `.gitignore` rule)

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
