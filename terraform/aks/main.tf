terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.60"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }
}

provider "azurerm" {
  features {}
}

locals {
  # Common tags applied to every resource. Mirrors the tagging shape used by the
  # worker-VM module (../main.tf) so both modules read consistently.
  tags = merge(
    {
      environment = "migration"
      project     = "pg_cyan"
      managed_by  = "terraform"
      component   = "aks"
    },
    var.environment_tags,
  )

  # Kubernetes ServiceAccounts the deploy/k8s manifests bind to. The federated
  # identity credentials below MUST match these exactly (namespace + name) or
  # Workload Identity token exchange fails at pod start.
  #
  # Subject format is the OIDC subject AKS mints for a pod SA token:
  #   system:serviceaccount:<namespace>:<serviceaccount-name>
  k8s_namespace        = var.k8s_namespace
  orchestrator_sa_name = "pgcyan-orchestrator"
  worker_sa_name       = "pgcyan-worker"
  federated_audience   = "api://AzureADTokenExchange"
}

# ---------------------------------------------------------------------------
# Resource Group
# ---------------------------------------------------------------------------
resource "azurerm_resource_group" "aks" {
  name     = var.resource_group_name
  location = var.azure_region
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# Networking (Azure CNI Overlay)
# ---------------------------------------------------------------------------
# Node IPs come from the subnet; pod IPs come from the overlay pod_cidr and are
# NOT consumed from the VNet. This keeps VNet address exhaustion off the table
# even with many concurrent shard pods.
resource "azurerm_virtual_network" "aks" {
  name                = "${var.cluster_name}-vnet"
  address_space       = var.vnet_address_space
  location            = azurerm_resource_group.aks.location
  resource_group_name = azurerm_resource_group.aks.name
  tags                = local.tags
}

resource "azurerm_subnet" "aks_nodes" {
  name                 = "${var.cluster_name}-node-subnet"
  resource_group_name  = azurerm_resource_group.aks.name
  virtual_network_name = azurerm_virtual_network.aks.name
  address_prefixes     = var.node_subnet_address_prefixes
}

# ---------------------------------------------------------------------------
# Log Analytics + Container Insights (monitoring)
# ---------------------------------------------------------------------------
resource "azurerm_log_analytics_workspace" "aks" {
  name                = "${var.cluster_name}-logs"
  location            = azurerm_resource_group.aks.location
  resource_group_name = azurerm_resource_group.aks.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_in_days
  tags                = local.tags
}

# ---------------------------------------------------------------------------
# Azure Container Registry (hosts the REPLACE_ME/pgcyan image)
# ---------------------------------------------------------------------------
resource "azurerm_container_registry" "aks" {
  name                = var.acr_name
  resource_group_name = azurerm_resource_group.aks.name
  location            = azurerm_resource_group.aks.location
  sku                 = var.acr_sku
  admin_enabled       = false # prefer AAD/AcrPull over admin user+password
  tags                = local.tags
}

# ---------------------------------------------------------------------------
# AKS cluster
# ---------------------------------------------------------------------------
# The two capabilities the deploy/k8s manifests hard-depend on:
#   * oidc_issuer_enabled     -> issues the SA token AKS federates to Entra
#   * workload_identity_enabled -> injects the token into pods labelled
#                                  azure.workload.identity/use: "true"
# Both are required for 01-rbac.yaml's passwordless DB auth story to work.
resource "azurerm_kubernetes_cluster" "aks" {
  name                = var.cluster_name
  location            = azurerm_resource_group.aks.location
  resource_group_name = azurerm_resource_group.aks.name
  dns_prefix          = var.cluster_name
  kubernetes_version  = var.kubernetes_version
  sku_tier            = var.sku_tier

  # --- Workload Identity: the whole point of this ticket ---
  oidc_issuer_enabled       = true
  workload_identity_enabled = true

  default_node_pool {
    name                 = "system"
    vm_size              = var.node_vm_size
    vnet_subnet_id       = azurerm_subnet.aks_nodes.id
    orchestrator_version = var.kubernetes_version
    enable_auto_scaling  = true
    min_count            = var.node_min_count
    max_count            = var.node_max_count
    max_pods             = var.max_pods_per_node
    os_disk_size_gb      = var.node_os_disk_size_gb
    tags                 = local.tags
  }

  # Control-plane identity. Kubelet identity is derived from this and is what
  # actually pulls from ACR (see the AcrPull assignment below).
  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin      = "azure"
    network_plugin_mode = "overlay"
    network_policy      = "calico"
    load_balancer_sku   = "standard"
    pod_cidr            = var.pod_cidr
    service_cidr        = var.service_cidr
    dns_service_ip      = var.dns_service_ip
  }

  # Azure Files CSI driver provides the ReadWriteMany StorageClass
  # (`azurefile-csi`) that 20-pvc-workdir.yaml claims. It is enabled by default
  # on AKS; we set it explicitly so the RWX guarantee is not left implicit.
  storage_profile {
    file_driver_enabled = true
    disk_driver_enabled = true
  }

  # Container Insights -> Log Analytics.
  oms_agent {
    log_analytics_workspace_id = azurerm_log_analytics_workspace.aks.id
  }

  tags = local.tags
}

# Let the cluster's kubelet identity pull images from the registry so
# `REPLACE_ME/pgcyan:latest` (repointed at this ACR) resolves without imagePullSecrets.
resource "azurerm_role_assignment" "kubelet_acr_pull" {
  scope                            = azurerm_container_registry.aks.id
  role_definition_name             = "AcrPull"
  principal_id                     = azurerm_kubernetes_cluster.aks.kubelet_identity[0].object_id
  skip_service_principal_aad_check = true
}

# ---------------------------------------------------------------------------
# Workload Identity: user-assigned managed identities + federated credentials
# ---------------------------------------------------------------------------
# One identity per ServiceAccount. The federated credential ties an Entra
# managed identity to a specific (issuer, subject, audience) triple so that a
# pod running as that SA can exchange its projected SA token for an Entra token
# — no static DB password in a Secret.

# --- orchestrator: the control plane (planner Job / DAG driver) ---
resource "azurerm_user_assigned_identity" "orchestrator" {
  name                = "${var.cluster_name}-orchestrator"
  resource_group_name = azurerm_resource_group.aks.name
  location            = azurerm_resource_group.aks.location
  tags                = local.tags
}

resource "azurerm_federated_identity_credential" "orchestrator" {
  name                = "${var.cluster_name}-orchestrator-fedcred"
  resource_group_name = azurerm_resource_group.aks.name
  parent_id           = azurerm_user_assigned_identity.orchestrator.id
  audience            = [local.federated_audience]
  issuer              = azurerm_kubernetes_cluster.aks.oidc_issuer_url
  subject             = "system:serviceaccount:${local.k8s_namespace}:${local.orchestrator_sa_name}"
}

# --- worker: the data plane (every pgcopydb Job/pod) ---
resource "azurerm_user_assigned_identity" "worker" {
  name                = "${var.cluster_name}-worker"
  resource_group_name = azurerm_resource_group.aks.name
  location            = azurerm_resource_group.aks.location
  tags                = local.tags
}

resource "azurerm_federated_identity_credential" "worker" {
  name                = "${var.cluster_name}-worker-fedcred"
  resource_group_name = azurerm_resource_group.aks.name
  parent_id           = azurerm_user_assigned_identity.worker.id
  audience            = [local.federated_audience]
  issuer              = azurerm_kubernetes_cluster.aks.oidc_issuer_url
  subject             = "system:serviceaccount:${local.k8s_namespace}:${local.worker_sa_name}"
}

# ---------------------------------------------------------------------------
# Wire the MI client-ids back into the deploy/k8s manifests
# ---------------------------------------------------------------------------
# 01-rbac.yaml carries the SA annotations as TODO comments because the client-ids
# do not exist until apply time. Rather than mutate the committed manifest, we
# render a ready-to-run patch script + a values file the operator applies once
# after `terraform apply`. See README for the one-liner.
resource "local_file" "workload_identity_patch" {
  filename        = "${path.module}/generated/apply-workload-identity.sh"
  file_permission = "0755"
  content         = <<-EOT
    #!/usr/bin/env bash
    # GENERATED by terraform/aks — do not edit. Re-run `terraform apply` to refresh.
    #
    # Annotates the pgcyan ServiceAccounts with the managed-identity client-ids
    # so Azure Workload Identity can mint DB tokens. Run once after applying the
    # deploy/k8s manifests (namespace + 01-rbac.yaml must already exist):
    #
    #   az aks get-credentials -g ${azurerm_resource_group.aks.name} -n ${var.cluster_name}
    #   kubectl apply -f deploy/k8s/00-namespace.yaml -f deploy/k8s/01-rbac.yaml
    #   ./apply-workload-identity.sh
    set -euo pipefail

    kubectl annotate serviceaccount ${local.orchestrator_sa_name} \
      -n ${local.k8s_namespace} \
      azure.workload.identity/client-id=${azurerm_user_assigned_identity.orchestrator.client_id} \
      --overwrite

    kubectl annotate serviceaccount ${local.worker_sa_name} \
      -n ${local.k8s_namespace} \
      azure.workload.identity/client-id=${azurerm_user_assigned_identity.worker.client_id} \
      --overwrite

    echo "Annotated ${local.orchestrator_sa_name} and ${local.worker_sa_name} in ${local.k8s_namespace}."
  EOT
}
