variable "resource_group_name" {
  description = "Azure Resource Group name for the AKS cluster and its supporting resources"
  type        = string
  default     = "pg-cyan-aks-rg"
}

variable "azure_region" {
  description = "Azure region where resources will be deployed"
  type        = string
  default     = "westus2"
}

variable "cluster_name" {
  description = "Name of the AKS cluster (also used as a prefix for dependent resources)"
  type        = string
  default     = "pg-cyan-aks"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.cluster_name))
    error_message = "cluster_name must be 3-63 chars, lowercase alphanumeric or hyphens, and start/end alphanumeric."
  }
}

variable "kubernetes_version" {
  description = "Kubernetes version for the control plane and default node pool"
  type        = string
  default     = "1.33"
}

variable "sku_tier" {
  description = "AKS SKU tier. 'Standard' gives an SLA on the API server; 'Free' for dev/test."
  type        = string
  default     = "Standard"

  validation {
    condition     = contains(["Free", "Standard", "Premium"], var.sku_tier)
    error_message = "sku_tier must be one of: Free, Standard, Premium."
  }
}

variable "k8s_namespace" {
  description = "Kubernetes namespace the deploy/k8s manifests run in (must match 00-namespace.yaml)"
  type        = string
  default     = "pgcyan-migration"
}

# --- node pool ---
variable "node_vm_size" {
  description = "VM size for the default node pool"
  type        = string
  default     = "Standard_D4s_v5" # 4 vCPU / 16 GB — headroom for concurrent COPY shards
}

variable "node_min_count" {
  description = "Minimum node count for cluster autoscaler"
  type        = number
  default     = 2
}

variable "node_max_count" {
  description = "Maximum node count for cluster autoscaler (bounds concurrent shard fan-out)"
  type        = number
  default     = 6

  validation {
    condition     = var.node_max_count >= var.node_min_count
    error_message = "node_max_count must be >= node_min_count."
  }
}

variable "max_pods_per_node" {
  description = "Maximum pods per node"
  type        = number
  default     = 60
}

variable "node_os_disk_size_gb" {
  description = "OS disk size (GiB) for each node"
  type        = number
  default     = 128
}

# --- networking (Azure CNI Overlay) ---
variable "vnet_address_space" {
  description = "Address space for the AKS virtual network (node IPs)"
  type        = list(string)
  default     = ["10.10.0.0/16"]
}

variable "node_subnet_address_prefixes" {
  description = "Address prefixes for the node subnet"
  type        = list(string)
  default     = ["10.10.0.0/20"]
}

variable "pod_cidr" {
  description = "Overlay CIDR for pod IPs (not consumed from the VNet)"
  type        = string
  default     = "10.244.0.0/16"
}

variable "service_cidr" {
  description = "CIDR for Kubernetes ClusterIP services (must not overlap vnet/pod CIDRs)"
  type        = string
  default     = "10.0.0.0/16"
}

variable "dns_service_ip" {
  description = "Cluster DNS service IP (must be within service_cidr)"
  type        = string
  default     = "10.0.0.10"
}

# --- ACR ---
variable "acr_name" {
  description = "Azure Container Registry name (globally unique, alphanumeric only)"
  type        = string
  default     = "pgcyanacr"

  validation {
    condition     = can(regex("^[a-zA-Z0-9]{5,50}$", var.acr_name))
    error_message = "acr_name must be 5-50 alphanumeric characters (no hyphens)."
  }
}

variable "acr_sku" {
  description = "ACR SKU"
  type        = string
  default     = "Standard"

  validation {
    condition     = contains(["Basic", "Standard", "Premium"], var.acr_sku)
    error_message = "acr_sku must be one of: Basic, Standard, Premium."
  }
}

# --- GitHub Actions OIDC (CI/CD) ---
variable "github_repo" {
  description = "GitHub \"org/repo\" that publish-image.yml's `publish`/`deploy` jobs run in — the federated credential trusts only this exact repo"
  type        = string
  default     = "berenguel/pgcyan"

  validation {
    condition     = can(regex("^[^/]+/[^/]+$", var.github_repo))
    error_message = "github_repo must be in \"org/repo\" form, e.g. berenguel/pgcyan."
  }
}

# --- monitoring ---
variable "log_retention_in_days" {
  description = "Retention period for the Log Analytics workspace"
  type        = number
  default     = 30

  validation {
    condition     = var.log_retention_in_days >= 30 && var.log_retention_in_days <= 730
    error_message = "log_retention_in_days must be between 30 and 730."
  }
}

variable "environment_tags" {
  description = "Additional tags merged onto all resources"
  type        = map(string)
  default = {
    team = "database-migration"
  }
}
