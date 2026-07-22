variable "resource_group_name" {
  description = "Azure Resource Group name"
  type        = string
  default     = "pg-cyan-rg"
}

variable "azure_region" {
  description = "Azure region where resources will be deployed"
  type        = string
  default     = "westus2"
}

variable "vm_name" {
  description = "Name of the migration worker VM"
  type        = string
  default     = "pg-cyan-worker"
}

variable "vm_size" {
  description = "Azure VM size (minimal resources for now, scale later)"
  type        = string
  default     = "Standard_B2s"  # 2 vCPU, 4 GB RAM
  
  validation {
    condition     = can(regex("^Standard_B[1-4][a-z]*$|^Standard_D[2-4][a-z]*$", var.vm_size))
    error_message = "VM size must be a suitable Azure size (e.g., Standard_B2s, Standard_B4ms, Standard_D2s_v3)."
  }
}

variable "admin_username" {
  description = "Admin username for SSH access"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key_path" {
  description = "Path to public SSH key file for VM authentication"
  type        = string
  sensitive   = false
}

variable "postgresql_version" {
  description = "PostgreSQL version to install on the VM"
  type        = string
  default     = "15"
  
  validation {
    condition     = can(regex("^(12|13|14|15|16)$", var.postgresql_version))
    error_message = "PostgreSQL version must be one of: 12, 13, 14, 15, 16."
  }
}

variable "vnet_address_space" {
  description = "Address space for the virtual network"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "subnet_address_prefixes" {
  description = "Address prefixes for the subnet"
  type        = list(string)
  default     = ["10.0.1.0/24"]
}

variable "allowed_ssh_cidr" {
  description = "CIDR range allowed for SSH access"
  type        = string
  default     = "*"  # WARNING: Change to specific IPs in production (e.g., 203.0.113.0/24)
}

variable "allowed_postgres_cidr" {
  description = "CIDR range allowed for PostgreSQL access"
  type        = string
  default     = "10.0.0.0/16"  # Internal VNet only by default
}

variable "environment_tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default = {
    project = "pg_cyan"
    team    = "migration"
  }
}
