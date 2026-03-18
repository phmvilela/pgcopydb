output "vm_public_ip" {
  description = "Public IP address of the migration worker VM"
  value       = azurerm_public_ip.pg_cyan.ip_address
}

output "vm_private_ip" {
  description = "Private IP address of the migration worker VM"
  value       = azurerm_network_interface.pg_cyan.private_ip_address
}

output "vm_fqdn" {
  description = "Fully qualified domain name of the VM"
  value       = azurerm_public_ip.pg_cyan.fqdn
}

output "resource_group_name" {
  description = "Name of the resource group"
  value       = azurerm_resource_group.pg_cyan.name
}

output "resource_group_id" {
  description = "ID of the resource group"
  value       = azurerm_resource_group.pg_cyan.id
}

output "vnet_id" {
  description = "ID of the virtual network"
  value       = azurerm_virtual_network.pg_cyan.id
}

output "subnet_id" {
  description = "ID of the subnet"
  value       = azurerm_subnet.pg_cyan.id
}

output "ssh_command" {
  description = "Command to SSH into the VM"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.pg_cyan.ip_address}"
}

output "provisioning_status" {
  description = "VM provisioning status message"
  value       = "VM provisioned. SSH available at: ssh ${var.admin_username}@${azurerm_public_ip.pg_cyan.ip_address}. PostgreSQL ${var.postgresql_version} installed on port 5432."
}
