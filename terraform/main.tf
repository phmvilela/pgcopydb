terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Resource Group
resource "azurerm_resource_group" "pg_cyan" {
  name     = var.resource_group_name
  location = var.azure_region

  tags = {
    environment = "migration"
    project     = "pg_cyan"
    managed_by  = "terraform"
  }
}

# Virtual Network
resource "azurerm_virtual_network" "pg_cyan" {
  name                = "${var.vm_name}-vnet"
  address_space       = var.vnet_address_space
  location            = azurerm_resource_group.pg_cyan.location
  resource_group_name = azurerm_resource_group.pg_cyan.name

  tags = azurerm_resource_group.pg_cyan.tags
}

# Subnet
resource "azurerm_subnet" "pg_cyan" {
  name                 = "${var.vm_name}-subnet"
  resource_group_name  = azurerm_resource_group.pg_cyan.name
  virtual_network_name = azurerm_virtual_network.pg_cyan.name
  address_prefixes     = var.subnet_address_prefixes
}

# Network Security Group
resource "azurerm_network_security_group" "pg_cyan" {
  name                = "${var.vm_name}-nsg"
  location            = azurerm_resource_group.pg_cyan.location
  resource_group_name = azurerm_resource_group.pg_cyan.name

  ingress_rule {
    name                       = "AllowSSH"
    priority                   = 1000
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.allowed_ssh_cidr
    destination_address_prefix = "*"
  }

  ingress_rule {
    name                       = "AllowPostgreSQL"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5432"
    source_address_prefix      = var.allowed_postgres_cidr
    destination_address_prefix = "*"
  }

  egress_rule {
    name                       = "AllowOutbound"
    priority                   = 1000
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  tags = azurerm_resource_group.pg_cyan.tags
}

# Public IP
resource "azurerm_public_ip" "pg_cyan" {
  name                = "${var.vm_name}-pip"
  location            = azurerm_resource_group.pg_cyan.location
  resource_group_name = azurerm_resource_group.pg_cyan.name
  allocation_method   = "Static"
  sku                 = "Standard"

  tags = azurerm_resource_group.pg_cyan.tags
}

# Network Interface
resource "azurerm_network_interface" "pg_cyan" {
  name                = "${var.vm_name}-nic"
  location            = azurerm_resource_group.pg_cyan.location
  resource_group_name = azurerm_resource_group.pg_cyan.name

  ip_configuration {
    name                          = "testconfiguration1"
    subnet_id                     = azurerm_subnet.pg_cyan.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.pg_cyan.id
  }

  tags = azurerm_resource_group.pg_cyan.tags
}

# Associate NSG with NIC
resource "azurerm_network_interface_security_group_association" "pg_cyan" {
  network_interface_id      = azurerm_network_interface.pg_cyan.id
  network_security_group_id = azurerm_network_security_group.pg_cyan.id
}

# Virtual Machine
resource "azurerm_linux_virtual_machine" "pg_cyan" {
  name                = var.vm_name
  location            = azurerm_resource_group.pg_cyan.location
  resource_group_name = azurerm_resource_group.pg_cyan.name
  size                = var.vm_size

  admin_username = var.admin_username

  # Disable password auth; use SSH keys
  disable_password_authentication = true

  admin_ssh_key {
    username   = var.admin_username
    public_key = file(var.ssh_public_key_path)
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"  # Ubuntu 22.04 LTS
    version   = "latest"
  }

  network_interface_ids = [
    azurerm_network_interface.pg_cyan.id,
  ]

  # Cloud-init script for provisioning
  custom_data = base64encode(templatefile("${path.module}/cloud-init.sh", {
    postgresql_version = var.postgresql_version
  }))

  tags = azurerm_resource_group.pg_cyan.tags

  depends_on = [azurerm_resource_group.pg_cyan]
}
