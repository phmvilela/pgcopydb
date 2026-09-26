#!/usr/bin/env bash
# Reconstruct terraform.tfvars for terraform/aks from the live cluster, via
# Terraform state (provider-native field names -- no az-CLI translation).
#
#   cd terraform/aks
#   terraform apply -refresh-only        # adopt reality into state (state only, no infra change)
#   ./reconstruct-tfvars.sh > terraform.tfvars
#
# Pass a JSON file to read that instead of running terraform (for testing).
set -euo pipefail
if [ $# -ge 1 ]; then SRC=$(cat "$1"); else SRC=$(terraform show -json); fi
printf '%s' "$SRC" | jq -r '
  ((.values.root_module.resources // [])
    | map({key: .address, value: .values}) | from_entries) as $r
| ($r["azurerm_kubernetes_cluster.aks"]        // {}) as $k
| ($k.default_node_pool[0]                     // {}) as $np
| ($k.network_profile[0]                       // {}) as $net
| ($r["azurerm_container_registry.aks"]        // {}) as $acr
| ($r["azurerm_log_analytics_workspace.aks"]   // {}) as $law
| ($r["azurerm_virtual_network.aks"]           // {}) as $vnet
| ($r["azurerm_subnet.aks_nodes"]              // {}) as $subnet
| (($r["azurerm_federated_identity_credential.worker"] // {}).subject // "") as $wsub
| (($r["azurerm_federated_identity_credential.github_actions"] // {}).subject // "") as $gsub
| ($k.tags // {} | with_entries(select(
      ((.key | IN("project","managed_by","component")) | not)
      and ((.key == "environment" and .value == "migration") | not)
  ))) as $envtags
| "# Reconstructed from live state on \(now | todate) - gitignored, do not commit.",
  "resource_group_name = \"\($k.resource_group_name)\"",
  "azure_region        = \"\($k.location)\"",
  "cluster_name        = \"\($k.name)\"",
  "kubernetes_version  = \"\($k.kubernetes_version)\"",
  "sku_tier            = \"\($k.sku_tier)\"",
  "",
  "k8s_namespace = \"\($wsub | split(":")[2] // "CHECK-ME")\"",
  "github_repo   = \"\($gsub | ltrimstr("repo:") | split(":")[0] // "CHECK-ME")\"",
  "",
  "node_vm_size         = \"\($np.vm_size)\"",
  "max_pods_per_node    = \($np.max_pods)",
  "node_os_disk_size_gb = \($np.os_disk_size_gb)",
  "",
  "# LIVE: enable_auto_scaling=\($np.enable_auto_scaling) node_count=\($np.node_count) min=\($np.min_count) max=\($np.max_count)",
  "# main.tf hardcodes enable_auto_scaling=true, so this cannot be made drift-free via tfvars alone.",
  "node_min_count = \(if ($np.min_count // 0) > 0 then $np.min_count else ($np.node_count // 2) end)",
  "node_max_count = \(if ($np.max_count // 0) > 0 then $np.max_count else ((($np.node_count // 2) * 3)) end)",
  "",
  "vnet_address_space           = \($vnet.address_space // [] | tojson)",
  "node_subnet_address_prefixes = \($subnet.address_prefixes // [] | tojson)",
  "pod_cidr       = \"\($net.pod_cidr)\"",
  "service_cidr   = \"\($net.service_cidr)\"",
  "dns_service_ip = \"\($net.dns_service_ip)\"",
  "",
  "acr_name = \"\($acr.name)\"",
  "acr_sku  = \"\($acr.sku)\"",
  "",
  "log_retention_in_days = \($law.retention_in_days)",
  "",
  "environment_tags = {",
  ($envtags | to_entries | map("  \(.key) = \"\(.value)\"") | .[]),
  "}"
'
