output "resource_group_name" {
  description = "Name of the resource group holding the cluster"
  value       = azurerm_resource_group.aks.name
}

output "cluster_name" {
  description = "Name of the AKS cluster"
  value       = azurerm_kubernetes_cluster.aks.name
}

output "get_credentials_command" {
  description = "Command to fetch kubeconfig for the cluster"
  value       = "az aks get-credentials --resource-group ${azurerm_resource_group.aks.name} --name ${azurerm_kubernetes_cluster.aks.name}"
}

output "oidc_issuer_url" {
  description = "OIDC issuer URL of the cluster (subject of the federated credentials)"
  value       = azurerm_kubernetes_cluster.aks.oidc_issuer_url
}

output "kube_config" {
  description = "Raw kubeconfig for the cluster"
  value       = azurerm_kubernetes_cluster.aks.kube_config_raw
  sensitive   = true
}

# --- Workload Identity client-ids: fill the SA annotations in 01-rbac.yaml ---
output "orchestrator_identity_client_id" {
  description = "Client ID for the azure.workload.identity/client-id annotation on the pgcyan-orchestrator ServiceAccount"
  value       = azurerm_user_assigned_identity.orchestrator.client_id
}

output "worker_identity_client_id" {
  description = "Client ID for the azure.workload.identity/client-id annotation on the pgcyan-worker ServiceAccount"
  value       = azurerm_user_assigned_identity.worker.client_id
}

output "orchestrator_identity_principal_id" {
  description = "Principal (object) ID of the orchestrator managed identity — grant this login on the target Postgres if the orchestrator needs DB access"
  value       = azurerm_user_assigned_identity.orchestrator.principal_id
}

output "worker_identity_principal_id" {
  description = "Principal (object) ID of the worker managed identity — grant this login on BOTH source and target Postgres (Entra auth)"
  value       = azurerm_user_assigned_identity.worker.principal_id
}

# --- ACR ---
output "acr_login_server" {
  description = "ACR login server — repoint the REPLACE_ME/pgcyan image at this (e.g. <login_server>/pgcyan:latest)"
  value       = azurerm_container_registry.aks.login_server
}

output "workload_identity_patch_script" {
  description = "Path to the generated script that annotates the ServiceAccounts with the MI client-ids"
  value       = local_file.workload_identity_patch.filename
}

# --- GitHub Actions OIDC (CI/CD): set these as GitHub repo secrets ---
output "github_actions_client_id" {
  description = "Set as the GitHub repo secret AZURE_CLIENT_ID"
  value       = azurerm_user_assigned_identity.github_actions.client_id
}

output "github_actions_tenant_id" {
  description = "Set as the GitHub repo secret AZURE_TENANT_ID"
  value       = data.azurerm_client_config.current.tenant_id
}

output "github_actions_subscription_id" {
  description = "Set as the GitHub repo secret AZURE_SUBSCRIPTION_ID"
  value       = data.azurerm_client_config.current.subscription_id
}
