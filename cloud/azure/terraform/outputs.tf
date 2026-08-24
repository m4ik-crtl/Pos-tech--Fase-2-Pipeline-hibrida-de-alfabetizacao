output "databricks_workspace_url" {
  description = "URL do workspace Databricks."
  value       = "https://${azurerm_databricks_workspace.dbx.workspace_url}"
}

output "lakehouse_abfss" {
  description = "URI base do lakehouse — vira LAKEHOUSE_URI na aplicação."
  value       = "abfss://gold@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}

output "kafka_bootstrap_servers" {
  description = "Endpoint Kafka do Event Hubs — vira KAFKA_BOOTSTRAP_SERVERS."
  value       = "${azurerm_eventhub_namespace.ehns.name}.servicebus.windows.net:9093"
}

output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.logs.workspace_id
}
