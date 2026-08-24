###############################################################################
# Infraestrutura da pipeline — Azure
#
# Recursos provisionados:
#   - Resource Group
#   - Storage Account com Data Lake Gen2 (namespace hierárquico) + containers
#     bronze / silver / gold, cada um com regra de ciclo de vida própria
#   - Azure Databricks Workspace (engine batch + streaming, Unity Catalog)
#   - Event Hubs Namespace + Hub com endpoint Kafka (perna streaming)
#   - Log Analytics + Action Group (observabilidade e alertas)
#
# Uso:
#   terraform init && terraform plan -var="ambiente=dev"
###############################################################################

terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

locals {
  prefixo = "alfabetizacao-${var.ambiente}"
  tags = {
    projeto     = "tech-challenge-fase2"
    dominio     = "educacao"
    ambiente    = var.ambiente
    responsavel = "engenharia-de-dados"
    # Tag de centro de custo: é o que permite rastrear gasto por domínio no
    # Azure Cost Management — pré-requisito de qualquer prática de FinOps.
    centro_custo = var.centro_custo
  }
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.prefixo}"
  location = var.regiao
  tags     = local.tags
}

# --------------------------------------------------------------------------- #
# Data Lake Gen2 — o lakehouse
# --------------------------------------------------------------------------- #
resource "azurerm_storage_account" "lake" {
  name                     = replace("st${local.prefixo}", "-", "")
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = var.replicacao # LRS em dev, ZRS/GRS em produção
  is_hns_enabled           = true           # Data Lake Gen2
  min_tls_version          = "TLS1_2"

  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 30
    }
  }
  tags = local.tags
}

resource "azurerm_storage_container" "camadas" {
  for_each              = toset(["bronze", "silver", "gold", "quarentena", "observabilidade"])
  name                  = each.key
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# FinOps: o histórico bruto envelhece para camadas mais baratas em vez de ficar
# em Hot para sempre. Silver e gold permanecem quentes porque são consultados.
resource "azurerm_storage_management_policy" "ciclo_vida" {
  storage_account_id = azurerm_storage_account.lake.id

  rule {
    name    = "bronze-envelhece"
    enabled = true
    filters {
      prefix_match = ["bronze/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 180
        delete_after_days_since_modification_greater_than          = 2555 # 7 anos
      }
    }
  }

  rule {
    name    = "quarentena-expira"
    enabled = true
    filters {
      prefix_match = ["quarentena/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = 90
      }
    }
  }
}

# --------------------------------------------------------------------------- #
# Databricks — engine de processamento
# --------------------------------------------------------------------------- #
resource "azurerm_databricks_workspace" "dbx" {
  name                = "dbw-${local.prefixo}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = var.ambiente == "prod" ? "premium" : "standard"
  tags                = local.tags
}

# Identidade gerenciada: o Databricks acessa o lake sem chave em código.
resource "azurerm_databricks_access_connector" "conector" {
  name                = "dbac-${local.prefixo}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }
  tags = local.tags
}

resource "azurerm_role_assignment" "dbx_no_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.conector.identity[0].principal_id
}

# --------------------------------------------------------------------------- #
# Event Hubs — ingestão streaming (protocolo Kafka)
# --------------------------------------------------------------------------- #
resource "azurerm_eventhub_namespace" "ehns" {
  name                = "evhns-${local.prefixo}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Standard" # Standard já expõe endpoint Kafka
  capacity            = var.throughput_units
  auto_inflate_enabled = true
  maximum_throughput_units = var.throughput_units * 4
  tags                = local.tags
}

resource "azurerm_eventhub" "eventos" {
  name              = "alfabetizacao.eventos.v1"
  namespace_id      = azurerm_eventhub_namespace.ehns.id
  partition_count   = 3
  message_retention = 7
}

# --------------------------------------------------------------------------- #
# Observabilidade
# --------------------------------------------------------------------------- #
resource "azurerm_log_analytics_workspace" "logs" {
  name                = "log-${local.prefixo}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

resource "azurerm_monitor_action_group" "alertas" {
  name                = "ag-${local.prefixo}"
  resource_group_name = azurerm_resource_group.rg.name
  short_name          = "alfabet"

  email_receiver {
    name          = "plantao-dados"
    email_address = var.email_alertas
  }
}

resource "azurerm_monitor_diagnostic_setting" "dbx_diag" {
  name                       = "diag-databricks"
  target_resource_id         = azurerm_databricks_workspace.dbx.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.logs.id

  enabled_log {
    category = "jobs"
  }
  enabled_log {
    category = "clusters"
  }
  metric {
    category = "AllMetrics"
  }
}

# FinOps: orçamento com alerta em 80% e 100% do teto mensal.
resource "azurerm_consumption_budget_resource_group" "orcamento" {
  name              = "budget-${local.prefixo}"
  resource_group_id = azurerm_resource_group.rg.id
  amount            = var.orcamento_mensal_usd
  time_grain        = "Monthly"

  time_period {
    start_date = var.inicio_orcamento
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    contact_emails = [var.email_alertas]
  }
  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThan"
    contact_emails = [var.email_alertas]
  }
}
