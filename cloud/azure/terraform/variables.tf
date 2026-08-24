variable "ambiente" {
  description = "Ambiente alvo (dev, hml, prod)."
  type        = string
  default     = "dev"
}

variable "regiao" {
  description = "Região do Azure. brazilsouth mantém o dado em território nacional."
  type        = string
  default     = "brazilsouth"
}

variable "replicacao" {
  description = "Redundância do storage: LRS em dev, ZRS ou GRS em produção."
  type        = string
  default     = "LRS"
}

variable "throughput_units" {
  description = "Throughput Units do Event Hubs (1 TU = 1 MB/s de entrada)."
  type        = number
  default     = 1
}

variable "email_alertas" {
  description = "Destino dos alertas de falha e de estouro de orçamento."
  type        = string
  default     = "plantao-dados@exemplo.gov.br"
}

variable "orcamento_mensal_usd" {
  description = "Teto mensal de gasto monitorado pelo Azure Cost Management."
  type        = number
  default     = 500
}

variable "inicio_orcamento" {
  description = "Início do período de orçamento (YYYY-MM-01T00:00:00Z)."
  type        = string
  default     = "2026-01-01T00:00:00Z"
}

variable "centro_custo" {
  description = "Centro de custo para rateio no Cost Management."
  type        = string
  default     = "educacao-basica"
}
