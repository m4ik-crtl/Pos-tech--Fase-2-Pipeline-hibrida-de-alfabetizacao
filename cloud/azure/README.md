# Implantação em nuvem — Azure Databricks

O projeto foi desenhado para que **o código não mude** entre a máquina local e a
nuvem. O que muda são variáveis de ambiente.

| Componente local | Equivalente em produção | Como troca |
|---|---|---|
| `./data/lakehouse` | Azure Data Lake Storage Gen2 | `LAKEHOUSE_URI=abfss://...` |
| Kafka no Docker | Azure Event Hubs (endpoint Kafka) | `KAFKA_BOOTSTRAP_SERVERS=...servicebus.windows.net:9093` |
| `local[*]` | Cluster Databricks (Photon + autoscale) | `SPARK_MASTER` definido pelo runtime |
| Parquet (fallback) | Delta Lake | `FORMATO_TABELA=delta` |
| `relatorio.md` | Azure Monitor / Log Analytics | `LOG_FORMATO=json` |

## Por que Azure Databricks

O desafio pede uma nuvem; a disciplina trabalha Databricks e arquitetura
medalhão. Entre os três provedores, **Azure Databricks é a única oferta
first-party** — o serviço é vendido, faturado e suportado pela própria
Microsoft, com integração nativa a ADLS Gen2, Entra ID e Unity Catalog. Some-se
a isso o fato de o **Event Hubs falar o protocolo Kafka**: o mesmo
`spark.readStream.format("kafka")` que roda contra o broker do `docker compose`
roda contra o Event Hubs trocando apenas o endpoint. Nenhuma linha de código de
streaming precisa ser reescrita para ir para produção.

## Passo a passo

```bash
# 1. Infraestrutura
cd cloud/azure/terraform
terraform init
terraform plan  -var="ambiente=dev"
terraform apply -var="ambiente=dev"

# 2. Empacotar o projeto e enviar
python -m build                          # gera dist/alfabetizacao-1.0.0-py3-none-any.whl
databricks fs cp dist/*.whl dbfs:/artefatos/

# 3. Criar os jobs
databricks jobs create --json @../databricks/job_batch.json
databricks jobs create --json @../databricks/job_streaming.json
```

## Segurança e governança

- **Sem credencial em código**: o acesso ao lake usa o *Access Connector* com
  identidade gerenciada (`azurerm_databricks_access_connector`), não chave de
  conta.
- **Unity Catalog** organiza `bronze`, `silver`, `gold` como schemas com
  permissão por grupo: analista lê Gold, engenharia escreve Silver, ninguém
  edita Bronze.
- **Dados pessoais**: a base não contém identificação de aluno. O microdado é
  pseudonimizado com SHA-256 na origem (`src/ingestao/preparar_raw.py`) e o
  identificador não é reversível — a Gold só publica agregados municipais.
- **Retenção**: bronze envelhece para Cool aos 30 dias e Archive aos 180
  (política de ciclo de vida no Terraform); quarentena expira em 90 dias.
