# Estimativa de custo — arquitetura em Azure Databricks

Base observada na última execução local: **12.9 MB** gravados, **175s** de processamento, **205,482** registros publicados.

| Cenário | Volume | Armazenamento | Batch | Streaming | Event Hubs | **Total/mês (USD)** | **Total/mês (BRL)** |
|---|---:|---:|---:|---:|---:|---:|---:|
| Piloto (uma UF) | 0.1 GB | US$ 0.0 | US$ 3.58 | US$ 59.04 | US$ 21.9 | **US$ 84.52** | **R$ 456.42** |
| Produção nacional | 1.01 GB | US$ 0.02 | US$ 71.61 | US$ 354.24 | US$ 21.9 | **US$ 447.77** | **R$ 2417.96** |
| Pico de divulgação de resultados | 2.42 GB | US$ 0.04 | US$ 687.47 | US$ 354.24 | US$ 43.8 | **US$ 1085.55** | **R$ 5861.99** |

> Preços de referência declarados em `config/pipeline.yml` (câmbio R$ 5.4/US$). Ajuste-os para a sua região e contrato antes de usar como orçamento.

## De onde vem a economia

| Decisão | Efeito |
|---|---|
| Jobs Compute em vez de All-Purpose | DBU ~3,7x mais barata para carga agendada |
| Auto-terminate e cluster efêmero por job | zero custo entre execuções |
| Parquet/Delta com Snappy + partição por ano e UF | menos bytes lidos por consulta |
| `maxOffsetsPerTrigger` no streaming | pico de eventos não vira pico de cluster |
| AQE + coalesce de partições | menos shuffle, menos tempo de cluster |
| Camada Gold materializada | painel lê tabela pronta em vez de recalcular join |
| Spot/Low-priority nos executores do batch | até 60% de desconto na VM |
