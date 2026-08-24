# ADR 0001 — Azure Databricks como nuvem e engine

**Status:** aceito · **Data:** 2026-08

## Contexto

O desafio exige implementação em nuvem (AWS, GCP ou Azure) com arquitetura
medalhão, ingestão híbrida e Spark como engine. As três nuvens atendem. A
escolha precisa de um critério melhor do que preferência pessoal.

## Alternativas

| Opção | A favor | Contra |
|---|---|---|
| **AWS** (S3 + Glue + Athena + Kinesis) | Glue é serverless e barato para carga pequena; forte integração com Athena | Databricks na AWS é serviço de terceiro (marketplace); Kinesis não fala protocolo Kafka, exigindo código específico |
| **GCP** (GCS + Dataproc + BigQuery + Pub/Sub) | A Base dos Dados vive no BigQuery — leitura direta sem exportação | Pub/Sub também não é Kafka; Dataproc exige mais operação manual de cluster |
| **Azure** (ADLS Gen2 + Databricks + Event Hubs) | Azure Databricks é **oferta first-party** (vendida, faturada e suportada pela Microsoft); Event Hubs expõe **endpoint compatível com o protocolo Kafka**; Unity Catalog resolve governança e linhagem | Custo de DBU maior que Glue para cargas muito pequenas |

## Decisão

**Azure Databricks**, com ADLS Gen2 como lakehouse e Event Hubs como camada de
mensageria.

Dois motivos decidiram:

1. **Paridade local–nuvem sem reescrever código.** O Event Hubs aceita clientes
   Kafka. O mesmo `spark.readStream.format("kafka")` que roda contra o broker do
   `docker compose` roda contra o Event Hubs mudando apenas
   `KAFKA_BOOTSTRAP_SERVERS`. Com Kinesis ou Pub/Sub, a perna de streaming
   precisaria de um conector diferente do usado em desenvolvimento — e código
   que só existe em produção é código que ninguém testou.
2. **Databricks é a plataforma trabalhada na disciplina**, e o Azure é a única
   nuvem em que ele é serviço nativo, com identidade gerenciada (Entra ID),
   Unity Catalog e faturamento integrado.

## Consequências

- **Positivas:** um único código para local, Docker e nuvem; governança de
  catálogo pronta; Photon e autoscale disponíveis sem configuração extra.
- **Negativas:** para volumes muito pequenos, Glue seria mais barato — o custo
  fixo de DBU aparece mesmo em job curto. Mitigação: **Jobs Compute** em vez de
  All-Purpose (DBU ~3,7x mais barata) e cluster efêmero com auto-terminate.
- **Reversibilidade:** o código não usa nenhuma API proprietária do Databricks
  fora dos jobs. Migrar para EMR ou Dataproc significa trocar o JSON de job e a
  variável `LAKEHOUSE_URI` — a lógica de negócio fica intacta.
