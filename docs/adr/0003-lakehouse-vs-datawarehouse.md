# ADR 0003 — Lakehouse em vez de data warehouse

**Status:** aceito · **Data:** 2026-08

## Contexto

O consumo previsto tem três perfis muito diferentes:

1. **Painel de gestão** — consultas SQL agregadas, muitos usuários, baixa
   latência esperada. Perfil clássico de data warehouse.
2. **Análise de desigualdade** — exploração ad hoc, joins largos, notebooks.
3. **Treino de modelo** — leitura de arquivo em escala, acesso por Python/Spark,
   necessidade de reproduzir exatamente o dado de uma data passada.

Um data warehouse puro atende bem (1) e mal (3): exportar dado para treinar
modelo cria cópia fora de governança. Um data lake puro atende bem (3) e mal
(1): sem transação e sem estatística, o painel fica lento e sujeito a leitura
suja durante a carga.

## Decisão

**Lakehouse** — arquivos Delta em ADLS Gen2, com Databricks SQL servindo o
painel diretamente sobre as tabelas Gold.

O que o Delta acrescenta ao Parquet e que decide a questão:

- **Transações ACID:** o painel nunca lê uma carga pela metade.
- **Time travel:** `VERSION AS OF` permite reproduzir o número exato divulgado
  em qualquer data — requisito de auditoria em política pública.
- **Evolução de schema:** coluna nova na origem não quebra a tabela.
- **`OPTIMIZE` + Z-ORDER:** compacta arquivos pequenos e reordena por UF e
  município, cortando os bytes lidos por consulta do painel.

## Consequências

- Uma cópia única do dado serve painel, análise e modelo — sem ETL de exportação
  e sem cópia fora de governança.
- O projeto **não fica preso ao Delta**: `src/spark_session.py` abstrai a
  escrita e cai automaticamente para Parquet quando o jar do Delta não está
  disponível (ambiente sem acesso a repositório Maven). A semântica do pipeline
  é idêntica; o que se perde no fallback é ACID e time travel, não a lógica.
- Para consultas muito repetitivas do painel, uma camada de cache do Databricks
  SQL Warehouse resolve; se ainda assim faltar desempenho, materializar um
  agregado adicional é mais barato que introduzir um warehouse separado.
