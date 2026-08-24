# ADR 0004 — Formato, particionamento e custo por consulta

**Status:** aceito · **Data:** 2026-08

## Contexto

Em nuvem, o custo de uma consulta é proporcional aos **bytes lidos** e ao
**tempo de cluster**. Formato e particionamento são, na prática, decisões
financeiras.

## Decisão

**Delta (Parquet + log transacional), compressão Snappy**, com particionamento
por camada:

| Camada | Partição | Motivo |
|---|---|---|
| Bronze | `_ingestion_date` (+ `ano` nas entidades grandes) | Reprocessar exatamente a carga de uma data; histórico separado por lote |
| Silver | `ano` | Todo consumo filtra por ano; partição pequena demais geraria arquivos minúsculos |
| Gold | `ano` + Z-ORDER por `sigla_uf`, `id_municipio` | O painel filtra por UF e detalha por município |

**O que foi deliberadamente evitado:** particionar por `sigla_uf` **e** `ano`
**e** `id_municipio`. Com 5.570 municípios × 3 anos, isso produziria mais de
16 mil diretórios com poucos KB cada — o clássico *small files problem*, que
torna a listagem de arquivos mais cara que a leitura dos dados.

Snappy em vez de Gzip: comprime um pouco menos, mas é *splittable* e muito mais
rápido para descompactar. Em carga analítica, CPU de descompressão custa mais
que os bytes economizados.

## Consequências

- Consulta típica do painel ("indicador de 2024 no Ceará") lê uma partição de
  ano e, dentro dela, os blocos ordenados por UF — em vez da tabela inteira.
- `OPTIMIZE` diário (ver `cloud/azure/databricks/otimizacao.sql`) compacta os
  arquivos que o streaming gera de minuto em minuto. Sem isso, a Gold em tempo
  real degrada em dias.
- `VACUUM RETAIN 168 HOURS` mantém 7 dias de time travel e devolve o
  armazenamento das versões antigas.
- Bronze envelhece para Cool aos 30 dias e Archive aos 180, por política de
  ciclo de vida no Terraform — o histórico continua auditável a uma fração do
  custo de armazenamento quente.
