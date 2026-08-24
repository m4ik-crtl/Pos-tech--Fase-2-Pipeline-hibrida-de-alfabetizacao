# Monitoramento e observabilidade

## O que é medido

O desafio pede quatro coisas. Todas saem da mesma instrumentação
(`src/observabilidade/runs.py`), que envolve cada etapa da pipeline:

| Métrica pedida | Onde aparece |
|---|---|
| **Falhas de ingestão** | `status = ERRO` + coluna `erro` com tipo e mensagem; alerta disparado na hora |
| **Latência do pipeline** | `duracao_s` por etapa (batch) e `latencia_s` por evento (streaming: emissão → processamento) |
| **Volume de dados processados** | `registros_entrada`, `registros_saida`, `bytes_escritos` por tabela |
| **Alertas de erro** | `alertar()` — log em nível ERROR sempre; webhook (Teams/Slack/Logic App) se `ALERTA_WEBHOOK` estiver definido |

Além dessas, a instrumentação registra `score_qualidade` e
`registros_quarentena` por tabela — que é o que permite ver uma fonte degradando
**antes** de virar incidente.

## Onde os dados ficam

| Destino | Conteúdo |
|---|---|
| `data/_observabilidade/<run_id>.jsonl` | Um evento por etapa, com o detalhe de cada check |
| `data/_observabilidade/relatorio.md` | Relatório legível da última execução |
| `_observabilidade/pipeline_runs` (lakehouse) | Tabela histórica — série temporal de todas as execuções |

No Azure, `LOG_FORMATO=json` faz cada linha de log virar um registro estruturado
no Log Analytics, e a tabela `pipeline_runs` alimenta o workbook do Azure
Monitor. A visão SQL pronta está em
`cloud/azure/databricks/otimizacao.sql` (`gold.vw_qualidade_execucoes`).

## Exemplo de relatório gerado

```
| Camada | Tabela                    | Status | Entrada | Saída | Quarentena | Qualidade | Duração (s) |
|--------|---------------------------|--------|--------:|------:|-----------:|----------:|------------:|
| bronze | municipio                 | OK     |   5.593 | 5.593 |          0 |    100,0% |        3,78 |
| silver | dim_municipio             | OK     |   5.571 | 5.571 |          0 |    100,0% |        6,11 |
| silver | fato_aluno                | OK     |  60.000 | 59.945|         55 |    100,0% |        7,28 |
| gold   | indicador_..._municipio   | OK     |  16.713 | 16.713|          0 |    100,0% |        4,63 |
```

Gerar: `make relatorio` (ou `python -m src.observabilidade.relatorio`).

## Alertas configurados

### Locais (o que roda no projeto)

| Condição | Ação |
|---|---|
| Check crítico falha acima da tolerância | `RuntimeError` — o job para antes de publicar |
| Qualquer etapa lança exceção | Evento com `status=ERRO` + `alertar()` |
| Consulta de streaming morre | O laço de supervisão detecta e propaga a exceção real da consulta |
| Score de qualidade < 90% | Destacado no relatório como tabela degradada |

### Em produção (definidos no Terraform e nos jobs)

| Condição | Onde |
|---|---|
| Job batch falha | `email_notifications.on_failure` no `job_batch.json` |
| Job batch passa de 1h | Regra de saúde `RUN_DURATION_SECONDS > 3600` |
| Backlog do streaming > 5 min | Regra `STREAMING_BACKLOG_SECONDS` no `job_streaming.json` |
| Backlog > 100 mil registros | Regra `STREAMING_BACKLOG_RECORDS` |
| Gasto passa de 80% do orçamento | `azurerm_consumption_budget_resource_group` |

## Por que medir latência de evento, e não só duração de job

Duração de job responde "a pipeline está lenta?". Latência de evento responde
"o número no painel está velho?" — que é a pergunta que o gestor faz. Por isso o
produtor emite deliberadamente ~7% dos eventos com atraso de 6 a 20 minutos: é o
caso que exercita o watermark e que faz a métrica de latência dizer alguma coisa.

A latência é calculada como `_recebido_em - emitido_em` e agregada por janela na
tabela `gold/indicador_tempo_real`, com média e máximo. Se a média subir, o
cluster de streaming está no limite; se só o máximo subir, há origem atrasando.
