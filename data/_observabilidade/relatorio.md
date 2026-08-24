# Relatório de monitoramento da pipeline

- **Execução:** `run-20260824T222953-cac087`
- **Gerado em:** 2026-08-24T22:31:18+00:00
- **Formato de tabela:** parquet

## Resumo

| Métrica | Valor |
|---|---|
| Etapas executadas | 19 |
| Falhas de ingestão | 0 |
| Latência total do pipeline | 77.6 s |
| Registros publicados | 205,082 |
| Registros em quarentena | 55 |
| Volume gravado | 12.88 MB |
| Tabelas com qualidade < 90% | 0 |

## Etapas

| Camada | Tabela | Status | Entrada | Saída | Quarentena | Qualidade | Duração (s) | Volume |
|---|---|---|---:|---:|---:|---:|---:|---:|
| bronze | uf | OK | 27 | 27 | 0 | 100.0% | 8.45 | 0.01 MB |
| bronze | municipio | OK | 5,593 | 5,593 | 0 | 100.0% | 3.40 | 0.51 MB |
| bronze | meta_alfabetizacao_brasil | OK | 8 | 8 | 0 | 100.0% | 1.69 | 0.01 MB |
| bronze | meta_alfabetizacao_uf | OK | 216 | 216 | 0 | 100.0% | 1.99 | 0.02 MB |
| bronze | meta_alfabetizacao_municipio | OK | 16,779 | 16,779 | 0 | 100.0% | 2.78 | 1.25 MB |
| bronze | aluno | OK | 60,240 | 60,240 | 0 | 100.0% | 5.38 | 5.23 MB |
| bronze | contexto_socioeconomico_municipio | OK | 5,564 | 5,564 | 0 | 100.0% | 2.29 | 0.71 MB |
| silver | dim_uf | OK | 27 | 27 | 0 | 100.0% | 6.71 | 0.03 MB |
| silver | dim_municipio | OK | 5,571 | 5,571 | 0 | 100.0% | 7.62 | 0.71 MB |
| silver | fato_meta_brasil | OK | 8 | 8 | 0 | 100.0% | 2.74 | 0.02 MB |
| silver | fato_indicador_uf | OK | 216 | 216 | 0 | 100.0% | 3.20 | 0.04 MB |
| silver | fato_indicador_municipio | OK | 16,713 | 16,713 | 0 | 100.0% | 5.14 | 0.43 MB |
| silver | fato_aluno | OK | 60,000 | 59,945 | 55 | 100.0% | 7.29 | 1.61 MB |
| gold | indicador_alfabetizacao_municipio | OK | 16,713 | 16,713 | 0 | 100.0% | 4.97 | 1.42 MB |
| gold | meta_vs_realizado_uf | OK | 81 | 81 | 0 | 100.0% | 4.58 | 0.02 MB |
| gold | evolucao_temporal_brasil | OK | 8 | 8 | 0 | 100.0% | 1.50 | 0.01 MB |
| gold | painel_desigualdade | OK | 60 | 60 | 0 | 100.0% | 2.76 | 0.01 MB |
| gold | ranking_municipios | OK | 600 | 600 | 0 | 100.0% | 1.84 | 0.03 MB |
| gold | features_ml_municipio | OK | 16,713 | 16,713 | 0 | 100.0% | 3.20 | 0.83 MB |
