# Relatório de monitoramento da pipeline

- **Execução:** `run-20260831T145617-b341e5`
- **Gerado em:** 2026-08-31T14:58:02+00:00
- **Formato de tabela:** delta

## Resumo

| Métrica | Valor |
|---|---|
| Etapas executadas | 19 |
| Falhas de ingestão | 0 |
| Latência total do pipeline | 87.2 s |
| Registros publicados | 205,082 |
| Registros em quarentena | 55 |
| Volume gravado | 13.32 MB |
| Tabelas com qualidade < 90% | 0 |

## Etapas

| Camada | Tabela | Status | Entrada | Saída | Quarentena | Qualidade | Duração (s) | Volume |
|---|---|---|---:|---:|---:|---:|---:|---:|
| bronze | uf | OK | 27 | 27 | 0 | 100.0% | 11.26 | 0.01 MB |
| bronze | municipio | OK | 5,593 | 5,593 | 0 | 100.0% | 3.75 | 0.51 MB |
| bronze | meta_alfabetizacao_brasil | OK | 8 | 8 | 0 | 100.0% | 2.59 | 0.01 MB |
| bronze | meta_alfabetizacao_uf | OK | 216 | 216 | 0 | 100.0% | 2.58 | 0.03 MB |
| bronze | meta_alfabetizacao_municipio | OK | 16,779 | 16,779 | 0 | 100.0% | 4.30 | 1.26 MB |
| bronze | aluno | OK | 60,240 | 60,240 | 0 | 100.0% | 4.24 | 5.27 MB |
| bronze | contexto_socioeconomico_municipio | OK | 5,564 | 5,564 | 0 | 100.0% | 2.81 | 0.72 MB |
| silver | dim_uf | OK | 27 | 27 | 0 | 100.0% | 5.72 | 0.05 MB |
| silver | dim_municipio | OK | 5,571 | 5,571 | 0 | 100.0% | 6.96 | 0.76 MB |
| silver | fato_meta_brasil | OK | 8 | 8 | 0 | 100.0% | 3.81 | 0.04 MB |
| silver | fato_indicador_uf | OK | 216 | 216 | 0 | 100.0% | 3.90 | 0.06 MB |
| silver | fato_indicador_municipio | OK | 16,713 | 16,713 | 0 | 100.0% | 4.57 | 0.50 MB |
| silver | fato_aluno | OK | 60,000 | 59,945 | 55 | 100.0% | 7.27 | 1.68 MB |
| gold | indicador_alfabetizacao_municipio | OK | 16,713 | 16,713 | 0 | 100.0% | 6.87 | 1.44 MB |
| gold | meta_vs_realizado_uf | OK | 81 | 81 | 0 | 100.0% | 4.82 | 0.03 MB |
| gold | evolucao_temporal_brasil | OK | 8 | 8 | 0 | 100.0% | 3.51 | 0.01 MB |
| gold | painel_desigualdade | OK | 60 | 60 | 0 | 100.0% | 2.83 | 0.02 MB |
| gold | ranking_municipios | OK | 600 | 600 | 0 | 100.0% | 2.36 | 0.04 MB |
| gold | features_ml_municipio | OK | 16,713 | 16,713 | 0 | 100.0% | 3.09 | 0.87 MB |
