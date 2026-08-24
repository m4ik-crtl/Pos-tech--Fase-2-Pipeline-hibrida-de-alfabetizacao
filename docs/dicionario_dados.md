# Dicionário de dados

Gerado automaticamente a partir do schema real do lakehouse (`python scripts/gerar_dicionario.py`). Colunas iniciadas por `_` são metadados técnicos de linhagem, presentes em todas as camadas.

## Camada Bronze

### `bronze.aluno`

Microdados de aluno avaliado (pseudonimizados)

- **Registros:** 60.240
- **Colunas:** 18

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_aluno` | string | Identificador pseudonimizado do estudante (SHA-256 truncado, não reversível) |
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `rede` | string | Rede de ensino: MUNICIPAL | ESTADUAL | FEDERAL | PRIVADA |
| `localizacao` | string | Localização da escola: URBANA | RURAL |
| `sexo` | string | Sexo declarado do estudante |
| `idade` | int | Idade do estudante no momento da avaliação |
| `proficiencia_saeb` | double | Proficiência do estudante na escala Saeb |
| `alfabetizado` | int | 1 se proficiência >= ponto de corte |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_source_file` | string | Arquivo de origem |
| `_source_system` | string | Sistema de origem |
| `_source_entity` | string | Entidade de origem |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_record_hash` | string | SHA-256 do conteúdo de negócio — detecta mudança de registro |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `ano` | int | Ano de referência do indicador |

### `bronze.contexto_socioeconomico_municipio`

Contexto socioeconômico municipal (Atlas do Desenvolvimento Humano, Censo 2010)

- **Registros:** 5.564
- **Colunas:** 31

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio_6dig` | bigint | Código IBGE de 6 dígitos, formato do Atlas do Desenvolvimento Humano |
| `idhm` | double | IDHM municipal (Atlas/PNUD, Censo 2010) |
| `idhm_educacao` | double | Subíndice de educação do IDHM |
| `idhm_renda` | double | Subíndice de renda do IDHM |
| `idhm_longevidade` | double | Subíndice de longevidade do IDHM |
| `renda_per_capita` | double | Renda domiciliar per capita (R$, Censo 2010) |
| `indice_gini` | double | Índice de Gini da renda domiciliar |
| `pct_pobres` | double | % da população pobre |
| `pct_criancas_pobres` | double | % de crianças em situação de pobreza |
| `taxa_analfabetismo_15mais` | double | Taxa de analfabetismo a partir dos 15 anos |
| `expectativa_anos_estudo` | double | Expectativa de anos de estudo |
| `pct_6a14_na_escola` | double | % de crianças de 6 a 14 anos na escola |
| `pct_6a14_fora_escola` | double | % de crianças de 6 a 14 anos fora da escola |
| `pct_criancas_dom_sem_fund` | double | % de crianças em domicílio sem ninguém com fundamental completo |
| `pct_agua_encanada` | double | % da população em domicílio com água encanada |
| `pct_energia_eletrica` | double | % da população em domicílio com energia elétrica |
| `pct_coleta_lixo` | double | % da população em domicílio com coleta de lixo |
| `pct_6a14_fund_sem_atraso` | double | % de 6 a 14 anos no fundamental sem atraso escolar |
| `populacao_total` | bigint | População residente (Censo 2010) |
| `populacao_urbana` | bigint | População urbana (Censo 2010) |
| `populacao_6_anos` | bigint | População de 6 anos (Censo 2010) |
| `populacao_6a10_anos` | bigint | População de 6 a 10 anos (Censo 2010) |
| `ano_referencia` | int | Ano de referência da fonte externa |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_source_file` | string | Arquivo de origem |
| `_source_system` | string | Sistema de origem |
| `_source_entity` | string | Entidade de origem |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_record_hash` | string | SHA-256 do conteúdo de negócio — detecta mudança de registro |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |

### `bronze.eventos_stream`



- **Registros:** 400
- **Colunas:** 10

| Coluna | Tipo | Descrição |
|---|---|---|
| `_chave` | string |  |
| `_payload` | string |  |
| `_topico` | string |  |
| `_particao` | int |  |
| `_offset` | bigint |  |
| `_recebido_em` | timestamp |  |
| `_ingestion_timestamp` | timestamp | Momento da ingestão (UTC) |
| `_fonte_stream` | string | kafka ou arquivo |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |

### `bronze.meta_alfabetizacao_brasil`

Meta e resultado nacional do Indicador Criança Alfabetizada

- **Registros:** 8
- **Colunas:** 14

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `meta_pct` | double | Meta pactuada para o ano |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `origem_meta` | string | Proveniência da meta (publicada, interpolada, derivada por regra) |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `ponto_corte_saeb` | int | Ponto de corte de alfabetização na escala Saeb (743) |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_source_file` | string | Arquivo de origem |
| `_source_system` | string | Sistema de origem |
| `_source_entity` | string | Entidade de origem |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_record_hash` | string | SHA-256 do conteúdo de negócio — detecta mudança de registro |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |

### `bronze.meta_alfabetizacao_municipio`

Meta e resultado por município

- **Registros:** 16.779
- **Colunas:** 17

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `matriculas_avaliadas` | int | Estudantes avaliados no 2º ano da rede pública |
| `alunos_alfabetizados` | int | Estudantes que atingiram o ponto de corte |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `meta_pct` | double | Meta pactuada para o ano |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `origem_meta` | string | Proveniência da meta (publicada, interpolada, derivada por regra) |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_source_file` | string | Arquivo de origem |
| `_source_system` | string | Sistema de origem |
| `_source_entity` | string | Entidade de origem |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_record_hash` | string | SHA-256 do conteúdo de negócio — detecta mudança de registro |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `ano` | int | Ano de referência do indicador |

### `bronze.meta_alfabetizacao_uf`

Meta e resultado por Unidade da Federação

- **Registros:** 216
- **Colunas:** 14

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `meta_pct` | double | Meta pactuada para o ano |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `origem_meta` | string | Proveniência da meta (publicada, interpolada, derivada por regra) |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_source_file` | string | Arquivo de origem |
| `_source_system` | string | Sistema de origem |
| `_source_entity` | string | Entidade de origem |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_record_hash` | string | SHA-256 do conteúdo de negócio — detecta mudança de registro |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |

### `bronze.municipio`

Municípios brasileiros (IBGE)

- **Registros:** 5.593
- **Colunas:** 17

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `nome_municipio` | string | Nome do município padronizado |
| `id_uf` | int | Código IBGE da UF (2 dígitos) |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `capital` | int | 1 se o município é capital estadual |
| `latitude` | double | Latitude do centroide |
| `longitude` | double | Longitude do centroide |
| `ddd` | int | Código DDD do município |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_source_file` | string | Arquivo de origem |
| `_source_system` | string | Sistema de origem |
| `_source_entity` | string | Entidade de origem |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_record_hash` | string | SHA-256 do conteúdo de negócio — detecta mudança de registro |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |

### `bronze.uf`

Unidades da Federação (IBGE)

- **Registros:** 27
- **Colunas:** 14

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_uf` | int | Código IBGE da UF (2 dígitos) |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `nome_uf` | string | Nome da Unidade da Federação |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `latitude` | double | Latitude do centroide |
| `longitude` | double | Longitude do centroide |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_source_file` | string | Arquivo de origem |
| `_source_system` | string | Sistema de origem |
| `_source_entity` | string | Entidade de origem |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_record_hash` | string | SHA-256 do conteúdo de negócio — detecta mudança de registro |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |

## Camada Silver

### `silver.dim_municipio`

Dimensão de município enriquecida com contexto socioeconômico

- **Registros:** 5.571
- **Colunas:** 40

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_uf` | int | Código IBGE da UF (2 dígitos) |
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `nome_municipio` | string | Nome do município padronizado |
| `capital` | int | 1 se o município é capital estadual |
| `latitude` | double | Latitude do centroide |
| `longitude` | double | Longitude do centroide |
| `ddd` | int | Código DDD do município |
| `fonte_territorial` | string | Fonte da malha territorial (IBGE) |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `nome_uf` | string | Nome da Unidade da Federação |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `idhm` | double | IDHM municipal (Atlas/PNUD, Censo 2010) |
| `idhm_educacao` | double | Subíndice de educação do IDHM |
| `idhm_renda` | double | Subíndice de renda do IDHM |
| `idhm_longevidade` | double | Subíndice de longevidade do IDHM |
| `renda_per_capita` | double | Renda domiciliar per capita (R$, Censo 2010) |
| `indice_gini` | double | Índice de Gini da renda domiciliar |
| `pct_pobres` | double | % da população pobre |
| `pct_criancas_pobres` | double | % de crianças em situação de pobreza |
| `taxa_analfabetismo_15mais` | double | Taxa de analfabetismo a partir dos 15 anos |
| `expectativa_anos_estudo` | double | Expectativa de anos de estudo |
| `pct_6a14_na_escola` | double | % de crianças de 6 a 14 anos na escola |
| `pct_6a14_fora_escola` | double | % de crianças de 6 a 14 anos fora da escola |
| `pct_criancas_dom_sem_fund` | double | % de crianças em domicílio sem ninguém com fundamental completo |
| `pct_agua_encanada` | double | % da população em domicílio com água encanada |
| `pct_energia_eletrica` | double | % da população em domicílio com energia elétrica |
| `pct_coleta_lixo` | double | % da população em domicílio com coleta de lixo |
| `pct_6a14_fund_sem_atraso` | double | % de 6 a 14 anos no fundamental sem atraso escolar |
| `populacao_total` | bigint | População residente (Censo 2010) |
| `populacao_urbana` | bigint | População urbana (Censo 2010) |
| `populacao_6_anos` | bigint | População de 6 anos (Censo 2010) |
| `populacao_6a10_anos` | bigint | População de 6 a 10 anos (Censo 2010) |
| `ano_referencia` | int | Ano de referência da fonte externa |
| `fonte_contexto` | string | Fonte do contexto socioeconômico (Atlas/PNUD) |
| `contexto_disponivel` | boolean | Há contexto socioeconômico para este município? |
| `porte_municipio` | string | PEQUENO (<20 mil) | MEDIO (<100 mil) | GRANDE |
| `_silver_processed_at` | timestamp | Momento do processamento na Silver |

### `silver.dim_uf`

Dimensão de Unidade da Federação

- **Registros:** 27
- **Colunas:** 11

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_uf` | int | Código IBGE da UF (2 dígitos) |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `nome_uf` | string | Nome da Unidade da Federação |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `latitude` | double | Latitude do centroide |
| `longitude` | double | Longitude do centroide |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `_silver_processed_at` | timestamp | Momento do processamento na Silver |

### `silver.eventos_stream`



- **Registros:** 400
- **Colunas:** 18

| Coluna | Tipo | Descrição |
|---|---|---|
| `evento_id` | string | Identificador único do evento (chave de deduplicação) |
| `tipo_evento` | string | ATUALIZACAO_INDICADOR | NOVA_MEDICAO | ATUALIZACAO_META |
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `ano_referencia` | int | Ano de referência da fonte externa |
| `emitido_em` | timestamp | Instante em que o evento nasceu na origem (event time) |
| `origem` | string | Origem do evento no streaming |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `meta_pct` | double | Meta pactuada para o ano |
| `proficiencia_saeb` | double | Proficiência do estudante na escala Saeb |
| `alunos_avaliados` | int | Estudantes avaliados no evento |
| `_recebido_em` | timestamp |  |
| `_ingestion_timestamp` | timestamp | Momento da ingestão (UTC) |
| `_fonte_stream` | string | kafka ou arquivo |
| `_particao` | int |  |
| `_offset` | bigint |  |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `latencia_s` | bigint | Segundos entre a emissão e o processamento |

### `silver.fato_aluno`

Microdado de aluno avaliado

- **Registros:** 59.945
- **Colunas:** 15

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_aluno` | string | Identificador pseudonimizado do estudante (SHA-256 truncado, não reversível) |
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `rede` | string | Rede de ensino: MUNICIPAL | ESTADUAL | FEDERAL | PRIVADA |
| `localizacao` | string | Localização da escola: URBANA | RURAL |
| `sexo` | string | Sexo declarado do estudante |
| `idade` | int | Idade do estudante no momento da avaliação |
| `proficiencia_saeb` | double | Proficiência do estudante na escala Saeb |
| `alfabetizado` | int | 1 se proficiência >= ponto de corte |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `_silver_processed_at` | timestamp | Momento do processamento na Silver |
| `ano` | int | Ano de referência do indicador |

### `silver.fato_indicador_municipio`

Indicador Criança Alfabetizada por município e ano

- **Registros:** 16.713
- **Colunas:** 15

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `matriculas_avaliadas` | int | Estudantes avaliados no 2º ano da rede pública |
| `alunos_alfabetizados` | int | Estudantes que atingiram o ponto de corte |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `meta_pct` | double | Meta pactuada para o ano |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `origem_meta` | string | Proveniência da meta (publicada, interpolada, derivada por regra) |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `gap_meta_pp` | double | Indicador menos meta, em pontos percentuais |
| `_silver_processed_at` | timestamp | Momento do processamento na Silver |
| `ano` | int | Ano de referência do indicador |

### `silver.fato_indicador_uf`

Indicador e meta por UF e ano

- **Registros:** 216
- **Colunas:** 13

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `meta_pct` | double | Meta pactuada para o ano |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `origem_meta` | string | Proveniência da meta (publicada, interpolada, derivada por regra) |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `indicador_disponivel` | boolean | Houve coleta divulgada para esta UF/ano? |
| `gap_meta_pp` | double | Indicador menos meta, em pontos percentuais |
| `_silver_processed_at` | timestamp | Momento do processamento na Silver |

### `silver.fato_meta_brasil`

Série nacional de meta e resultado

- **Registros:** 8
- **Colunas:** 13

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `meta_pct` | double | Meta pactuada para o ano |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `origem_meta` | string | Proveniência da meta (publicada, interpolada, derivada por regra) |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `ponto_corte_saeb` | int | Ponto de corte de alfabetização na escala Saeb (743) |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `indicador_disponivel` | boolean | Houve coleta divulgada para esta UF/ano? |
| `gap_meta_pp` | double | Indicador menos meta, em pontos percentuais |
| `_silver_processed_at` | timestamp | Momento do processamento na Silver |

## Camada Gold

### `gold.evolucao_temporal_brasil`

Série histórica nacional

- **Registros:** 8
- **Colunas:** 15

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `meta_pct` | double | Meta pactuada para o ano |
| `indicador_publicado_pct` | double | Valor publicado pelo INEP para a UF/Brasil |
| `origem_meta` | string | Proveniência da meta (publicada, interpolada, derivada por regra) |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `ponto_corte_saeb` | int | Ponto de corte de alfabetização na escala Saeb (743) |
| `matriculas_avaliadas` | bigint | Estudantes avaliados no 2º ano da rede pública |
| `alunos_alfabetizados` | bigint | Estudantes que atingiram o ponto de corte |
| `municipios_cobertos` | bigint | Municípios cobertos pela apuração |
| `indicador_calculado_pct` | double | Valor recalculado a partir do grão municipal |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `variacao_pp` | double | Variação do indicador em relação ao ano anterior (p.p.) |
| `gap_meta_pp` | double | Indicador menos meta, em pontos percentuais |
| `distancia_meta_2030_pp` | double | Distância até a meta de 80% em 2030 (p.p.) |
| `_gold_processed_at` | timestamp | Momento do processamento na Gold |

### `gold.features_ml_municipio`

Feature store para predição do indicador (sem vazamento)

- **Registros:** 16.713
- **Colunas:** 27

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `nome_municipio` | string | Nome do município padronizado |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `alvo_indicador_pct` | double | ALVO do modelo preditivo: indicador do município no ano t |
| `indicador_ano_anterior_pct` | double | Indicador do município em t-1 (feature sem vazamento) |
| `matriculas_ano_anterior` | int | Matrículas avaliadas em t-1 |
| `variacao_ano_anterior_pp` | double | Variação observada entre t-2 e t-1 |
| `meta_pactuada_pct` | double | Meta pactuada para o ano (feature conhecida ex-ante) |
| `idhm` | double | IDHM municipal (Atlas/PNUD, Censo 2010) |
| `idhm_educacao` | double | Subíndice de educação do IDHM |
| `idhm_renda` | double | Subíndice de renda do IDHM |
| `renda_per_capita` | double | Renda domiciliar per capita (R$, Censo 2010) |
| `indice_gini` | double | Índice de Gini da renda domiciliar |
| `pct_pobres` | double | % da população pobre |
| `pct_criancas_pobres` | double | % de crianças em situação de pobreza |
| `taxa_analfabetismo_15mais` | double | Taxa de analfabetismo a partir dos 15 anos |
| `pct_6a14_fora_escola` | double | % de crianças de 6 a 14 anos fora da escola |
| `pct_criancas_dom_sem_fund` | double | % de crianças em domicílio sem ninguém com fundamental completo |
| `pct_6a14_fund_sem_atraso` | double | % de 6 a 14 anos no fundamental sem atraso escolar |
| `pct_agua_encanada` | double | % da população em domicílio com água encanada |
| `pct_energia_eletrica` | double | % da população em domicílio com energia elétrica |
| `populacao_total` | bigint | População residente (Censo 2010) |
| `porte_municipio` | string | PEQUENO (<20 mil) | MEDIO (<100 mil) | GRANDE |
| `capital` | int | 1 se o município é capital estadual |
| `_gold_processed_at` | timestamp | Momento do processamento na Gold |

### `gold.indicador_alfabetizacao_municipio`

Visão analítica municipal: indicador, meta, gap e contexto

- **Registros:** 16.713
- **Colunas:** 44

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `matriculas_avaliadas` | int | Estudantes avaliados no 2º ano da rede pública |
| `alunos_alfabetizados` | int | Estudantes que atingiram o ponto de corte |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `meta_pct` | double | Meta pactuada para o ano |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `origem_meta` | string | Proveniência da meta (publicada, interpolada, derivada por regra) |
| `fonte` | string | Sistema de origem do registro |
| `_ingestion_timestamp` | string | Momento da ingestão (UTC) |
| `_run_id` | string | Identificador da execução que gravou a linha |
| `_ingestion_date` | date | Data da ingestão — coluna de partição |
| `gap_meta_pp` | double | Indicador menos meta, em pontos percentuais |
| `_silver_processed_at` | timestamp | Momento do processamento na Silver |
| `nome_municipio` | string | Nome do município padronizado |
| `id_uf` | int | Código IBGE da UF (2 dígitos) |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `nome_uf` | string | Nome da Unidade da Federação |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `capital` | int | 1 se o município é capital estadual |
| `latitude` | double | Latitude do centroide |
| `longitude` | double | Longitude do centroide |
| `idhm` | double | IDHM municipal (Atlas/PNUD, Censo 2010) |
| `idhm_educacao` | double | Subíndice de educação do IDHM |
| `idhm_renda` | double | Subíndice de renda do IDHM |
| `renda_per_capita` | double | Renda domiciliar per capita (R$, Censo 2010) |
| `indice_gini` | double | Índice de Gini da renda domiciliar |
| `pct_pobres` | double | % da população pobre |
| `pct_criancas_pobres` | double | % de crianças em situação de pobreza |
| `taxa_analfabetismo_15mais` | double | Taxa de analfabetismo a partir dos 15 anos |
| `pct_6a14_fora_escola` | double | % de crianças de 6 a 14 anos fora da escola |
| `pct_criancas_dom_sem_fund` | double | % de crianças em domicílio sem ninguém com fundamental completo |
| `pct_6a14_fund_sem_atraso` | double | % de 6 a 14 anos no fundamental sem atraso escolar |
| `pct_agua_encanada` | double | % da população em domicílio com água encanada |
| `pct_energia_eletrica` | double | % da população em domicílio com energia elétrica |
| `populacao_total` | bigint | População residente (Censo 2010) |
| `porte_municipio` | string | PEQUENO (<20 mil) | MEDIO (<100 mil) | GRANDE |
| `contexto_disponivel` | boolean | Há contexto socioeconômico para este município? |
| `status_meta` | string | ACIMA_DA_META | NA_META | ABAIXO_DA_META |
| `faixa_indicador` | string | CRITICO (<40) | ATENCAO (<60) | ADEQUADO (<80) | AVANCADO |
| `ranking_uf` | int | Posição do município dentro da sua UF no ano |
| `ranking_brasil` | int | Posição do município no Brasil no ano |
| `alunos_nao_alfabetizados` | int | Estudantes abaixo do ponto de corte |
| `_gold_processed_at` | timestamp | Momento do processamento na Gold |
| `ano` | int | Ano de referência do indicador |

### `gold.indicador_tempo_real`



- **Registros:** 13
- **Colunas:** 10

| Coluna | Tipo | Descrição |
|---|---|---|
| `janela_inicio` | timestamp | Início da janela de agregação do streaming |
| `janela_fim` | timestamp | Fim da janela de agregação do streaming |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `eventos` | bigint | Quantidade de eventos na janela |
| `medicoes` | bigint | Eventos do tipo NOVA_MEDICAO na janela |
| `indicador_medio_pct` | double | Indicador médio dos eventos da janela |
| `pct_alfabetizados_janela` | double | % de medições acima do ponto de corte na janela |
| `latencia_media_s` | double | Latência média dos eventos da janela |
| `latencia_max_s` | bigint | Maior latência observada na janela |
| `municipios_distintos` | bigint | Municípios distintos na janela (contagem aproximada) |

### `gold.meta_vs_realizado_uf`

Comparação meta x realizado por UF

- **Registros:** 81
- **Colunas:** 19

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `nome_uf` | string | Nome da Unidade da Federação |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `matriculas_avaliadas` | bigint | Estudantes avaliados no 2º ano da rede pública |
| `alunos_alfabetizados` | bigint | Estudantes que atingiram o ponto de corte |
| `municipios_avaliados` | bigint | Municípios com resultado no ano |
| `municipios_abaixo_da_meta` | bigint | Municípios que não atingiram a meta pactuada |
| `idhm_medio` | double | IDHM médio do agrupamento |
| `indicador_publicado_pct` | double | Valor publicado pelo INEP para a UF/Brasil |
| `meta_pct` | double | Meta pactuada para o ano |
| `indicador_disponivel` | boolean | Houve coleta divulgada para esta UF/ano? |
| `origem_indicador` | string | Proveniência do indicador (publicado, simulado, não coletado) |
| `indicador_calculado_pct` | double | Valor recalculado a partir do grão municipal |
| `divergencia_pp` | double | Diferença (p.p.) entre calculado e publicado — reconciliação |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `gap_meta_pp` | double | Indicador menos meta, em pontos percentuais |
| `status_meta` | string | ACIMA_DA_META | NA_META | ABAIXO_DA_META |
| `_gold_processed_at` | timestamp | Momento do processamento na Gold |

### `gold.painel_desigualdade`

Indicador por quartil de IDHM e região

- **Registros:** 60
- **Colunas:** 12

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `quartil_idhm` | int | Quartil de IDHM no ano (1 = mais baixo) |
| `municipios` | bigint | Quantidade de municípios no agrupamento |
| `indicador_medio_pct` | double | Indicador médio dos eventos da janela |
| `indicador_mediano_pct` | double | Mediana do indicador no agrupamento |
| `idhm_medio` | double | IDHM médio do agrupamento |
| `pct_criancas_pobres_medio` | double | % médio de crianças pobres no agrupamento |
| `matriculas_avaliadas` | bigint | Estudantes avaliados no 2º ano da rede pública |
| `alunos_nao_alfabetizados` | bigint | Estudantes abaixo do ponto de corte |
| `faixa_idhm` | string | Rótulo legível do quartil de IDHM |
| `_gold_processed_at` | timestamp | Momento do processamento na Gold |

### `gold.ranking_municipios`



- **Registros:** 600
- **Colunas:** 16

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do indicador |
| `grupo` | string | MELHORES ou PIORES no ranking do ano |
| `ranking_brasil` | int | Posição do município no Brasil no ano |
| `ranking_uf` | int | Posição do município dentro da sua UF no ano |
| `id_municipio` | bigint | Código IBGE do município (7 dígitos) — chave territorial |
| `nome_municipio` | string | Nome do município padronizado |
| `sigla_uf` | string | Sigla da Unidade da Federação |
| `regiao` | string | Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul) |
| `indicador_pct` | double | % de estudantes do 2º ano com proficiência >= 743 no Saeb |
| `meta_pct` | double | Meta pactuada para o ano |
| `gap_meta_pp` | double | Indicador menos meta, em pontos percentuais |
| `status_meta` | string | ACIMA_DA_META | NA_META | ABAIXO_DA_META |
| `matriculas_avaliadas` | int | Estudantes avaliados no 2º ano da rede pública |
| `idhm` | double | IDHM municipal (Atlas/PNUD, Censo 2010) |
| `porte_municipio` | string | PEQUENO (<20 mil) | MEDIO (<100 mil) | GRANDE |
| `_gold_processed_at` | timestamp | Momento do processamento na Gold |

