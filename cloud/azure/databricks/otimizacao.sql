-- ===========================================================================
-- Manutenção das tabelas Gold — roda ao fim do job batch.
-- É aqui que a conta de nuvem encolhe: menos arquivos, menos bytes lidos,
-- menos segundos de cluster por consulta do painel.
-- ===========================================================================

-- 1) Compactação + Z-ORDER pelas colunas mais usadas nos filtros do painel.
--    Sem isso, cada consulta abre milhares de arquivos pequenos.
OPTIMIZE gold.indicador_alfabetizacao_municipio
  ZORDER BY (sigla_uf, id_municipio);

OPTIMIZE gold.meta_vs_realizado_uf
  ZORDER BY (sigla_uf);

OPTIMIZE gold.features_ml_municipio
  ZORDER BY (sigla_uf, id_municipio);

-- 2) Estatísticas para o otimizador escolher o plano certo nos joins.
ANALYZE TABLE gold.indicador_alfabetizacao_municipio COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE silver.dim_municipio COMPUTE STATISTICS FOR ALL COLUMNS;

-- 3) Limpeza do histórico: mantém 7 dias de time travel (auditoria) e
--    devolve o armazenamento das versões antigas.
VACUUM gold.indicador_alfabetizacao_municipio RETAIN 168 HOURS;
VACUUM silver.fato_indicador_municipio RETAIN 168 HOURS;

-- 4) Comentários no catálogo: governança que aparece para quem consome.
COMMENT ON TABLE gold.indicador_alfabetizacao_municipio IS
  'Indicador Criança Alfabetizada por município e ano, com meta pactuada, gap e contexto socioeconômico. Grão: (ano, id_municipio). Atualização diária às 04h.';

COMMENT ON TABLE gold.features_ml_municipio IS
  'Feature store para predição do indicador. Contém apenas variáveis conhecidas ANTES do ano do alvo — colunas derivadas do alvo no mesmo ano são vetadas por construção.';

-- 5) Visão de qualidade consumida pelo painel de observabilidade.
CREATE OR REPLACE VIEW gold.vw_qualidade_execucoes AS
SELECT
  run_id,
  camada,
  tabela,
  status,
  registros_entrada,
  registros_saida,
  registros_quarentena,
  score_qualidade,
  duracao_s,
  ROUND(bytes_escritos / 1048576, 2) AS mb_escritos,
  iniciado_em,
  finalizado_em
FROM observabilidade.pipeline_runs
WHERE iniciado_em >= date_sub(current_date(), 30);
