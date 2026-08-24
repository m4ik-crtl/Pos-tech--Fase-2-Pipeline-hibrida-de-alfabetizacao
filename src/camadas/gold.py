"""
Camada GOLD — datasets analíticos prontos para consumo.

Produtos publicados:
  1. indicador_alfabetizacao_municipio — indicador por município, com meta, gap,
     contexto socioeconômico e posição no ranking da UF;
  2. meta_vs_realizado_uf — meta x realizado por UF, com **reconciliação** entre
     o valor agregado dos municípios e o valor publicado pelo INEP;
  3. evolucao_temporal_brasil — série histórica nacional e distância da meta;
  4. painel_desigualdade — indicador por região e quartil de IDHM;
  5. ranking_municipios — melhores e piores desempenhos por ano;
  6. features_ml_municipio — feature store para predição do indicador.

Sobre a tabela 6 — prevenção de vazamento (data leakage)
--------------------------------------------------------
O alvo é o indicador do município no ano t. Ficam **fora** das features todas as
variáveis que são função aritmética do alvo naquele mesmo ano
(`alunos_alfabetizados_t`, `matriculas_avaliadas_t`, `indicador_uf_t`,
`gap_meta_t`): incluí-las produziria um R² artificialmente perfeito e um modelo
inútil em produção, porque no momento da predição esses valores ainda não
existem. Só entram variáveis conhecidas **antes** do ano t: contexto
socioeconômico do Censo, o indicador defasado (t-1) e a meta pactuada.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import spark_session
from src.config import CFG
from src.logging_conf import log
from src.observabilidade import runs
from src.qualidade import contratos

LOG = log(__name__)

TOLERANCIA_META_PP = 0.5

# Variáveis proibidas na feature store — ver docstring do módulo.
FEATURES_VETADAS = [
    "alunos_alfabetizados",
    "matriculas_avaliadas",
    "indicador_uf_pct",
    "gap_meta_pp",
]


def _silver(spark: SparkSession, tabela: str) -> DataFrame:
    return spark_session.ler_tabela(spark, CFG.camada(f"silver/{tabela}"))


def _publicar(df: DataFrame, tabela: str, particoes: list[str] | None = None) -> DataFrame:
    destino = CFG.camada(f"gold/{tabela}")
    contrato = CFG.contratos("gold").get(tabela, {})
    with runs.etapa("gold", tabela, destino) as ev:
        ev.registros_entrada = df.count()
        validos, quarentena, rel = contratos.aplicar(df, contrato, tabela, "gold")
        ev.score_qualidade = rel.score
        ev.registros_quarentena = rel.registros_quarentena
        ev.detalhes = rel.como_dict()
        if rel.criticos_falhos and CFG.bruto["qualidade"]["parar_em_falha_critica"]:
            raise RuntimeError(f"[DQ:GOLD] check(s) crítico(s) falharam em '{tabela}'.")
        spark_session.escrever_tabela(validos, destino, particoes=particoes or [])
        ev.registros_saida = rel.registros_validos
    return validos


# --------------------------------------------------------------------------- #
# 1. Indicador por município
# --------------------------------------------------------------------------- #
def indicador_municipio(spark: SparkSession) -> DataFrame:
    fato = _silver(spark, "fato_indicador_municipio")
    mun = _silver(spark, "dim_municipio")

    colunas_contexto = [
        "idhm", "idhm_educacao", "idhm_renda", "renda_per_capita", "indice_gini",
        "pct_pobres", "pct_criancas_pobres", "taxa_analfabetismo_15mais",
        "pct_6a14_fora_escola", "pct_criancas_dom_sem_fund", "pct_6a14_fund_sem_atraso",
        "pct_agua_encanada", "pct_energia_eletrica", "populacao_total", "porte_municipio",
        "contexto_disponivel",
    ]

    # A sigla vem da dimensão (fonte única de verdade territorial); a cópia
    # desnormalizada do fato é descartada para não gerar coluna ambígua.
    base = fato.drop("sigla_uf").alias("f").join(
        mun.select(
            "id_municipio", "nome_municipio", "id_uf", "sigla_uf", "nome_uf",
            "regiao", "capital", "latitude", "longitude", *colunas_contexto,
        ).alias("m"),
        on="id_municipio",
        how="inner",
    )

    janela_uf = Window.partitionBy("ano", "sigla_uf").orderBy(F.col("indicador_pct").desc())
    janela_br = Window.partitionBy("ano").orderBy(F.col("indicador_pct").desc())

    df = (
        base.withColumn(
            "status_meta",
            F.when(F.col("indicador_pct") >= F.col("meta_pct") + TOLERANCIA_META_PP, "ACIMA_DA_META")
             .when(F.col("indicador_pct") >= F.col("meta_pct") - TOLERANCIA_META_PP, "NA_META")
             .otherwise("ABAIXO_DA_META"),
        )
        .withColumn(
            "faixa_indicador",
            F.when(F.col("indicador_pct") < 40, "CRITICO")
             .when(F.col("indicador_pct") < 60, "ATENCAO")
             .when(F.col("indicador_pct") < 80, "ADEQUADO")
             .otherwise("AVANCADO"),
        )
        .withColumn("ranking_uf", F.row_number().over(janela_uf))
        .withColumn("ranking_brasil", F.row_number().over(janela_br))
        .withColumn("alunos_nao_alfabetizados",
                    F.col("matriculas_avaliadas") - F.col("alunos_alfabetizados"))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )
    return _publicar(df, "indicador_alfabetizacao_municipio", particoes=["ano"])


# --------------------------------------------------------------------------- #
# 2. Meta x realizado por UF (com reconciliação)
# --------------------------------------------------------------------------- #
def meta_vs_realizado_uf(spark: SparkSession, municipal: DataFrame) -> DataFrame:
    publicado = _silver(spark, "fato_indicador_uf").select(
        "ano", "sigla_uf",
        F.col("indicador_pct").alias("indicador_publicado_pct"),
        F.col("meta_pct").alias("meta_pct"),
        "indicador_disponivel", "origem_indicador",
    )

    agregado = municipal.groupBy("ano", "sigla_uf", "nome_uf", "regiao").agg(
        F.sum("matriculas_avaliadas").alias("matriculas_avaliadas"),
        F.sum("alunos_alfabetizados").alias("alunos_alfabetizados"),
        F.count("*").alias("municipios_avaliados"),
        F.sum(F.when(F.col("status_meta") == "ABAIXO_DA_META", 1).otherwise(0))
            .alias("municipios_abaixo_da_meta"),
        F.round(F.avg("idhm"), 3).alias("idhm_medio"),
    )

    df = (
        agregado.join(publicado, on=["ano", "sigla_uf"], how="left")
        .withColumn(
            "indicador_calculado_pct",
            F.round(100 * F.col("alunos_alfabetizados") / F.col("matriculas_avaliadas"), 1),
        )
        # Reconciliação: o quanto o agregado dos municípios se afasta do publicado.
        .withColumn(
            "divergencia_pp",
            F.round(F.col("indicador_calculado_pct") - F.col("indicador_publicado_pct"), 2),
        )
        .withColumn(
            "indicador_pct",
            F.coalesce(F.col("indicador_publicado_pct"), F.col("indicador_calculado_pct")),
        )
        .withColumn("gap_meta_pp", F.round(F.col("indicador_pct") - F.col("meta_pct"), 1))
        .withColumn(
            "status_meta",
            F.when(F.col("gap_meta_pp") >= TOLERANCIA_META_PP, "ACIMA_DA_META")
             .when(F.col("gap_meta_pp") >= -TOLERANCIA_META_PP, "NA_META")
             .otherwise("ABAIXO_DA_META"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )
    return _publicar(df, "meta_vs_realizado_uf")


# --------------------------------------------------------------------------- #
# 3. Evolução temporal nacional
# --------------------------------------------------------------------------- #
def evolucao_brasil(spark: SparkSession, municipal: DataFrame) -> DataFrame:
    nacional = _silver(spark, "fato_meta_brasil").select(
        "ano", "meta_pct",
        F.col("indicador_pct").alias("indicador_publicado_pct"),
        "origem_meta", "origem_indicador", "ponto_corte_saeb",
    )
    agregado = municipal.groupBy("ano").agg(
        F.sum("matriculas_avaliadas").alias("matriculas_avaliadas"),
        F.sum("alunos_alfabetizados").alias("alunos_alfabetizados"),
        F.countDistinct("id_municipio").alias("municipios_cobertos"),
    )

    janela = Window.orderBy("ano")
    df = (
        nacional.join(agregado, on="ano", how="left")
        .withColumn(
            "indicador_calculado_pct",
            F.round(100 * F.col("alunos_alfabetizados") / F.col("matriculas_avaliadas"), 1),
        )
        .withColumn("indicador_pct",
                    F.coalesce(F.col("indicador_publicado_pct"), F.col("indicador_calculado_pct")))
        .withColumn("variacao_pp",
                    F.round(F.col("indicador_pct") - F.lag("indicador_pct").over(janela), 1))
        .withColumn("gap_meta_pp", F.round(F.col("indicador_pct") - F.col("meta_pct"), 1))
        .withColumn("distancia_meta_2030_pp", F.round(F.lit(80.0) - F.col("indicador_pct"), 1))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )
    return _publicar(df, "evolucao_temporal_brasil")


# --------------------------------------------------------------------------- #
# 4. Painel de desigualdade educacional
# --------------------------------------------------------------------------- #
def painel_desigualdade(municipal: DataFrame) -> DataFrame:
    com_contexto = municipal.filter(F.col("idhm").isNotNull())
    janela = Window.partitionBy("ano").orderBy(F.col("idhm").asc())

    df = (
        com_contexto.withColumn("quartil_idhm", F.ntile(4).over(janela))
        .groupBy("ano", "regiao", "quartil_idhm")
        .agg(
            F.count("*").alias("municipios"),
            F.round(F.avg("indicador_pct"), 1).alias("indicador_medio_pct"),
            F.round(F.expr("percentile_approx(indicador_pct, 0.5)"), 1).alias("indicador_mediano_pct"),
            F.round(F.avg("idhm"), 3).alias("idhm_medio"),
            F.round(F.avg("pct_criancas_pobres"), 1).alias("pct_criancas_pobres_medio"),
            F.sum("matriculas_avaliadas").alias("matriculas_avaliadas"),
            F.sum("alunos_nao_alfabetizados").alias("alunos_nao_alfabetizados"),
        )
        .withColumn(
            "faixa_idhm",
            F.when(F.col("quartil_idhm") == 1, "1 - IDHM mais baixo")
             .when(F.col("quartil_idhm") == 2, "2 - IDHM médio-baixo")
             .when(F.col("quartil_idhm") == 3, "3 - IDHM médio-alto")
             .otherwise("4 - IDHM mais alto"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )
    return _publicar(df, "painel_desigualdade")


# --------------------------------------------------------------------------- #
# 5. Ranking de municípios
# --------------------------------------------------------------------------- #
def ranking_municipios(municipal: DataFrame, topo: int = 100) -> DataFrame:
    total = municipal.select("ano", F.max("ranking_brasil").over(Window.partitionBy("ano")).alias("n"))
    n_por_ano = total.groupBy("ano").agg(F.max("n").alias("total_municipios"))

    df = (
        municipal.join(n_por_ano, on="ano", how="left")
        .filter(
            (F.col("ranking_brasil") <= topo)
            | (F.col("ranking_brasil") > F.col("total_municipios") - topo)
        )
        .withColumn(
            "grupo",
            F.when(F.col("ranking_brasil") <= topo, "MELHORES").otherwise("PIORES"),
        )
        .select(
            "ano", "grupo", "ranking_brasil", "ranking_uf", "id_municipio", "nome_municipio",
            "sigla_uf", "regiao", "indicador_pct", "meta_pct", "gap_meta_pp", "status_meta",
            "matriculas_avaliadas", "idhm", "porte_municipio",
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )
    return _publicar(df, "ranking_municipios")


# --------------------------------------------------------------------------- #
# 6. Feature store para IA (sem vazamento)
# --------------------------------------------------------------------------- #
def features_ml(municipal: DataFrame) -> DataFrame:
    janela = Window.partitionBy("id_municipio").orderBy("ano")

    base = (
        municipal.withColumn("indicador_ano_anterior_pct", F.lag("indicador_pct").over(janela))
        .withColumn("matriculas_ano_anterior", F.lag("matriculas_avaliadas").over(janela))
        .withColumn(
            "variacao_ano_anterior_pp",
            F.round(F.col("indicador_ano_anterior_pct")
                    - F.lag("indicador_pct", 2).over(janela), 1),
        )
    )

    df = base.select(
        # chaves
        "ano", "id_municipio", "nome_municipio", "sigla_uf", "regiao",
        # ---------------- alvo ----------------
        F.col("indicador_pct").alias("alvo_indicador_pct"),
        # ------- features conhecidas ANTES do ano t -------
        "indicador_ano_anterior_pct",
        "matriculas_ano_anterior",
        "variacao_ano_anterior_pp",
        F.col("meta_pct").alias("meta_pactuada_pct"),
        "idhm", "idhm_educacao", "idhm_renda", "renda_per_capita", "indice_gini",
        "pct_pobres", "pct_criancas_pobres", "taxa_analfabetismo_15mais",
        "pct_6a14_fora_escola", "pct_criancas_dom_sem_fund", "pct_6a14_fund_sem_atraso",
        "pct_agua_encanada", "pct_energia_eletrica", "populacao_total",
        "porte_municipio", "capital",
    ).withColumn("_gold_processed_at", F.current_timestamp())

    vazadas = [c for c in FEATURES_VETADAS if c in df.columns]
    if vazadas:  # trava explícita: falha alto em vez de treinar um modelo inútil
        raise RuntimeError(
            f"Vazamento detectado na feature store: {vazadas}. "
            "Essas colunas são função aritmética do alvo no mesmo ano."
        )
    LOG.info(
        "[GOLD] features_ml_municipio | %d features | colunas vetadas por vazamento: %s",
        len(df.columns) - 6, ", ".join(FEATURES_VETADAS),
    )
    return _publicar(df, "features_ml_municipio")


def executar(spark: SparkSession) -> dict[str, DataFrame]:
    LOG.info("=" * 78)
    LOG.info("CAMADA GOLD — datasets analíticos")
    LOG.info("=" * 78)
    municipal = indicador_municipio(spark).cache()
    return {
        "indicador_alfabetizacao_municipio": municipal,
        "meta_vs_realizado_uf": meta_vs_realizado_uf(spark, municipal),
        "evolucao_temporal_brasil": evolucao_brasil(spark, municipal),
        "painel_desigualdade": painel_desigualdade(municipal),
        "ranking_municipios": ranking_municipios(municipal),
        "features_ml_municipio": features_ml(municipal),
    }
