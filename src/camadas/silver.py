"""
Camada SILVER — dados tratados, padronizados e **integrados**.

O que acontece aqui:
  * limpeza de texto (trim, capitalização consistente, siglas em maiúsculo);
  * tipagem e normalização de chaves (id_municipio com 7 dígitos, id_uf derivado);
  * deduplicação pela chave de negócio, mantendo a ingestão mais recente;
  * tratamento de valores ausentes — com a decisão explícita de **não imputar**
    o indicador: ausência de coleta (ex.: Roraima em 2024) é informação, não
    ruído, e vira a flag `indicador_disponivel`;
  * integração das bases: município + UF + contexto socioeconômico;
  * aplicação do contrato Silver, com quarentena para o que não passar.
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

COLUNAS_TECNICAS = ["_source_file", "_source_system", "_source_entity", "_record_hash"]


def _bronze(spark: SparkSession, entidade: str) -> DataFrame:
    return spark_session.ler_tabela(spark, CFG.camada(f"bronze/{entidade}"))


def _dedup(df: DataFrame, chaves: list[str]) -> DataFrame:
    """Mantém uma linha por chave de negócio: a de ingestão mais recente."""
    janela = Window.partitionBy(*chaves).orderBy(F.col("_ingestion_timestamp").desc())
    return (
        df.withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


CONECTIVOS = ["De", "Da", "Do", "Das", "Dos", "E"]


def _texto(coluna: str) -> F.Column:
    """
    Padroniza texto livre de topônimo.

    Três problemas reais de base pública tratados de uma vez:
      1. espaços extras e repetidos (`"  Belém  "`);
      2. capitalização inconsistente (`"BELÉM"`, `"belém"`);
      3. `initcap` ingênuo, que estraga nomes compostos — "Centro-Oeste" virava
         "Centro-oeste" e "Abadia de Goiás" virava "Abadia De Goiás".
    """
    limpo = F.regexp_replace(F.trim(F.col(coluna)), r"\s+", " ")
    # Capitaliza cada parte separada por hífen, preservando o hífen.
    capitalizado = F.array_join(
        F.transform(F.split(limpo, "-"), lambda parte: F.initcap(parte)), "-"
    )
    # Conectivos voltam a minúsculo quando não são a primeira palavra.
    for conectivo in CONECTIVOS:
        capitalizado = F.regexp_replace(
            capitalizado, rf"(?<=.)\b{conectivo}\b(?=\s)", conectivo.lower()
        )
    return capitalizado


def _persistir(
    df: DataFrame,
    tabela: str,
    referencias: dict[str, DataFrame] | None = None,
    particoes: list[str] | None = None,
) -> DataFrame:
    destino = CFG.camada(f"silver/{tabela}")
    contrato = CFG.contratos("silver").get(tabela, {})
    with runs.etapa("silver", tabela, destino) as ev:
        ev.registros_entrada = df.count()
        validos, quarentena, rel = contratos.aplicar(df, contrato, tabela, "silver", referencias)
        ev.score_qualidade = rel.score
        ev.registros_quarentena = rel.registros_quarentena
        ev.detalhes = rel.como_dict()

        if rel.criticos_falhos and CFG.bruto["qualidade"]["parar_em_falha_critica"]:
            raise RuntimeError(f"[DQ:SILVER] check(s) crítico(s) falharam em '{tabela}'.")

        spark_session.escrever_tabela(validos, destino, particoes=particoes or [])
        ev.registros_saida = rel.registros_validos
        if rel.registros_quarentena:
            spark_session.escrever_tabela(
                quarentena, CFG.camada(f"_quarentena/silver/{tabela}"), modo="overwrite"
            )
    return validos


# --------------------------------------------------------------------------- #
# Dimensões
# --------------------------------------------------------------------------- #
def dim_uf(spark: SparkSession) -> DataFrame:
    df = _bronze(spark, "uf")
    df = (
        _dedup(df, ["id_uf"])
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("nome_uf", _texto("nome_uf"))
        .withColumn("regiao", _texto("regiao"))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop(*COLUNAS_TECNICAS)
    )
    return _persistir(df, "dim_uf")


def dim_municipio(spark: SparkSession, uf: DataFrame) -> DataFrame:
    mun = _bronze(spark, "municipio")
    ctx = _bronze(spark, "contexto_socioeconomico_municipio")

    mun = (
        _dedup(mun, ["id_municipio"])
        .withColumn("nome_municipio", _texto("nome_municipio"))
        # A UF é sempre derivada do código IBGE do município — normalização de
        # chave que impede divergência entre a sigla textual e o código real.
        .withColumn("id_uf", F.floor(F.col("id_municipio") / F.lit(100000)).cast("int"))
        .withColumn("capital", F.coalesce(F.col("capital"), F.lit(0)).cast("int"))
        .withColumnRenamed("fonte", "fonte_territorial")
        .drop("sigla_uf", "regiao", *COLUNAS_TECNICAS)
    )

    # Normalização de chave entre fontes: o Atlas do Desenvolvimento Humano usa
    # o código IBGE de 6 dígitos; a malha territorial usa 7 (com dígito
    # verificador). Sem essa conversão o join devolve tudo nulo — em silêncio.
    ctx = (
        _dedup(ctx, ["id_municipio_6dig"])
        .withColumnRenamed("fonte", "fonte_contexto")
        .drop(*COLUNAS_TECNICAS, "_ingestion_date", "_run_id", "_ingestion_timestamp")
    )

    df = (
        mun.join(uf.select("id_uf", "sigla_uf", "nome_uf", "regiao"), on="id_uf", how="left")
        .withColumn("_id_municipio_6dig", F.floor(F.col("id_municipio") / F.lit(10)).cast("long"))
        .join(ctx, F.col("_id_municipio_6dig") == F.col("id_municipio_6dig"), how="left")
        .drop("_id_municipio_6dig", "id_municipio_6dig")
        # Município presente na malha do IBGE mas ausente no Atlas 2010
        # (criado depois do Censo): mantido, marcado, nunca inventado.
        .withColumn("contexto_disponivel", F.col("idhm").isNotNull())
        .withColumn("porte_municipio",
                    F.when(F.col("populacao_total") < 20000, "PEQUENO")
                     .when(F.col("populacao_total") < 100000, "MEDIO")
                     .when(F.col("populacao_total").isNotNull(), "GRANDE"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    return _persistir(df, "dim_municipio", referencias={"dim_uf": uf})


# --------------------------------------------------------------------------- #
# Fatos
# --------------------------------------------------------------------------- #
def fato_meta_brasil(spark: SparkSession) -> DataFrame:
    df = _bronze(spark, "meta_alfabetizacao_brasil")
    df = (
        _dedup(df, ["ano"])
        .withColumn("indicador_disponivel", F.col("indicador_pct").isNotNull())
        .withColumn("gap_meta_pp",
                    F.round(F.col("indicador_pct") - F.col("meta_pct"), 1))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop(*COLUNAS_TECNICAS)
    )
    return _persistir(df, "fato_meta_brasil")


def fato_indicador_uf(spark: SparkSession, uf: DataFrame) -> DataFrame:
    df = _bronze(spark, "meta_alfabetizacao_uf")
    df = (
        _dedup(df, ["ano", "sigla_uf"])
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("indicador_disponivel", F.col("indicador_pct").isNotNull())
        .withColumn("gap_meta_pp", F.round(F.col("indicador_pct") - F.col("meta_pct"), 1))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop(*COLUNAS_TECNICAS)
    )
    return _persistir(df, "fato_indicador_uf", referencias={"dim_uf": uf})


def fato_indicador_municipio(spark: SparkSession, municipio: DataFrame) -> DataFrame:
    df = _bronze(spark, "meta_alfabetizacao_municipio")
    df = (
        _dedup(df, ["ano", "id_municipio"])
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("indicador_pct", F.round(F.col("indicador_pct").cast("double"), 1))
        .withColumn("meta_pct", F.round(F.col("meta_pct").cast("double"), 1))
        .withColumn("gap_meta_pp", F.round(F.col("indicador_pct") - F.col("meta_pct"), 1))
        # Consistência entre colunas: alfabetizados nunca pode exceder avaliados.
        .withColumn("alunos_alfabetizados",
                    F.least(F.col("alunos_alfabetizados"), F.col("matriculas_avaliadas")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop(*COLUNAS_TECNICAS)
    )
    return _persistir(
        df, "fato_indicador_municipio",
        referencias={"dim_municipio": municipio}, particoes=["ano"],
    )


def fato_aluno(spark: SparkSession, municipio: DataFrame) -> DataFrame:
    df = _bronze(spark, "aluno")
    df = (
        _dedup(df, ["id_aluno"])
        .withColumn("rede", F.upper(F.trim(F.col("rede"))))
        .withColumn("localizacao", F.upper(F.trim(F.col("localizacao"))))
        .withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
        # Valor impossível (-1) vira nulo aqui e é barrado pelo contrato adiante:
        # o registro vai para a quarentena com o motivo registrado.
        .withColumn("proficiencia_saeb",
                    F.when(F.col("proficiencia_saeb") < 0, None)
                     .otherwise(F.col("proficiencia_saeb")))
        .withColumn("alfabetizado",
                    (F.col("proficiencia_saeb") >= F.lit(CFG.ponto_corte_saeb)).cast("int"))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop(*COLUNAS_TECNICAS)
    )
    return _persistir(
        df, "fato_aluno", referencias={"dim_municipio": municipio}, particoes=["ano"]
    )


def executar(spark: SparkSession) -> dict[str, DataFrame]:
    LOG.info("=" * 78)
    LOG.info("CAMADA SILVER — limpeza, padronização e integração das bases")
    LOG.info("=" * 78)
    uf = dim_uf(spark)
    municipio = dim_municipio(spark, uf)
    return {
        "dim_uf": uf,
        "dim_municipio": municipio,
        "fato_meta_brasil": fato_meta_brasil(spark),
        "fato_indicador_uf": fato_indicador_uf(spark, uf),
        "fato_indicador_municipio": fato_indicador_municipio(spark, municipio),
        "fato_aluno": fato_aluno(spark, municipio),
    }
