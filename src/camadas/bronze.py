"""
Camada BRONZE — dados brutos, com histórico preservado.

Princípios:
  * lê a fonte com **schema explícito** (nunca inferSchema em produção: schema
    inferido muda quando o arquivo muda, e isso quebra a pipeline em silêncio);
  * não transforma o conteúdo — apenas anexa metadados de ingestão;
  * grava particionado por data de ingestão, preservando o histórico completo;
  * aplica os checks de chegada do contrato bronze.

Metadados anexados a todo registro:
  _ingestion_timestamp, _ingestion_date, _source_file, _source_system,
  _record_hash (detecção de mudança), _run_id (rastreabilidade da execução).
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src import spark_session
from src.config import CFG
from src.logging_conf import log
from src.observabilidade import runs
from src.qualidade import contratos

LOG = log(__name__)

_AGORA = datetime.now(UTC)
DATA_INGESTAO = _AGORA.strftime("%Y-%m-%d")


def _s(nome: str) -> StructField:
    return StructField(nome, StringType(), True)


def _i(nome: str) -> StructField:
    return StructField(nome, IntegerType(), True)


def _l(nome: str) -> StructField:
    return StructField(nome, LongType(), True)


def _d(nome: str) -> StructField:
    return StructField(nome, DoubleType(), True)


SCHEMAS: dict[str, StructType] = {
    "uf": StructType([_i("id_uf"), _s("sigla_uf"), _s("nome_uf"), _s("regiao"),
                      _d("latitude"), _d("longitude"), _s("fonte")]),
    "municipio": StructType([_l("id_municipio"), _s("nome_municipio"), _i("id_uf"),
                             _s("sigla_uf"), _s("regiao"), _i("capital"), _d("latitude"),
                             _d("longitude"), _i("ddd"), _s("fonte")]),
    "meta_alfabetizacao_brasil": StructType([_i("ano"), _d("meta_pct"), _d("indicador_pct"),
                                             _s("origem_meta"), _s("origem_indicador"),
                                             _i("ponto_corte_saeb"), _s("fonte")]),
    "meta_alfabetizacao_uf": StructType([_i("ano"), _s("sigla_uf"), _d("meta_pct"),
                                         _d("indicador_pct"), _s("origem_meta"),
                                         _s("origem_indicador"), _s("fonte")]),
    "meta_alfabetizacao_municipio": StructType([_i("ano"), _l("id_municipio"), _s("sigla_uf"),
                                                _i("matriculas_avaliadas"), _i("alunos_alfabetizados"),
                                                _d("indicador_pct"), _d("meta_pct"),
                                                _s("origem_indicador"), _s("origem_meta"), _s("fonte")]),
    "aluno": StructType([_s("id_aluno"), _i("ano"), _l("id_municipio"), _s("sigla_uf"),
                         _s("rede"), _s("localizacao"), _s("sexo"), _i("idade"),
                         _d("proficiencia_saeb"), _i("alfabetizado"), _s("fonte")]),
}

# O contexto socioeconômico tem 24 colunas numéricas; o schema é montado abaixo.
_CTX_NUM = [
    "idhm", "idhm_educacao", "idhm_renda", "idhm_longevidade", "renda_per_capita",
    "indice_gini", "pct_pobres", "pct_criancas_pobres", "taxa_analfabetismo_15mais",
    "expectativa_anos_estudo", "pct_6a14_na_escola", "pct_6a14_fora_escola",
    "pct_criancas_dom_sem_fund", "pct_agua_encanada", "pct_energia_eletrica",
    "pct_coleta_lixo", "pct_6a14_fund_sem_atraso",
]
_CTX_INT = ["populacao_total", "populacao_urbana", "populacao_6_anos", "populacao_6a10_anos"]
SCHEMAS["contexto_socioeconomico_municipio"] = StructType(
    [_l("id_municipio_6dig")]
    + [_d(c) for c in _CTX_NUM]
    + [_l(c) for c in _CTX_INT]
    + [_i("ano_referencia"), _s("fonte")]
)

# Entidades particionadas por ano (a partição por data de ingestão é sempre aplicada).
PARTICAO_ANO = {"meta_alfabetizacao_municipio", "aluno"}


def _com_metadados(df: DataFrame, entidade: str, arquivo: str) -> DataFrame:
    negocio = [c for c in df.columns if not c.startswith("_")]
    return (
        df.withColumn("_ingestion_timestamp", F.lit(_AGORA.isoformat(timespec="seconds")))
        .withColumn("_ingestion_date", F.lit(DATA_INGESTAO))
        .withColumn("_source_file", F.lit(arquivo))
        .withColumn("_source_system", F.lit("BASE_DOS_DADOS_INEP_IBGE_ATLAS"))
        .withColumn("_source_entity", F.lit(entidade))
        .withColumn("_run_id", F.lit(runs.RUN_ID))
        .withColumn("_record_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"),
                                                                          F.lit("")) for c in negocio]), 256))
    )


def ingerir(spark: SparkSession, entidade: str) -> str:
    """Ingere uma entidade do diretório raw para a camada bronze."""
    arquivo = CFG.dir_raw / f"{entidade}.csv"
    destino = CFG.camada(f"bronze/{entidade}")
    contrato = CFG.contratos("bronze").get(entidade, {})

    with runs.etapa("bronze", entidade, destino) as ev:
        if not arquivo.exists():
            raise FileNotFoundError(
                f"Fonte ausente: {arquivo.relative_to(CFG.raiz)}. "
                "Rode `python -m src.ingestao.preparar_raw` (ou `make dados`)."
            )

        df = (
            spark.read.option("header", True)
            .option("encoding", "UTF-8")
            .schema(SCHEMAS[entidade])
            .csv(str(arquivo))
        )
        df = _com_metadados(df, entidade, arquivo.name)
        ev.registros_entrada = df.count()

        validos, quarentena, rel = contratos.aplicar(df, contrato, entidade, "bronze")
        ev.score_qualidade = rel.score
        ev.registros_quarentena = rel.registros_quarentena
        ev.detalhes = rel.como_dict()

        if rel.criticos_falhos and CFG.bruto["qualidade"]["parar_em_falha_critica"]:
            raise RuntimeError(
                f"[DQ:BRONZE] {rel.criticos_falhos} check(s) crítico(s) falharam em '{entidade}'."
            )

        particoes = ["_ingestion_date"] + (["ano"] if entidade in PARTICAO_ANO else [])
        spark_session.escrever_tabela(validos, destino, particoes=particoes)
        ev.registros_saida = rel.registros_validos

        if rel.registros_quarentena:
            spark_session.escrever_tabela(
                quarentena, CFG.camada(f"_quarentena/bronze/{entidade}"), modo="overwrite"
            )
    return destino


def executar(spark: SparkSession) -> dict[str, str]:
    LOG.info("=" * 78)
    LOG.info("CAMADA BRONZE — ingestão batch | run_id=%s", runs.RUN_ID)
    LOG.info("=" * 78)
    return {ent: ingerir(spark, ent) for ent in SCHEMAS}
