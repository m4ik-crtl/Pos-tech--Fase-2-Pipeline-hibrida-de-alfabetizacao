"""
Criação da SparkSession e abstração de escrita/leitura de tabelas.

Por que uma abstração?
----------------------
A arquitetura-alvo é **Azure Databricks + Delta Lake**. Localmente, porém, o jar
do Delta é resolvido via Maven, o que exige rede. Em vez de deixar o projeto
quebrar em ambiente restrito (o que impediria um avaliador de reproduzir os
resultados), a sessão detecta a disponibilidade do Delta em tempo de execução e
faz *downgrade* transparente para Parquet, registrando o fato no log e no
manifesto da execução.

O código de negócio (bronze/silver/gold) nunca sabe qual formato está em uso —
ele chama `escrever_tabela` / `ler_tabela`.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterable
from pathlib import Path

import pyspark
from pyspark.sql import DataFrame, SparkSession

from src.config import CFG
from src.logging_conf import log

LOG = log(__name__)

_VERSAO_DELTA = "io.delta:delta-spark_2.13:4.0.0"
_PACOTE_KAFKA = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

# Formato realmente em uso (preenchido na primeira chamada de `criar`).
FORMATO_EFETIVO: str = "parquet"
# O conector Kafka do Structured Streaming carregou? (idem: definido em `criar`)
KAFKA_DISPONIVEL: bool = False


def _config_base(builder: SparkSession.Builder) -> SparkSession.Builder:
    return (
        builder.appName("alfabetizacao-brasil")
        .master(CFG.spark_master)
        .config("spark.sql.shuffle.partitions", CFG.shuffle_partitions)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # FinOps: AQE junta partições pequenas e evita shuffle desnecessário,
        # reduzindo tempo de cluster (= custo) sem mudar o resultado.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
    )


def _tentar_com_pacotes(delta: bool, kafka: bool) -> SparkSession | None:
    """Tenta subir a sessão com os jars externos. Retorna None se não resolverem."""
    pacotes = ([_VERSAO_DELTA] if delta else []) + ([_PACOTE_KAFKA] if kafka else [])
    if not pacotes:
        return None
    try:
        builder = _config_base(SparkSession.builder).config("spark.jars.packages", ",".join(pacotes))
        if delta:
            builder = builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        sessao = builder.getOrCreate()
        if delta:  # prova real de que o jar carregou
            sessao._jvm.io.delta.tables.DeltaTable  # noqa: B018
        return sessao
    except Exception as exc:  # noqa: BLE001
        LOG.warning(
            "[SPARK] Jars externos indisponíveis neste ambiente (%s: %s). "
            "O pipeline continua com Parquet e streaming por arquivo — a semântica "
            "é a mesma; no Databricks/Docker os jars resolvem e voltam Delta + Kafka.",
            type(exc).__name__, ", ".join(pacotes),
        )
        return None


def criar(formato: str | None = None, com_kafka: bool = False) -> SparkSession:
    """Cria (ou reaproveita) a SparkSession do projeto."""
    global FORMATO_EFETIVO, KAFKA_DISPONIVEL

    formato = (formato or CFG.formato_tabela).lower()
    quer_delta = formato == "delta"
    quer_kafka = com_kafka and CFG.fonte_stream == "kafka"

    sessao = _tentar_com_pacotes(quer_delta, quer_kafka)
    FORMATO_EFETIVO = "delta" if (sessao is not None and quer_delta) else "parquet"
    KAFKA_DISPONIVEL = sessao is not None and quer_kafka

    if sessao is None:
        # Limpa qualquer sessão parcial deixada pela tentativa anterior.
        ativa = SparkSession.getActiveSession()
        if ativa is not None:
            ativa.stop()
        sessao = _config_base(SparkSession.builder).getOrCreate()

    sessao.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "ERROR"))
    LOG.info(
        "[SPARK] Sessão ativa | master=%s | formato=%s | conector Kafka=%s",
        CFG.spark_master, FORMATO_EFETIVO, "sim" if KAFKA_DISPONIVEL else "não",
    )
    return sessao


# --------------------------------------------------------------------------- #
# Escrita / leitura
# --------------------------------------------------------------------------- #
def escrever_tabela(
    df: DataFrame,
    caminho: str,
    modo: str = "overwrite",
    particoes: Iterable[str] | None = None,
) -> str:
    """Grava uma tabela no formato efetivo, particionada quando fizer sentido."""
    writer = df.write.format(FORMATO_EFETIVO).mode(modo)
    particoes = list(particoes or [])
    if particoes:
        writer = writer.partitionBy(*particoes)
    if FORMATO_EFETIVO == "delta":
        writer = writer.option("overwriteSchema", "true")
        if modo == "overwrite":
            # `overwriteSchema=true` só é aceito em modo estático de sobrescrita
            # de partição — conflita com o `partitionOverwriteMode=dynamic`
            # configurado globalmente na sessão (útil para cargas incrementais
            # que preservam partições não tocadas). Aqui cada execução resscreve
            # a tabela inteira (mode="overwrite", e `--reprocessar` já limpa o
            # diretório antes), então "static" é o modo correto, não um
            # contorno: DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE.
            writer = writer.option("partitionOverwriteMode", "static")
    writer.save(caminho)
    return caminho


def ler_tabela(spark: SparkSession, caminho: str) -> DataFrame:
    return spark.read.format(FORMATO_EFETIVO).load(caminho)


def existe(caminho: str) -> bool:
    return "://" in caminho or Path(caminho).exists()


def limpar(caminho: str) -> None:
    """Remove uma tabela local (usado para reprocessamento do zero)."""
    if "://" in caminho:
        return
    p = Path(caminho)
    if p.exists():
        shutil.rmtree(p)
