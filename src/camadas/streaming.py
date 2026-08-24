"""
Ingestão STREAMING — Spark Structured Streaming.

Percurso do evento (mesmo medalhão do batch, em tempo quase real):

    Kafka / arquivo
        -> bronze_eventos        append bruto + metadados, checkpoint
        -> silver_eventos        parse, validação, deduplicação por evento_id
        -> gold_indicador_tempo_real  agregação por janela + UF, com watermark

Decisões que importam:
  * **watermark** de `streaming.watermark_minutos` — o produtor emite ~7% dos
    eventos atrasados de propósito; sem watermark eles seriam contados fora da
    janela certa ou manteriam estado crescendo para sempre;
  * **deduplicação com watermark** por `evento_id` — Kafka entrega *at least
    once*, então evento repetido é a regra, não a exceção;
  * **checkpoint** por consulta — é o que garante retomada exata depois de uma
    falha, sem reprocessar nem perder evento;
  * **latência medida** (`emitido_em` -> processamento) e gravada na
    observabilidade, que é a métrica que o desafio pede monitorar.

O mesmo código roda em Azure Databricks apontando para o Event Hubs, que expõe
endpoint compatível com Kafka: muda a configuração, não a lógica.
"""

from __future__ import annotations

import time
from pathlib import Path

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

LOG = log(__name__)

ESQUEMA_EVENTO = StructType([
    StructField("evento_id", StringType()),
    StructField("tipo_evento", StringType()),
    StructField("id_municipio", LongType()),
    StructField("sigla_uf", StringType()),
    StructField("ano_referencia", IntegerType()),
    StructField("emitido_em", StringType()),
    StructField("origem", StringType()),
    StructField("indicador_pct", DoubleType()),
    StructField("meta_pct", DoubleType()),
    StructField("proficiencia_saeb", DoubleType()),
    StructField("alunos_avaliados", IntegerType()),
])

TIPOS_VALIDOS = ["ATUALIZACAO_INDICADOR", "NOVA_MEDICAO", "ATUALIZACAO_META"]


def _checkpoint(nome: str) -> str:
    caminho = CFG.dir_checkpoints / nome
    caminho.mkdir(parents=True, exist_ok=True)
    return str(caminho)


# --------------------------------------------------------------------------- #
# Fonte
# --------------------------------------------------------------------------- #
def ler_fluxo(spark: SparkSession) -> tuple[DataFrame, str]:
    """Lê do Kafka quando o conector está disponível; senão, do file source."""
    if CFG.fonte_stream == "kafka" and spark_session.KAFKA_DISPONIVEL:
        LOG.info("[STREAM] Fonte: Kafka %s | tópico %s", CFG.kafka_bootstrap, CFG.topico_eventos)
        bruto = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", CFG.kafka_bootstrap)
            .option("subscribe", CFG.topico_eventos)
            .option("startingOffsets", "earliest")
            # Limite de vazão por microlote: protege o cluster (e o custo) de um
            # pico de eventos e mantém a latência previsível.
            .option("maxOffsetsPerTrigger", 5000)
            .option("failOnDataLoss", "false")
            .load()
        )
        eventos = bruto.select(
            F.col("key").cast("string").alias("_chave"),
            F.col("value").cast("string").alias("_payload"),
            F.col("topic").alias("_topico"),
            F.col("partition").alias("_particao"),
            F.col("offset").alias("_offset"),
            F.col("timestamp").alias("_recebido_em"),
        )
        return eventos, "kafka"

    diretorio = CFG.dir_stream_arquivos
    diretorio.mkdir(parents=True, exist_ok=True)
    LOG.info("[STREAM] Fonte: arquivos JSON em %s", Path(diretorio).relative_to(CFG.raiz))
    bruto = (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", 4)
        .load(f"{diretorio}/*.json")
    )
    eventos = bruto.select(
        F.lit(None).cast("string").alias("_chave"),
        F.col("value").alias("_payload"),
        F.lit("arquivo").alias("_topico"),
        F.lit(0).alias("_particao"),
        F.lit(0).cast("long").alias("_offset"),
        F.current_timestamp().alias("_recebido_em"),
    )
    return eventos, "arquivo"


# --------------------------------------------------------------------------- #
# Camadas do fluxo
# --------------------------------------------------------------------------- #
def bronze_stream(eventos: DataFrame, fonte: str) -> DataFrame:
    return (
        eventos.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
        .withColumn("_fonte_stream", F.lit(fonte))
        .withColumn("_run_id", F.lit(runs.RUN_ID))
    )


def parse_stream(bronze: DataFrame) -> DataFrame:
    """Converte o payload JSON em colunas tipadas e calcula a latência do evento."""
    return (
        bronze.withColumn("dados", F.from_json(F.col("_payload"), ESQUEMA_EVENTO))
        .select("dados.*", "_recebido_em", "_ingestion_timestamp", "_fonte_stream",
                "_particao", "_offset", "_run_id")
        .withColumn("emitido_em", F.to_timestamp("emitido_em"))
        # Latência ponta a ponta: do evento nascer até o Spark processá-lo.
        .withColumn("latencia_s",
                    F.round(F.unix_timestamp("_recebido_em") - F.unix_timestamp("emitido_em"), 1))
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("evento_valido",
                    F.col("evento_id").isNotNull()
                    & F.col("id_municipio").isNotNull()
                    & F.col("tipo_evento").isin(TIPOS_VALIDOS)
                    & F.col("emitido_em").isNotNull())
    )


def silver_stream(parseado: DataFrame) -> DataFrame:
    """
    Deduplicação com watermark — Kafka entrega *at least once*, então evento
    repetido é a regra, não a exceção.

    Cada consulta carrega **um único operador com estado**: deduplicação aqui,
    agregação por janela na Gold. Encadear os dois na mesma consulta faria o
    Spark manter duas máquinas de estado sobre o mesmo checkpoint — origem
    clássica de falha de state store em produção.
    """
    return (
        parseado.withWatermark("emitido_em", f"{CFG.bruto['streaming']['watermark_minutos']} minutes")
        .dropDuplicates(["evento_id"])
    )


def gold_stream(parseado: DataFrame) -> DataFrame:
    janela = f"{CFG.bruto['streaming']['janela_minutos']} minutes"
    watermark = f"{CFG.bruto['streaming']['watermark_minutos']} minutes"
    return (
        parseado.filter(F.col("evento_valido"))
        .withWatermark("emitido_em", watermark)
        .groupBy(F.window("emitido_em", janela).alias("janela"), "sigla_uf")
        .agg(
            F.count("*").alias("eventos"),
            F.sum(F.when(F.col("tipo_evento") == "NOVA_MEDICAO", 1).otherwise(0)).alias("medicoes"),
            F.round(F.avg("indicador_pct"), 2).alias("indicador_medio_pct"),
            F.round(F.avg(
                F.when(F.col("tipo_evento") == "NOVA_MEDICAO",
                       (F.col("proficiencia_saeb") >= CFG.ponto_corte_saeb).cast("int"))
            ) * 100, 1).alias("pct_alfabetizados_janela"),
            F.round(F.avg("latencia_s"), 2).alias("latencia_media_s"),
            F.max("latencia_s").alias("latencia_max_s"),
            # Em streaming, contagem distinta exata exigiria estado ilimitado;
            # o Spark só aceita a versão aproximada (HyperLogLog).
            F.approx_count_distinct("id_municipio").alias("municipios_distintos"),
        )
        .select(
            F.col("janela.start").alias("janela_inicio"),
            F.col("janela.end").alias("janela_fim"),
            "sigla_uf", "eventos", "medicoes", "indicador_medio_pct",
            "pct_alfabetizados_janela", "latencia_media_s", "latencia_max_s",
            "municipios_distintos",
        )
    )


# --------------------------------------------------------------------------- #
# Execução
# --------------------------------------------------------------------------- #
def _gravar(df: DataFrame, caminho: str, checkpoint: str, particoes: list[str] | None = None,
            gatilho: str = "5 seconds"):
    """Inicia uma consulta de escrita contínua no formato efetivo do lakehouse."""
    writer = (
        df.writeStream.outputMode("append")
        .format(spark_session.FORMATO_EFETIVO)
        .option("checkpointLocation", _checkpoint(checkpoint))
        .option("path", caminho)
        .trigger(processingTime=gatilho)
    )
    if particoes:
        writer = writer.partitionBy(*particoes)
    return writer.start()


def _contar(spark: SparkSession, caminho: str) -> int:
    """Registros efetivamente gravados no destino (0 se o destino nem existiu)."""
    if not spark_session.existe(caminho):
        return 0
    try:
        return spark_session.ler_tabela(spark, caminho).count()
    except Exception:  # noqa: BLE001  — destino vazio ainda sem schema
        return 0


def _metricas(spark: SparkSession, consulta, caminho: str) -> dict[str, float]:
    """
    Junta as duas visões de uma consulta de streaming:
      * o que ela **leu** e a que ritmo (progresso reportado pelo Spark);
      * o que ela **gravou** (contagem no destino) — que é o número que importa
        para quarentena, já que ler 240 e gravar 0 são coisas diferentes.
    """
    progresso = consulta.recentProgress
    gravados = _contar(spark, caminho)
    if not progresso:
        return {"registros": gravados, "lidos": 0, "lotes": 0,
                "duracao_media_ms": 0.0, "vazao_media_s": 0.0}
    duracoes = [p.get("batchDuration", 0) for p in progresso]
    vazoes = [p.get("processedRowsPerSecond", 0) or 0 for p in progresso]
    return {
        "registros": gravados,
        "lidos": sum(p.get("numInputRows", 0) for p in progresso),
        "lotes": len(progresso),
        "duracao_media_ms": round(sum(duracoes) / len(duracoes), 1),
        "vazao_media_s": round(sum(vazoes) / len(vazoes), 1),
    }


def executar(spark: SparkSession, duracao_s: int | None = None) -> dict:
    duracao_s = duracao_s or int(CFG.bruto["streaming"]["duracao_demo_segundos"])
    LOG.info("=" * 78)
    LOG.info("INGESTÃO STREAMING — Structured Streaming (%ds de demonstração)", duracao_s)
    LOG.info("=" * 78)

    eventos, fonte = ler_fluxo(spark)
    bronze = bronze_stream(eventos, fonte)
    parseado = parse_stream(bronze)
    silver = silver_stream(parseado)
    gold = gold_stream(parseado)

    with runs.etapa("streaming", "eventos_stream", CFG.camada("gold/indicador_tempo_real")) as ev:
        consultas = {
            "bronze": _gravar(
                bronze, CFG.camada("bronze/eventos_stream"), "bronze_eventos",
                particoes=["_ingestion_date"],
            ),
            "silver": _gravar(
                silver.filter(F.col("evento_valido")).drop("evento_valido"),
                CFG.camada("silver/eventos_stream"), "silver_eventos",
            ),
            # Evento malformado não é descartado: vai para a quarentena do stream,
            # com o mesmo payload, para investigação posterior.
            "quarentena": _gravar(
                silver.filter(~F.col("evento_valido")),
                CFG.camada("_quarentena/streaming/eventos"), "quarentena_eventos",
            ),
            "gold": _gravar(
                gold, CFG.camada("gold/indicador_tempo_real"),
                "gold_indicador_tempo_real", gatilho="10 seconds",
            ),
        }

        fim = time.time() + duracao_s
        try:
            while time.time() < fim:
                for nome, consulta in consultas.items():
                    if not consulta.isActive:
                        # `exception()` traz a causa real; sem isso o operador só
                        # veria "uma consulta morreu" e teria de caçar no log.
                        raise RuntimeError(
                            f"Consulta de streaming '{nome}' encerrou: {consulta.exception()}"
                        )
                progresso = consultas["bronze"].lastProgress
                if progresso:
                    LOG.info(
                        "[STREAM] lote %s | %s evento(s) | %.1f ev/s | atraso do gatilho %sms",
                        progresso.get("batchId"), progresso.get("numInputRows"),
                        progresso.get("processedRowsPerSecond") or 0.0,
                        progresso.get("durationMs", {}).get("triggerExecution", 0),
                    )
                time.sleep(10)
        finally:
            for consulta in reversed(list(consultas.values())):
                consulta.stop()

        destinos = {
            "bronze": CFG.camada("bronze/eventos_stream"),
            "silver": CFG.camada("silver/eventos_stream"),
            "quarentena": CFG.camada("_quarentena/streaming/eventos"),
            "gold": CFG.camada("gold/indicador_tempo_real"),
        }
        resumo = {nome: _metricas(spark, c, destinos[nome]) for nome, c in consultas.items()}
        ev.registros_entrada = int(resumo["bronze"]["registros"])
        ev.registros_saida = int(resumo["silver"]["registros"])
        ev.registros_quarentena = int(resumo["quarentena"]["registros"])
        ev.detalhes = {"fonte": fonte, "consultas": resumo}

    for nome, m in resumo.items():
        LOG.info("[STREAM] %-11s | gravados=%5d | lidos=%5d | %d lote(s) | %6.1f ms/lote | %.1f reg/s",
                 nome, m["registros"], m["lidos"], m["lotes"],
                 m["duracao_media_ms"], m["vazao_media_s"])
    return resumo


if __name__ == "__main__":  # pragma: no cover
    from src.logging_conf import configurar

    configurar()
    sessao = spark_session.criar(com_kafka=True)
    try:
        executar(sessao)
    finally:
        runs.persistir(sessao)
        sessao.stop()
