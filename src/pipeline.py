"""
Orquestrador da pipeline híbrida.

Uso:
    python -m src.pipeline                      # raw -> bronze -> silver -> gold
    python -m src.pipeline --etapas bronze silver
    python -m src.pipeline --formato parquet    # força o formato de tabela
    python -m src.pipeline --reprocessar        # limpa o lakehouse antes

No Azure Databricks o mesmo módulo é chamado pelo Job (ver
cloud/azure/databricks/job_batch.json) — não há código duplicado entre local e nuvem.
"""

from __future__ import annotations

import argparse
import sys

from src import spark_session
from src.config import CFG
from src.logging_conf import configurar, log
from src.observabilidade import relatorio, runs

LOG = log(__name__)

ETAPAS_PADRAO = ["raw", "bronze", "silver", "gold"]


def _argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pipeline híbrida — alfabetização no Brasil")
    p.add_argument("--etapas", nargs="+", default=ETAPAS_PADRAO,
                   choices=["raw", "bronze", "silver", "gold", "streaming"])
    p.add_argument("--formato", default=None, choices=["delta", "parquet"])
    p.add_argument("--reprocessar", action="store_true",
                   help="limpa bronze/silver/gold antes de executar")
    p.add_argument("--sem-relatorio", action="store_true")
    return p.parse_args()


def executar(etapas: list[str], formato: str | None = None, reprocessar: bool = False) -> int:
    LOG.info("#" * 78)
    LOG.info("# PIPELINE ALFABETIZAÇÃO BRASIL | run_id=%s | etapas=%s",
             runs.RUN_ID, ", ".join(etapas))
    LOG.info("#" * 78)

    if "raw" in etapas:
        from src.ingestao import preparar_raw

        preparar_raw.executar()

    precisa_spark = any(e in etapas for e in ("bronze", "silver", "gold", "streaming"))
    if not precisa_spark:
        return 0

    spark = spark_session.criar(formato, com_kafka="streaming" in etapas)

    if reprocessar:
        for camada in ("bronze", "silver", "gold", "_quarentena"):
            spark_session.limpar(CFG.camada(camada))
        LOG.info("[PIPELINE] Lakehouse limpo — reprocessamento completo")

    try:
        if "bronze" in etapas:
            from src.camadas import bronze

            bronze.executar(spark)

        if "silver" in etapas:
            from src.camadas import silver

            silver.executar(spark)

        if "gold" in etapas:
            from src.camadas import gold

            gold.executar(spark)

        if "streaming" in etapas:
            from src.camadas import streaming

            streaming.executar(spark)
    finally:
        runs.persistir(spark)
        spark_session.encerrar(spark)

    return 0


def main() -> int:
    args = _argumentos()
    configurar()
    codigo = 0
    try:
        codigo = executar(args.etapas, args.formato, args.reprocessar)
    except Exception as exc:  # noqa: BLE001
        LOG.exception("[PIPELINE] Execução interrompida: %s", exc)
        codigo = 1

    if not args.sem_relatorio:
        relatorio.gerar()
    return codigo


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
