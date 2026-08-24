"""
Observabilidade da pipeline.

Cada etapa registra uma linha em `_observabilidade/pipeline_runs` com:
run_id, camada, tabela, registros de entrada/saída, quarentena, score de
qualidade, duração e status. É a mesma tabela que, no Azure, alimenta o
workbook do Azure Monitor — aqui ela é lida por `src/observabilidade/relatorio.py`.

Métricas cobertas (exigidas no desafio):
  * falhas de ingestão  -> status = ERRO + coluna `erro`
  * latência do pipeline -> duracao_s por etapa e latência evento->gold no streaming
  * volume processado    -> registros_entrada / registros_saida / bytes
  * alertas de erro      -> `alertar()` (log ERROR + webhook opcional)
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config import CFG
from src.logging_conf import log

LOG = log(__name__)

RUN_ID = os.getenv("RUN_ID") or f"run-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


@dataclass
class Evento:
    run_id: str
    camada: str
    tabela: str
    status: str = "OK"
    registros_entrada: int = 0
    registros_saida: int = 0
    registros_quarentena: int = 0
    score_qualidade: float | None = None
    duracao_s: float = 0.0
    bytes_escritos: int = 0
    formato: str = ""
    erro: str | None = None
    iniciado_em: str = ""
    finalizado_em: str = ""
    detalhes: dict[str, Any] = field(default_factory=dict)


_EVENTOS: list[Evento] = []


def _tamanho(caminho: str | None) -> int:
    if not caminho or "://" in caminho:
        return 0
    p = Path(caminho)
    if not p.exists():
        return 0
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def alertar(mensagem: str, contexto: dict[str, Any] | None = None) -> None:
    """Alerta de erro: sempre loga; envia webhook se ALERTA_WEBHOOK estiver definido."""
    LOG.error("[ALERTA] %s | %s", mensagem, json.dumps(contexto or {}, ensure_ascii=False))
    url = os.getenv("ALERTA_WEBHOOK")
    if not url:
        return
    corpo = json.dumps({"texto": mensagem, "contexto": contexto or {}, "run_id": RUN_ID}).encode()
    req = urllib.request.Request(url, data=corpo, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=5)  # noqa: S310
    except (urllib.error.URLError, TimeoutError) as exc:  # pragma: no cover
        LOG.warning("[ALERTA] Webhook indisponível: %s", exc)


@contextmanager
def etapa(camada: str, tabela: str, caminho_saida: str | None = None) -> Iterator[Evento]:
    """Contexto que cronometra a etapa e registra o evento — inclusive em caso de falha."""
    ev = Evento(run_id=RUN_ID, camada=camada, tabela=tabela)
    ev.iniciado_em = datetime.now(UTC).isoformat(timespec="seconds")
    inicio = time.perf_counter()
    try:
        yield ev
    except Exception as exc:  # noqa: BLE001
        ev.status = "ERRO"
        ev.erro = f"{type(exc).__name__}: {exc}"
        alertar(f"Falha em {camada}.{tabela}", {"erro": ev.erro})
        raise
    finally:
        ev.duracao_s = round(time.perf_counter() - inicio, 3)
        ev.finalizado_em = datetime.now(UTC).isoformat(timespec="seconds")
        ev.bytes_escritos = _tamanho(caminho_saida)
        from src import spark_session

        ev.formato = spark_session.FORMATO_EFETIVO
        _EVENTOS.append(ev)
        LOG.info(
            "[OBS] %-8s | %-38s | %-4s | entrada=%7d saida=%7d quarentena=%5d | %6.2fs",
            camada, tabela, ev.status, ev.registros_entrada, ev.registros_saida,
            ev.registros_quarentena, ev.duracao_s,
        )


def eventos() -> list[Evento]:
    return list(_EVENTOS)


def persistir(spark=None) -> str:
    """Grava os eventos da execução como JSONL + tabela no lakehouse."""
    destino = Path(CFG.raiz) / "data" / "_observabilidade"
    destino.mkdir(parents=True, exist_ok=True)
    arquivo = destino / f"{RUN_ID}.jsonl"
    with arquivo.open("w", encoding="utf-8") as fh:
        for ev in _EVENTOS:
            fh.write(json.dumps(asdict(ev), ensure_ascii=False) + "\n")

    if spark is not None and _EVENTOS:
        from pyspark.sql.types import (
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        from src import spark_session

        esquema = StructType(
            [
                StructField("run_id", StringType(), True),
                StructField("camada", StringType(), True),
                StructField("tabela", StringType(), True),
                StructField("status", StringType(), True),
                StructField("registros_entrada", LongType(), True),
                StructField("registros_saida", LongType(), True),
                StructField("registros_quarentena", LongType(), True),
                StructField("score_qualidade", DoubleType(), True),
                StructField("duracao_s", DoubleType(), True),
                StructField("bytes_escritos", LongType(), True),
                StructField("formato", StringType(), True),
                StructField("erro", StringType(), True),
                StructField("iniciado_em", StringType(), True),
                StructField("finalizado_em", StringType(), True),
                StructField("detalhes", StringType(), True),
            ]
        )
        linhas = []
        for ev in _EVENTOS:
            d = asdict(ev)
            d["detalhes"] = json.dumps(d["detalhes"], ensure_ascii=False)
            linhas.append(tuple(d[c.name] for c in esquema))
        df = spark.createDataFrame(linhas, schema=esquema)
        spark_session.escrever_tabela(
            df, CFG.camada("_observabilidade/pipeline_runs"), modo="append"
        )
    LOG.info("[OBS] Execução %s registrada em %s", RUN_ID, arquivo.name)
    return str(arquivo)
