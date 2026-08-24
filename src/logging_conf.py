"""
Logging estruturado.

Todo job emite linhas com prefixo de camada (`[BRONZE]`, `[DQ:SILVER]`, ...) para
que o mesmo formato funcione no terminal, no `docker compose logs` e no
Azure Monitor / Log Analytics (onde as linhas JSON viram campos consultáveis).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime


class FormatadorJson(logging.Formatter):
    """Formato JSON — usado quando LOG_FORMATO=json (cloud)."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        payload = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "nivel": record.levelname,
            "logger": record.name,
            "mensagem": record.getMessage(),
        }
        for chave in ("run_id", "camada", "tabela", "registros", "duracao_s"):
            if hasattr(record, chave):
                payload[chave] = getattr(record, chave)
        if record.exc_info:
            payload["excecao"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configurar(nivel: str | None = None) -> None:
    nivel = (nivel or os.getenv("LOG_NIVEL", "INFO")).upper()
    handler = logging.StreamHandler(sys.stdout)

    if os.getenv("LOG_FORMATO", "texto").lower() == "json":
        handler.setFormatter(FormatadorJson())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )

    raiz = logging.getLogger()
    raiz.handlers.clear()
    raiz.addHandler(handler)
    raiz.setLevel(nivel)

    # Spark e py4j são barulhentos demais para o nível INFO.
    for ruidoso in ("py4j", "pyspark", "org.apache.spark"):
        logging.getLogger(ruidoso).setLevel(logging.ERROR)


def log(nome: str) -> logging.Logger:
    return logging.getLogger(nome)
