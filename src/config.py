"""
Configuração central do projeto.

Regra de ouro: **nenhum caminho absoluto**. Tudo é resolvido a partir da raiz do
repositório (o diretório que contém este pacote), de modo que o projeto roda
igual em Windows, Linux, macOS, Docker ou Databricks.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# --------------------------------------------------------------------------- #
# Raiz do projeto: .../tech-challenge-fase2/  (dois níveis acima de src/config.py)
# --------------------------------------------------------------------------- #
RAIZ = Path(__file__).resolve().parents[1]


def _caminho(base: Path, valor: str) -> Path:
    """Resolve um caminho do YAML. Absoluto só se o usuário insistir (ex.: abfss://)."""
    p = Path(valor)
    return p if p.is_absolute() else (base / p)


@dataclass
class Config:
    """Configuração efetiva da execução (YAML + variáveis de ambiente)."""

    raiz: Path = RAIZ
    bruto: dict[str, Any] = field(default_factory=dict)

    # ---------------------------------------------------------------- caminhos
    @property
    def dir_dados(self) -> Path:
        return _caminho(self.raiz, self.bruto["caminhos"]["dados"])

    @property
    def dir_externo(self) -> Path:
        return _caminho(self.raiz, self.bruto["caminhos"]["externo"])

    @property
    def dir_raw(self) -> Path:
        return _caminho(self.raiz, self.bruto["caminhos"]["raw"])

    @property
    def dir_lakehouse(self) -> Path:
        # ABFSS/S3 continuam funcionando: se vier com esquema, não vira Path local.
        destino = os.getenv("LAKEHOUSE_URI") or self.bruto["caminhos"]["lakehouse"]
        if "://" in str(destino):
            return destino  # type: ignore[return-value]
        return _caminho(self.raiz, str(destino))

    def camada(self, nome: str) -> str:
        """Caminho (string) da camada bronze/silver/gold/_observabilidade."""
        base = self.dir_lakehouse
        return f"{base}/{nome}" if isinstance(base, str) else str(base / nome)

    # ------------------------------------------------------------------ spark
    @property
    def formato_tabela(self) -> str:
        return os.getenv("FORMATO_TABELA", self.bruto["spark"]["formato_tabela"]).lower()

    @property
    def spark_master(self) -> str:
        return os.getenv("SPARK_MASTER", self.bruto["spark"]["master"])

    @property
    def shuffle_partitions(self) -> int:
        return int(os.getenv("SHUFFLE_PARTITIONS", self.bruto["spark"]["shuffle_partitions"]))

    # -------------------------------------------------------------- streaming
    @property
    def fonte_stream(self) -> str:
        """kafka (padrão, docker/cloud) ou arquivo (fallback sem conector Kafka)."""
        return os.getenv("FONTE_STREAM", self.bruto["streaming"]["fonte"]).lower()

    @property
    def kafka_bootstrap(self) -> str:
        return os.getenv("KAFKA_BOOTSTRAP_SERVERS", self.bruto["streaming"]["bootstrap_servers"])

    @property
    def topico_eventos(self) -> str:
        return os.getenv("KAFKA_TOPICO", self.bruto["streaming"]["topico"])

    @property
    def dir_stream_arquivos(self) -> Path:
        return _caminho(self.raiz, self.bruto["streaming"]["dir_arquivos"])

    @property
    def dir_checkpoints(self) -> Path:
        return _caminho(self.raiz, self.bruto["streaming"]["dir_checkpoints"])

    # ------------------------------------------------------------------ dados
    @property
    def anos(self) -> list[int]:
        return list(self.bruto["dominio"]["anos"])

    @property
    def anos_meta(self) -> list[int]:
        return list(self.bruto["dominio"]["anos_meta"])

    @property
    def ponto_corte_saeb(self) -> int:
        return int(self.bruto["dominio"]["ponto_corte_saeb"])

    @property
    def semente(self) -> int:
        return int(os.getenv("SEMENTE", self.bruto["dominio"]["semente"]))

    # -------------------------------------------------------------- contratos
    def contratos(self, camada: str) -> dict[str, Any]:
        arq = self.raiz / "config" / "contratos" / f"{camada}.yml"
        if not arq.exists():
            return {}
        return yaml.safe_load(arq.read_text(encoding="utf-8")) or {}


def carregar(arquivo: str | Path | None = None) -> Config:
    """Carrega config/pipeline.yml (ou outro arquivo informado)."""
    arq = Path(arquivo) if arquivo else (RAIZ / "config" / "pipeline.yml")
    if not arq.is_absolute():
        arq = RAIZ / arq
    bruto = yaml.safe_load(arq.read_text(encoding="utf-8"))
    return Config(raiz=RAIZ, bruto=bruto)


# Instância padrão, importada pelo resto do projeto.
CFG = carregar()
