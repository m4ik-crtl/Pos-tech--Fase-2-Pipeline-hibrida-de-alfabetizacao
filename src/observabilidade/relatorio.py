"""
Relatório de monitoramento da execução.

Lê os eventos de `data/_observabilidade/*.jsonl` e produz:
  * um resumo no terminal (o que um plantonista olha primeiro);
  * `data/_observabilidade/relatorio.md` — falhas, latência, volume e qualidade.

É o equivalente local do workbook do Azure Monitor descrito em docs/monitoramento.md.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from src.config import CFG
from src.logging_conf import log

LOG = log(__name__)

LIMITE_SCORE_ALERTA = 90.0


def _carregar(diretorio: Path) -> list[dict]:
    eventos: list[dict] = []
    for arquivo in sorted(diretorio.glob("*.jsonl")):
        for linha in arquivo.read_text(encoding="utf-8").splitlines():
            if linha.strip():
                eventos.append(json.loads(linha))
    return eventos


def _mb(n: int) -> str:
    return f"{n / 1_048_576:.2f} MB"


def gerar(destino: Path | None = None) -> Path | None:
    diretorio = Path(CFG.raiz) / "data" / "_observabilidade"
    if not diretorio.exists():
        LOG.warning("[OBS] Nenhuma execução registrada ainda.")
        return None

    eventos = _carregar(diretorio)
    if not eventos:
        return None

    ultimo_run = eventos[-1]["run_id"]
    atual = [e for e in eventos if e["run_id"] == ultimo_run]

    falhas = [e for e in atual if e["status"] != "OK"]
    degradados = [
        e for e in atual
        if e.get("score_qualidade") is not None and e["score_qualidade"] < LIMITE_SCORE_ALERTA
    ]
    duracao = sum(e["duracao_s"] for e in atual)
    volume = sum(e["registros_saida"] for e in atual)
    bytes_totais = sum(e["bytes_escritos"] for e in atual)
    quarentena = sum(e["registros_quarentena"] for e in atual)

    linhas = [
        "# Relatório de monitoramento da pipeline",
        "",
        f"- **Execução:** `{ultimo_run}`",
        f"- **Gerado em:** {datetime.now(UTC).isoformat(timespec='seconds')}",
        f"- **Formato de tabela:** {atual[0].get('formato', '-')}",
        "",
        "## Resumo",
        "",
        "| Métrica | Valor |",
        "|---|---|",
        f"| Etapas executadas | {len(atual)} |",
        f"| Falhas de ingestão | {len(falhas)} |",
        f"| Latência total do pipeline | {duracao:.1f} s |",
        f"| Registros publicados | {volume:,} |",
        f"| Registros em quarentena | {quarentena:,} |",
        f"| Volume gravado | {_mb(bytes_totais)} |",
        f"| Tabelas com qualidade < {LIMITE_SCORE_ALERTA:.0f}% | {len(degradados)} |",
        "",
        "## Etapas",
        "",
        "| Camada | Tabela | Status | Entrada | Saída | Quarentena | Qualidade | Duração (s) | Volume |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for e in atual:
        score = "-" if e.get("score_qualidade") is None else f"{e['score_qualidade']:.1f}%"
        linhas.append(
            f"| {e['camada']} | {e['tabela']} | {e['status']} | {e['registros_entrada']:,} | "
            f"{e['registros_saida']:,} | {e['registros_quarentena']:,} | {score} | "
            f"{e['duracao_s']:.2f} | {_mb(e['bytes_escritos'])} |"
        )

    if falhas:
        linhas += ["", "## Falhas", ""]
        for e in falhas:
            linhas.append(f"- **{e['camada']}.{e['tabela']}** — {e.get('erro')}")

    checks_falhos = [
        (e, c)
        for e in atual
        for c in e.get("detalhes", {}).get("detalhe_checks", [])
        if c["status"] != "PASS"
    ]
    if checks_falhos:
        linhas += ["", "## Checks de qualidade não aprovados", "",
                   "| Camada | Tabela | Check | Coluna | Status | Detalhe |", "|---|---|---|---|---|---|"]
        for e, c in checks_falhos:
            linhas.append(
                f"| {e['camada']} | {e['tabela']} | {c['tipo']} | {c['coluna']} | "
                f"{c['status']} | {c['detalhe']} |"
            )

    destino = destino or (diretorio / "relatorio.md")
    destino.write_text("\n".join(linhas) + "\n", encoding="utf-8")

    LOG.info("-" * 78)
    LOG.info("[OBS] Execução %s | etapas=%d | falhas=%d | %.1fs | %s | quarentena=%d",
             ultimo_run, len(atual), len(falhas), duracao, _mb(bytes_totais), quarentena)
    LOG.info("[OBS] Relatório: %s", destino.relative_to(CFG.raiz))
    LOG.info("-" * 78)
    return destino


if __name__ == "__main__":  # pragma: no cover
    from src.logging_conf import configurar

    configurar()
    gerar()
