"""
FinOps — estimativa de custo da arquitetura na nuvem.

Em vez de chutar um número para o README, a estimativa parte do que a execução
local realmente produziu: volume gravado por camada, duração de cada etapa e
volume de eventos do streaming (lidos de `data/_observabilidade/*.jsonl`).
Esses números são extrapolados para o cenário de produção com os preços
declarados em `config/pipeline.yml`.

Uso:
    python -m src.finops.estimativa_custos
    python -m src.finops.estimativa_custos --execucoes-mes 30 --fator-volume 50
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from src.config import CFG
from src.logging_conf import configurar, log

LOG = log(__name__)

HORAS_MES = 730


@dataclass
class Cenario:
    nome: str
    execucoes_batch_mes: int
    fator_volume: float          # quantas vezes o volume local
    horas_streaming_dia: float
    nos_cluster: int


CENARIOS = [
    Cenario("Piloto (uma UF)", 30, 5, 4, 2),
    Cenario("Produção nacional", 30, 50, 24, 4),
    Cenario("Pico de divulgação de resultados", 60, 120, 24, 8),
]


def _uso_local() -> dict[str, float]:
    """Lê a última execução registrada e devolve volume e duração observados."""
    diretorio = Path(CFG.raiz) / "data" / "_observabilidade"
    arquivos = sorted(diretorio.glob("*.jsonl")) if diretorio.exists() else []
    if not arquivos:
        LOG.warning("[FINOPS] Nenhuma execução registrada — rode `make batch` antes.")
        return {"bytes": 0.0, "segundos": 0.0, "registros": 0.0}

    eventos = []
    for arquivo in arquivos:
        for linha in arquivo.read_text(encoding="utf-8").splitlines():
            if linha.strip():
                eventos.append(json.loads(linha))

    # A última execução pode ter rodado só uma etapa (ex.: apenas streaming).
    # Para dimensionar a arquitetura inteira, pegamos a medição mais recente de
    # **cada** tabela — o retrato do pipeline completo, não de um pedaço dele.
    recente: dict[tuple[str, str], dict] = {}
    for e in eventos:
        if e["status"] != "OK":
            continue
        recente[(e["camada"], e["tabela"])] = e

    atual = list(recente.values())
    if not atual:
        return {"bytes": 0.0, "segundos": 0.0, "registros": 0.0}
    return {
        "bytes": float(sum(e["bytes_escritos"] for e in atual)),
        "segundos": float(sum(e["duracao_s"] for e in atual)),
        "registros": float(sum(e["registros_saida"] for e in atual)),
        "tabelas": float(len(atual)),
    }


def estimar(cenario: Cenario, uso: dict[str, float]) -> dict[str, float]:
    p = CFG.bruto["finops"]
    cambio = p["cambio_usd_brl"]

    # ---------------------------------------------------------- armazenamento
    gb = uso["bytes"] * cenario.fator_volume / 1_073_741_824
    # Histórico bronze cresce ~12 versões/ano; silver e gold são reescritos.
    gb_total = gb * 1.6
    custo_storage = gb_total * p["preco_adls_gb_mes_usd"]

    # ------------------------------------------------------------- computação
    # Jobs Compute (efêmero) é ~3,7x mais barato que All-Purpose por DBU: usar
    # cluster de job em vez de cluster interativo é a maior economia isolada.
    horas_batch = (uso["segundos"] * cenario.fator_volume / 3600) * cenario.execucoes_batch_mes
    horas_batch = max(horas_batch, 0.05 * cenario.execucoes_batch_mes)  # piso de partida
    dbu_batch = horas_batch * cenario.nos_cluster
    custo_batch = dbu_batch * (p["preco_dbu_jobs_usd"] + p["preco_vm_hora_usd"])

    horas_stream = cenario.horas_streaming_dia * 30
    dbu_stream = horas_stream * 2  # cluster mínimo do job de streaming
    custo_stream = dbu_stream * (p["preco_dbu_jobs_usd"] + p["preco_vm_hora_usd"])

    # -------------------------------------------------------------- mensageria
    custo_eventhubs = p["preco_event_hubs_tu_hora_usd"] * HORAS_MES * max(1, cenario.nos_cluster // 4)

    total_usd = custo_storage + custo_batch + custo_stream + custo_eventhubs
    return {
        "gb_armazenados": round(gb_total, 2),
        "horas_batch": round(horas_batch, 2),
        "armazenamento_usd": round(custo_storage, 2),
        "batch_usd": round(custo_batch, 2),
        "streaming_usd": round(custo_stream, 2),
        "mensageria_usd": round(custo_eventhubs, 2),
        "total_usd": round(total_usd, 2),
        "total_brl": round(total_usd * cambio, 2),
    }


def relatorio(execucoes_mes: int | None = None, fator_volume: float | None = None) -> str:
    uso = _uso_local()
    linhas = [
        "# Estimativa de custo — arquitetura em Azure Databricks",
        "",
        f"Base observada na última execução local: **{uso['bytes'] / 1_048_576:.1f} MB** gravados, "
        f"**{uso['segundos']:.0f}s** de processamento, **{uso['registros']:,.0f}** registros publicados.",
        "",
        "| Cenário | Volume | Armazenamento | Batch | Streaming | Event Hubs | **Total/mês (USD)** | **Total/mês (BRL)** |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for cenario in CENARIOS:
        c = Cenario(
            cenario.nome,
            execucoes_mes or cenario.execucoes_batch_mes,
            fator_volume or cenario.fator_volume,
            cenario.horas_streaming_dia,
            cenario.nos_cluster,
        )
        e = estimar(c, uso)
        linhas.append(
            f"| {c.nome} | {e['gb_armazenados']} GB | US$ {e['armazenamento_usd']} | "
            f"US$ {e['batch_usd']} | US$ {e['streaming_usd']} | US$ {e['mensageria_usd']} | "
            f"**US$ {e['total_usd']}** | **R$ {e['total_brl']}** |"
        )

    linhas += [
        "",
        "> Preços de referência declarados em `config/pipeline.yml` "
        f"(câmbio R$ {CFG.bruto['finops']['cambio_usd_brl']}/US$). "
        "Ajuste-os para a sua região e contrato antes de usar como orçamento.",
        "",
        "## De onde vem a economia",
        "",
        "| Decisão | Efeito |",
        "|---|---|",
        "| Jobs Compute em vez de All-Purpose | DBU ~3,7x mais barata para carga agendada |",
        "| Auto-terminate e cluster efêmero por job | zero custo entre execuções |",
        "| Parquet/Delta com Snappy + partição por ano e UF | menos bytes lidos por consulta |",
        "| `maxOffsetsPerTrigger` no streaming | pico de eventos não vira pico de cluster |",
        "| AQE + coalesce de partições | menos shuffle, menos tempo de cluster |",
        "| Camada Gold materializada | painel lê tabela pronta em vez de recalcular join |",
        "| Spot/Low-priority nos executores do batch | até 60% de desconto na VM |",
    ]
    return "\n".join(linhas) + "\n"


def main() -> None:  # pragma: no cover
    p = argparse.ArgumentParser(description="Estimativa de custo da arquitetura")
    p.add_argument("--execucoes-mes", type=int, default=None)
    p.add_argument("--fator-volume", type=float, default=None)
    p.add_argument("--salvar", default="docs/estimativa_custos.md")
    args = p.parse_args()

    configurar()
    texto = relatorio(args.execucoes_mes, args.fator_volume)
    destino = CFG.raiz / args.salvar
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(texto, encoding="utf-8")
    print(texto)
    LOG.info("[FINOPS] Estimativa salva em %s", args.salvar)


if __name__ == "__main__":  # pragma: no cover
    main()
