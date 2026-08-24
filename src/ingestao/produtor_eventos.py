"""
Produtor de eventos — a perna *streaming* da ingestão híbrida.

Simula o que, em produção, chegaria de sistemas de avaliação e das secretarias:

    ATUALIZACAO_INDICADOR   novo percentual apurado para um município
    NOVA_MEDICAO            resultado individual de uma prova aplicada
    ATUALIZACAO_META        repactuação de meta por UF ou município

Dois modos de transporte, mesmo payload:
  * **kafka**  — broker real (docker compose) ou Azure Event Hubs, que fala o
    mesmo protocolo Kafka; trocar de um para o outro é mudar `bootstrap_servers`.
  * **arquivo** — grava JSON em `data/stream_in/`, consumido pelo file source do
    Structured Streaming. É o fallback quando não há broker disponível.

Uso:
    python -m src.ingestao.produtor_eventos --eventos 500 --intervalo 0.05
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from src.config import CFG
from src.logging_conf import configurar, log

LOG = log(__name__)

TIPOS = ["ATUALIZACAO_INDICADOR", "NOVA_MEDICAO", "ATUALIZACAO_META"]
PESOS = [0.45, 0.45, 0.10]
# Fração de eventos emitidos com atraso, para exercitar o watermark do consumidor.
PCT_ATRASADOS = 0.07


def _municipios() -> pd.DataFrame:
    arq = CFG.dir_raw / "meta_alfabetizacao_municipio.csv"
    if not arq.exists():
        raise FileNotFoundError(
            "Rode `python -m src.ingestao.preparar_raw` antes de produzir eventos."
        )
    df = pd.read_csv(arq, usecols=["ano", "id_municipio", "sigla_uf", "indicador_pct"])
    return df[df["ano"] == max(CFG.anos)].drop_duplicates("id_municipio").reset_index(drop=True)


def gerar_evento(base: pd.DataFrame, rng: random.Random) -> dict:
    linha = base.iloc[rng.randrange(len(base))]
    tipo = rng.choices(TIPOS, weights=PESOS, k=1)[0]
    agora = datetime.now(UTC)

    # Alguns eventos chegam atrasados — é o caso que o watermark precisa tratar.
    emitido = agora - timedelta(minutes=rng.randint(6, 20)) if rng.random() < PCT_ATRASADOS else agora

    evento = {
        "evento_id": str(uuid.uuid4()),
        "tipo_evento": tipo,
        "id_municipio": int(linha["id_municipio"]),
        "sigla_uf": str(linha["sigla_uf"]),
        "ano_referencia": int(linha["ano"]),
        "emitido_em": emitido.isoformat(timespec="milliseconds"),
        "origem": "SECRETARIA_MUNICIPAL" if tipo != "ATUALIZACAO_META" else "MEC_PACTUACAO",
    }

    if tipo == "NOVA_MEDICAO":
        centro = CFG.ponto_corte_saeb + (float(linha["indicador_pct"]) - 55) * 1.4
        evento["proficiencia_saeb"] = round(rng.gauss(centro, 55), 1)
        evento["alunos_avaliados"] = 1
    elif tipo == "ATUALIZACAO_INDICADOR":
        evento["indicador_pct"] = round(
            min(max(float(linha["indicador_pct"]) + rng.gauss(0, 2.5), 0), 100), 1
        )
        evento["alunos_avaliados"] = rng.randint(20, 400)
    else:
        evento["meta_pct"] = round(min(float(linha["indicador_pct"]) + rng.uniform(2, 12), 100), 1)

    return evento


# --------------------------------------------------------------------------- #
# Transportes
# --------------------------------------------------------------------------- #
def _produtor_kafka():
    """Devolve um produtor Kafka, ou None se a biblioteca/broker não estiverem lá."""
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError:
        LOG.warning("[STREAM] kafka-python não instalado — usando modo arquivo.")
        return None
    try:
        return KafkaProducer(
            bootstrap_servers=CFG.kafka_bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
            key_serializer=lambda k: str(k).encode(),
            acks="all",  # confiabilidade: só confirma com réplica gravada
            linger_ms=20,
            retries=3,
        )
    except Exception as exc:  # noqa: BLE001
        LOG.warning("[STREAM] Broker %s inacessível (%s) — usando modo arquivo.",
                    CFG.kafka_bootstrap, type(exc).__name__)
        return None


def publicar(eventos: int = 500, intervalo: float = 0.05, modo: str | None = None) -> str:
    base = _municipios()
    rng = random.Random(CFG.semente)
    modo = (modo or CFG.fonte_stream).lower()

    produtor = _produtor_kafka() if modo == "kafka" else None
    modo_efetivo = "kafka" if produtor is not None else "arquivo"

    destino = CFG.dir_stream_arquivos
    destino.mkdir(parents=True, exist_ok=True)
    arquivo: Path | None = None
    if modo_efetivo == "arquivo":
        arquivo = destino / f"eventos_{datetime.now(UTC):%Y%m%dT%H%M%S}.json"

    LOG.info("[STREAM] Publicando %d eventos | modo=%s | destino=%s",
             eventos, modo_efetivo,
             CFG.topico_eventos if modo_efetivo == "kafka" else arquivo.name)  # type: ignore[union-attr]

    buffer: list[str] = []
    for i in range(eventos):
        evento = gerar_evento(base, rng)
        if produtor is not None:
            produtor.send(CFG.topico_eventos, key=evento["id_municipio"], value=evento)
        else:
            buffer.append(json.dumps(evento, ensure_ascii=False))
            # Grava em lotes para o file source enxergar arquivos completos.
            if len(buffer) >= 50:
                alvo = destino / f"eventos_{uuid.uuid4().hex[:8]}.json"
                alvo.write_text("\n".join(buffer) + "\n", encoding="utf-8")
                buffer.clear()
        if intervalo:
            time.sleep(intervalo)
        if (i + 1) % 100 == 0:
            LOG.info("[STREAM] %d/%d eventos publicados", i + 1, eventos)

    if produtor is not None:
        produtor.flush()
        produtor.close()
    elif buffer:
        alvo = destino / f"eventos_{uuid.uuid4().hex[:8]}.json"
        alvo.write_text("\n".join(buffer) + "\n", encoding="utf-8")

    LOG.info("[STREAM] Publicação concluída (%s).", modo_efetivo)
    return modo_efetivo


def main() -> None:  # pragma: no cover
    p = argparse.ArgumentParser(description="Produtor de eventos de alfabetização")
    p.add_argument("--eventos", type=int, default=500)
    p.add_argument("--intervalo", type=float, default=0.05, help="segundos entre eventos")
    p.add_argument("--modo", choices=["kafka", "arquivo"], default=None)
    args = p.parse_args()
    configurar()
    publicar(args.eventos, args.intervalo, args.modo)


if __name__ == "__main__":  # pragma: no cover
    main()
