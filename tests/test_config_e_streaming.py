"""Testes de configuração, reprodutibilidade e parsing do stream."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import functions as F

from src.camadas import streaming
from src.config import CFG, RAIZ


def test_nenhum_caminho_absoluto_no_codigo():
    """
    O feedback da fase anterior citou caminho absoluto como impedimento de
    reprodução. Este teste existe para que o problema não volte.
    """
    suspeitos = []
    for arquivo in list((RAIZ / "src").rglob("*.py")) + list((RAIZ / "config").rglob("*.yml")):
        texto = arquivo.read_text(encoding="utf-8")
        for linha_num, linha in enumerate(texto.splitlines(), 1):
            marcadores = ("C:\\", "c:\\", "/home/", "/Users/", "/mnt/c/")
            if any(m in linha for m in marcadores) and "exemplo" not in linha.lower():
                suspeitos.append(f"{arquivo.relative_to(RAIZ)}:{linha_num}: {linha.strip()}")
    assert not suspeitos, "Caminho absoluto encontrado:\n" + "\n".join(suspeitos)


def test_dados_de_entrada_estao_versionados():
    """Sem os CSVs no repositório, ninguém reproduz o resultado."""
    for arquivo in (
        "uf.csv", "municipio.csv", "contexto_socioeconomico_municipio.csv",
        "meta_alfabetizacao_brasil.csv", "meta_alfabetizacao_uf.csv",
        "meta_alfabetizacao_municipio.csv", "aluno.csv",
    ):
        caminho = CFG.dir_raw / arquivo
        assert caminho.exists(), f"Faltando data/raw/{arquivo} — rode `make dados`"
        assert caminho.stat().st_size > 0


def test_manifesto_declara_proveniencia():
    manifesto = json.loads((CFG.dir_raw / "_manifesto.json").read_text(encoding="utf-8"))
    assert "proveniencia" in manifesto
    assert "SIMULADO" in manifesto["proveniencia"]["meta_alfabetizacao_municipio.csv"]
    assert "REAL" in manifesto["proveniencia"]["uf.csv"]


def test_caminhos_resolvem_a_partir_da_raiz():
    assert CFG.dir_raw == RAIZ / "data" / "raw"
    assert Path(str(CFG.camada("gold"))).name == "gold"


def test_contratos_carregam_para_as_tres_camadas():
    for camada in ("bronze", "silver", "gold"):
        contratos = CFG.contratos(camada)
        assert contratos, f"contrato da camada {camada} vazio"
        for tabela, definicao in contratos.items():
            assert "checks" in definicao, f"{camada}.{tabela} sem checks"


def test_parse_do_evento_de_stream(spark):
    """O parser precisa tipar o payload e marcar evento malformado."""
    payloads = [
        json.dumps({
            "evento_id": "e1", "tipo_evento": "NOVA_MEDICAO", "id_municipio": 3550308,
            "sigla_uf": "sp", "ano_referencia": 2025,
            "emitido_em": "2026-08-24T10:00:00.000+00:00",
            "origem": "SECRETARIA_MUNICIPAL", "proficiencia_saeb": 800.0,
        }),
        json.dumps({"evento_id": "e2", "tipo_evento": "TIPO_DESCONHECIDO",
                    "id_municipio": 1, "emitido_em": "2026-08-24T10:00:00.000+00:00"}),
    ]
    bruto = spark.createDataFrame([(p,) for p in payloads], ["_payload"])
    bronze = (
        bruto.withColumn("_recebido_em", F.current_timestamp())
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_fonte_stream", F.lit("teste"))
        .withColumn("_particao", F.lit(0))
        .withColumn("_offset", F.lit(0).cast("long"))
        .withColumn("_run_id", F.lit("teste"))
    )
    resultado = {r["evento_id"]: r for r in streaming.parse_stream(bronze).collect()}

    assert resultado["e1"]["sigla_uf"] == "SP"          # padronizado
    assert resultado["e1"]["evento_valido"] is True
    assert resultado["e2"]["evento_valido"] is False    # tipo fora do domínio
