"""Testes do motor de contratos de dados."""

from __future__ import annotations

from src.qualidade import contratos


def _df(spark):
    return spark.createDataFrame(
        [
            ("a", 10.0, "MUNICIPAL"),
            ("b", 200.0, "MUNICIPAL"),   # fora da faixa
            ("c", None, "MUNICIPAL"),    # ausente
            (None, 30.0, "PRIVADA"),     # chave nula
            ("e", 40.0, "MARCIANA"),     # valor não permitido
        ],
        ["id", "valor", "rede"],
    )


def test_not_null_manda_para_quarentena(spark):
    contrato = {"checks": [{"tipo": "not_null", "coluna": "id", "critico": True}]}
    validos, quarentena, rel = contratos.aplicar(_df(spark), contrato, "t", "silver")

    assert validos.count() == 4
    assert quarentena.count() == 1
    assert "not_null:id" in quarentena.collect()[0]["_motivo_quarentena"]
    assert rel.criticos_falhos == 1


def test_range_com_nulo_proibido_nao_perde_registro(spark):
    """Nulo não pode escapar: ou é válido, ou está na quarentena — nunca some."""
    contrato = {
        "checks": [
            {"tipo": "range", "coluna": "valor", "valor": [0, 100],
             "critico": True, "permite_nulo": False}
        ]
    }
    validos, quarentena, _ = contratos.aplicar(_df(spark), contrato, "t", "silver")
    assert validos.count() + quarentena.count() == 5
    assert quarentena.count() == 2  # o 200.0 e o nulo


def test_range_permitindo_nulo(spark):
    contrato = {
        "checks": [{"tipo": "range", "coluna": "valor", "valor": [0, 100], "critico": True}]
    }
    validos, quarentena, _ = contratos.aplicar(_df(spark), contrato, "t", "silver")
    assert quarentena.count() == 1  # só o 200.0


def test_tolerancia_evita_parada_por_desvio_pequeno(spark):
    contrato = {
        "checks": [
            {"tipo": "valores_permitidos", "coluna": "rede",
             "valor": ["MUNICIPAL", "ESTADUAL", "PRIVADA"],
             "critico": True, "tolerancia_pct": 25}
        ]
    }
    _, quarentena, rel = contratos.aplicar(_df(spark), contrato, "t", "silver")
    assert quarentena.count() == 1          # o registro segue isolado
    assert rel.criticos_falhos == 0         # mas o job não é derrubado


def test_check_de_aviso_nao_isola_registro(spark):
    contrato = {
        "checks": [{"tipo": "range", "coluna": "valor", "valor": [0, 100], "critico": False}]
    }
    validos, quarentena, rel = contratos.aplicar(_df(spark), contrato, "t", "silver")
    assert quarentena.count() == 0
    assert validos.count() == 5
    assert rel.checks[0].status == "WARN"


def test_chave_estrangeira(spark):
    referencia = spark.createDataFrame([(1,), (2,)], ["id_uf"])
    dados = spark.createDataFrame([(1, "a"), (2, "b"), (99, "c")], ["id_uf", "nome"])
    contrato = {
        "checks": [
            {"tipo": "chave_estrangeira", "coluna": "id_uf",
             "referencia": "dim_uf", "critico": True}
        ]
    }
    validos, quarentena, _ = contratos.aplicar(
        dados, contrato, "t", "silver", referencias={"dim_uf": referencia}
    )
    assert validos.count() == 2
    assert quarentena.count() == 1
    # a coluna técnica do join não vaza para a saída
    assert not any(c.startswith("__fk") for c in validos.columns)


def test_unico_detecta_duplicidade(spark):
    dados = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    contrato = {"checks": [{"tipo": "unico", "colunas": ["id"], "critico": True}]}
    _, _, rel = contratos.aplicar(dados, contrato, "t", "bronze")
    assert rel.criticos_falhos == 1
    assert "1 registro(s) duplicado(s)" in rel.checks[0].detalhe
