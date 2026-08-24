"""
Governança computacional: contratos de dados aplicados por código.

Um contrato é declarado em YAML (`config/contratos/<camada>.yml`) e aplicado
pela mesma função em todas as camadas. Regras de **linha** (not_null, range,
valores_permitidos, regex, chave_estrangeira) separam registros válidos dos
reprovados — os reprovados vão para a **quarentena**, não são descartados.
Regras de **tabela** (min_count, unico) avaliam o conjunto.

Um check marcado como `critico: true` interrompe o job (fail-fast): é preferível
não publicar a publicar número errado num painel de política pública. Checks de
linha aceitam `tolerancia_pct` — a fração de registros que pode ser posta em
quarentena sem derrubar a execução. Sem isso, meia dúzia de alunos sem resultado
de prova pararia a pipeline de um país inteiro; com isso, o desvio vira métrica
monitorada e só vira incidente quando passa do limite acordado.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.logging_conf import log

LOG = log(__name__)

REGRAS_DE_LINHA = {"not_null", "range", "valores_permitidos", "regex", "chave_estrangeira"}


@dataclass
class ResultadoCheck:
    tipo: str
    coluna: str | None
    critico: bool
    passou: bool
    detalhe: str

    @property
    def status(self) -> str:
        return "PASS" if self.passou else ("FAIL" if self.critico else "WARN")


@dataclass
class RelatorioQualidade:
    tabela: str
    camada: str
    registros_entrada: int = 0
    registros_validos: int = 0
    registros_quarentena: int = 0
    checks: list[ResultadoCheck] = field(default_factory=list)

    @property
    def score(self) -> float:
        if not self.checks:
            return 100.0
        return round(100 * sum(c.passou for c in self.checks) / len(self.checks), 1)

    @property
    def criticos_falhos(self) -> int:
        return sum(1 for c in self.checks if not c.passou and c.critico)

    def como_dict(self) -> dict[str, Any]:
        return {
            "tabela": self.tabela,
            "camada": self.camada,
            "registros_entrada": self.registros_entrada,
            "registros_validos": self.registros_validos,
            "registros_quarentena": self.registros_quarentena,
            "score_qualidade": self.score,
            "checks_total": len(self.checks),
            "checks_falhos": sum(1 for c in self.checks if not c.passou),
            "checks_criticos_falhos": self.criticos_falhos,
            "detalhe_checks": [
                {
                    "tipo": c.tipo,
                    "coluna": c.coluna,
                    "critico": c.critico,
                    "status": c.status,
                    "detalhe": c.detalhe,
                }
                for c in self.checks
            ],
        }


# --------------------------------------------------------------------------- #
# Predicados de linha
# --------------------------------------------------------------------------- #
def _com_nulo(condicao: Column, coluna: str, check: dict) -> Column:
    """
    Torna o predicado à prova de nulo.

    Sem isso, uma linha com valor nulo produziria condição NULL e escaparia tanto
    do conjunto válido quanto da quarentena — sumindo da pipeline em silêncio.
    `permite_nulo` (padrão: true) decide se ausência é aceitável naquele campo.
    """
    if check.get("permite_nulo", True):
        condicao = condicao | F.col(coluna).isNull()
    return F.coalesce(condicao, F.lit(False))


def _limite(rel: RelatorioQualidade, check: dict) -> int:
    """Quantidade de registros que o check tolera reprovar sem falhar."""
    tolerancia = float(check.get("tolerancia_pct", 0.0))
    return int(rel.registros_entrada * tolerancia / 100)


def _dentro_da_tolerancia(invalidos: int, rel: RelatorioQualidade, check: dict) -> bool:
    return invalidos <= _limite(rel, check)


def _detalhe(invalidos: int, rel: RelatorioQualidade, check: dict, sufixo: str) -> str:
    limite = _limite(rel, check)
    pct = 100 * invalidos / rel.registros_entrada if rel.registros_entrada else 0.0
    extra = f" | tolerância={check.get('tolerancia_pct', 0)}% ({limite})" if limite else ""
    return f"{invalidos} registro(s) ({pct:.2f}%) {sufixo}{extra}"


def _predicado(check: dict, df: DataFrame, referencias: dict[str, DataFrame]) -> Column | None:
    tipo, coluna = check["tipo"], check.get("coluna")

    if coluna and coluna not in df.columns:
        return None

    if tipo == "not_null":
        cond = F.col(coluna).isNotNull()
        if dict(df.dtypes).get(coluna) == "string":
            cond = cond & (F.trim(F.col(coluna)) != "")
        return cond

    if tipo == "range":
        minimo, maximo = check["valor"]
        base = (F.col(coluna) >= F.lit(minimo)) & (F.col(coluna) <= F.lit(maximo))
        return _com_nulo(base, coluna, check)

    if tipo == "valores_permitidos":
        return _com_nulo(F.col(coluna).isin(check["valor"]), coluna, check)

    if tipo == "regex":
        return _com_nulo(F.col(coluna).rlike(check["valor"]), coluna, check)

    # chave_estrangeira é tratada em `aplicar` (precisa de join com a referência).
    LOG.warning("[DQ] Tipo de check desconhecido: %s", tipo)
    return None


def aplicar(
    df: DataFrame,
    contrato: dict,
    tabela: str,
    camada: str,
    referencias: dict[str, DataFrame] | None = None,
) -> tuple[DataFrame, DataFrame, RelatorioQualidade]:
    """
    Aplica o contrato e devolve `(validos, quarentena, relatorio)`.

    A quarentena carrega a coluna `_motivo_quarentena`, com a lista de regras
    violadas por aquele registro — o que torna o dado reprovado auditável.
    """
    referencias = referencias or {}
    checks = contrato.get("checks", [])
    rel = RelatorioQualidade(tabela=tabela, camada=camada)

    df = df.cache()
    rel.registros_entrada = df.count()

    motivos: list[Column] = []
    auxiliares: list[str] = []  # colunas técnicas criadas pelos checks (ex.: FK)
    spark: SparkSession = df.sparkSession

    for check in checks:
        tipo = check["tipo"]
        coluna = check.get("coluna")
        critico = bool(check.get("critico", True))

        # ---------------------------------------------------------- tabela
        if tipo == "min_count":
            ok = rel.registros_entrada >= check["valor"]
            rel.checks.append(
                ResultadoCheck(tipo, None, critico, ok,
                               f"registros={rel.registros_entrada} minimo={check['valor']}")
            )
            continue

        if tipo == "unico":
            colunas = check.get("colunas", [coluna])
            distintos = df.select(*colunas).distinct().count()
            dups = rel.registros_entrada - distintos
            rel.checks.append(
                ResultadoCheck(tipo, ",".join(colunas), critico, dups == 0,
                               f"{dups} registro(s) duplicado(s)")
            )
            continue

        # ------------------------------------------------------------ linha
        if tipo == "chave_estrangeira":
            ref = referencias.get(check["referencia"])
            if ref is None:
                LOG.warning("[DQ] Referência '%s' ausente — check ignorado", check["referencia"])
                continue
            ref_col = check.get("coluna_referencia", coluna)
            marcador = f"__fk_ok_{coluna}"
            chaves = ref.select(F.col(ref_col).alias("__fk_ref")).distinct()
            df = (
                df.join(chaves, df[coluna] == F.col("__fk_ref"), "left")
                .withColumn(marcador, F.col("__fk_ref").isNotNull())
                .drop("__fk_ref")
            )
            auxiliares.append(marcador)
            cond = F.col(marcador)
            invalidos = df.filter(~cond).count()
            rel.checks.append(
                ResultadoCheck(tipo, coluna, critico, _dentro_da_tolerancia(invalidos, rel, check),
                               _detalhe(invalidos, rel, check,
                                        f"chave(s) órfã(s) vs {check['referencia']}"))
            )
            if critico:
                motivos.append(F.when(~cond, F.lit(f"{tipo}:{coluna}")))
            continue

        if tipo in REGRAS_DE_LINHA:
            cond = _predicado(check, df, referencias)
            if cond is None:
                continue
            invalidos = df.filter(~cond).count()
            rel.checks.append(
                ResultadoCheck(tipo, coluna, critico, _dentro_da_tolerancia(invalidos, rel, check),
                               _detalhe(invalidos, rel, check, f"violam {tipo}"))
            )
            # Só regra CRÍTICA manda o registro para a quarentena. Regra de
            # aviso (critico: false) sinaliza o desvio no relatório e deixa o
            # dado seguir — é o caso de faixas geográficas e outliers plausíveis.
            if critico:
                motivos.append(F.when(~cond, F.lit(f"{tipo}:{coluna}")))

    # ------------------------------------------------------- particionamento
    if motivos:
        motivo_col = F.concat_ws("|", F.array_compact(F.array(*motivos)))
        marcado = df.withColumn("_motivo_quarentena", motivo_col).cache()
        validos = (
            marcado.filter(F.col("_motivo_quarentena") == "")
            .drop("_motivo_quarentena", *auxiliares)
        )
        quarentena = marcado.filter(F.col("_motivo_quarentena") != "").drop(*auxiliares)
    else:
        validos = df.drop(*auxiliares)
        quarentena = spark.createDataFrame([], validos.schema).withColumn(
            "_motivo_quarentena", F.lit(None).cast("string")
        )

    rel.registros_validos = validos.count()
    rel.registros_quarentena = quarentena.count()

    for c in rel.checks:
        linha = f"[DQ:{camada.upper()}] {c.status} | {tabela} | {c.tipo} | coluna={c.coluna} | {c.detalhe}"
        (LOG.info if c.passou else (LOG.error if c.critico else LOG.warning))(linha)

    LOG.info(
        "[DQ:%s] %s | score=%.1f%% | validos=%d | quarentena=%d",
        camada.upper(), tabela, rel.score, rel.registros_validos, rel.registros_quarentena,
    )
    return validos, quarentena, rel
