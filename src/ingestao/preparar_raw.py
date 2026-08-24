"""
Preparação da camada RAW (as seis entidades exigidas pelo desafio).

Este script materializa, em `data/raw/`, os arquivos que a pipeline ingere:

    uf.csv
    municipio.csv
    contexto_socioeconomico_municipio.csv   (fonte externa opcional — Atlas/PNUD)
    meta_alfabetizacao_brasil.csv
    meta_alfabetizacao_uf.csv
    meta_alfabetizacao_municipio.csv
    aluno.csv

Proveniência (honestidade sobre o dado)
---------------------------------------
* REAL: malha de UFs e municípios (IBGE), contexto socioeconômico (Atlas do
  Desenvolvimento Humano / Censo 2010), série nacional do Indicador Criança
  Alfabetizada e resultados por UF publicados pelo INEP/MEC.
* DERIVADO: metas por UF e município, calculadas pela regra de trajetória do
  Compromisso Nacional (convergência linear para 80% em 2030).
* SIMULADO_CALIBRADO: o recorte **municipal** do indicador e os microdados de
  aluno. O INEP publica esse grão apenas em consulta interativa, sem arquivo
  aberto. A simulação não é aleatória: cada município recebe um valor
  condicionado às suas variáveis socioeconômicas REAIS e reescalado para que a
  média ponderada por matrículas de cada UF reproduza o valor REAL publicado
  daquela UF. Toda linha carrega a coluna `origem_valor`.

Trocar a simulação pelo dado oficial é uma questão de substituir os arquivos em
`data/externo/` — a pipeline não muda.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from src.config import CFG
from src.ingestao import fontes_oficiais as fo
from src.logging_conf import log

LOG = log(__name__)

# Dispersão típica do indicador entre municípios de uma mesma UF (p.p.).
DISPERSAO_MUNICIPAL = 11.0
# Fração da população de 6 a 10 anos que corresponde a um ano escolar na rede pública.
FATOR_MATRICULA = 0.85 / 5
N_ALUNOS_AMOSTRA = 60_000


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def _z(serie: pd.Series) -> pd.Series:
    desvio = serie.std(ddof=0)
    if not desvio or np.isnan(desvio):
        return pd.Series(np.zeros(len(serie)), index=serie.index)
    return (serie - serie.mean()) / desvio


def _hash_aluno(id_municipio: int, ano: int, seq: int) -> str:
    """Pseudonimização: o identificador do aluno nunca é reversível."""
    bruto = f"{id_municipio}-{ano}-{seq}".encode()
    return hashlib.sha256(bruto).hexdigest()[:16]


def _phi_inv(p: np.ndarray) -> np.ndarray:
    """Quantil da normal padrão (Acklam / aproximação racional) — evita SciPy."""
    p = np.clip(p, 1e-6, 1 - 1e-6)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
         1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
         6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
         -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
         3.754408661907416e00]
    plow, phigh = 0.02425, 1 - 0.02425
    x = np.zeros_like(p)

    baixo = p < plow
    q = np.sqrt(-2 * np.log(p[baixo]))
    x[baixo] = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)

    alto = p > phigh
    q = np.sqrt(-2 * np.log(1 - p[alto]))
    x[alto] = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)

    meio = ~(baixo | alto)
    q = p[meio] - 0.5
    r = q * q
    x[meio] = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
              (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    return x


# --------------------------------------------------------------------------- #
# Entidades territoriais
# --------------------------------------------------------------------------- #
def construir_uf() -> pd.DataFrame:
    est = pd.read_csv(CFG.dir_externo / "ibge_estados.csv", encoding="utf-8-sig")
    uf = est.rename(
        columns={
            "codigo_uf": "id_uf",
            "uf": "sigla_uf",
            "nome": "nome_uf",
            "regiao": "regiao",
            "latitude": "latitude",
            "longitude": "longitude",
        }
    )[["id_uf", "sigla_uf", "nome_uf", "regiao", "latitude", "longitude"]]
    uf["fonte"] = "IBGE"
    return uf.sort_values("id_uf").reset_index(drop=True)


def construir_municipio(uf: pd.DataFrame) -> pd.DataFrame:
    mun = pd.read_csv(CFG.dir_externo / "ibge_municipios.csv", encoding="utf-8")
    mun = mun.rename(
        columns={
            "codigo_ibge": "id_municipio",
            "nome": "nome_municipio",
            "codigo_uf": "id_uf",
        }
    )
    mun = mun.merge(uf[["id_uf", "sigla_uf", "regiao"]], on="id_uf", how="left")
    mun["capital"] = mun["capital"].astype(int)
    mun = mun[
        [
            "id_municipio", "nome_municipio", "id_uf", "sigla_uf", "regiao",
            "capital", "latitude", "longitude", "ddd",
        ]
    ]
    mun["fonte"] = "IBGE"
    return mun.sort_values("id_municipio").reset_index(drop=True)


def construir_contexto() -> pd.DataFrame:
    """
    Contexto socioeconômico do Atlas do Desenvolvimento Humano.

    Atenção à chave: o Atlas publica o código de município do IBGE com **6
    dígitos** (sem o dígito verificador), enquanto a malha territorial usa 7.
    A raw preserva o código como veio da fonte — a normalização para a chave de
    7 dígitos acontece na Silver, que é onde a integração é responsabilidade.
    """
    ctx = pd.read_csv(CFG.dir_externo / "atlas_idhm_2010_municipios.csv")
    ctx = ctx.drop(columns=["nome_municipio_atlas", "id_uf"])
    ctx = ctx.rename(columns={"id_municipio": "id_municipio_6dig"})
    ctx["ano_referencia"] = 2010
    ctx["fonte"] = "ATLAS_DESENVOLVIMENTO_HUMANO_PNUD_CENSO2010"
    return ctx


# --------------------------------------------------------------------------- #
# Indicador e metas
# --------------------------------------------------------------------------- #
def construir_meta_brasil() -> pd.DataFrame:
    linhas = []
    anos = sorted(set(CFG.anos) | set(CFG.anos_meta))
    for ano in anos:
        meta, origem_meta = fo.meta_brasil(ano) if ano >= 2024 else (None, None)
        realizado = fo.INDICADOR_BRASIL.get(ano)
        linhas.append(
            {
                "ano": ano,
                "meta_pct": meta,
                "indicador_pct": realizado,
                "origem_meta": origem_meta,
                "origem_indicador": fo.PUBLICADO if realizado is not None else None,
                "ponto_corte_saeb": CFG.ponto_corte_saeb,
                "fonte": "INEP_MEC_COMPROMISSO_NACIONAL",
            }
        )
    return pd.DataFrame(linhas)


def construir_meta_uf(uf: pd.DataFrame) -> pd.DataFrame:
    linhas = []
    anos = sorted(set(CFG.anos) | set(CFG.anos_meta))
    for sigla in uf["sigla_uf"]:
        for ano in anos:
            realizado = fo.INDICADOR_UF.get(sigla, {}).get(ano)
            if ano >= 2024:
                meta, origem_meta = fo.meta_uf(sigla, ano)
            else:
                meta, origem_meta = None, None
            linhas.append(
                {
                    "ano": ano,
                    "sigla_uf": sigla,
                    "meta_pct": meta,
                    "indicador_pct": realizado,
                    "origem_meta": origem_meta,
                    "origem_indicador": fo.PUBLICADO if realizado is not None else fo.NAO_COLETADO,
                    "fonte": "INEP_MEC",
                }
            )
    return pd.DataFrame(linhas)


def _media_uf_ano(sigla: str, ano: int) -> tuple[float, str]:
    """Média-alvo da UF no ano: valor publicado ou estimativa pela variação nacional."""
    publicado = fo.INDICADOR_UF.get(sigla, {}).get(ano)
    if publicado is not None:
        return publicado, fo.PUBLICADO

    base = fo.INDICADOR_UF.get(sigla, {}).get(2024)
    if base is None:  # UF sem coleta em nenhum ano (Roraima)
        return fo.INDICADOR_BRASIL[ano], fo.SIMULADO
    delta_nacional = fo.INDICADOR_BRASIL[ano] - fo.INDICADOR_BRASIL[2024]
    return float(np.clip(base + delta_nacional, 3, 99)), fo.SIMULADO


def construir_meta_municipio(
    municipio: pd.DataFrame, contexto: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    base = municipio.assign(
        id_municipio_6dig=municipio["id_municipio"] // 10
    ).merge(contexto, on="id_municipio_6dig", how="left")

    # Matrículas avaliadas no 2º ano da rede pública (proxy real: população 6-10 anos).
    matriculas = (base["populacao_6a10_anos"].fillna(400) * FATOR_MATRICULA).round()
    base["matriculas_avaliadas"] = matriculas.clip(lower=5).astype(int)

    # Escore latente a partir de variáveis socioeconômicas REAIS.
    latente = (
        1.00 * _z(base["idhm_educacao"].fillna(base["idhm_educacao"].mean()))
        - 0.55 * _z(base["pct_criancas_pobres"].fillna(base["pct_criancas_pobres"].mean()))
        - 0.35 * _z(base["taxa_analfabetismo_15mais"].fillna(base["taxa_analfabetismo_15mais"].mean()))
        + 0.40 * _z(base["pct_6a14_fund_sem_atraso"].fillna(base["pct_6a14_fund_sem_atraso"].mean()))
    )
    base["latente"] = _z(latente)

    saida = []
    for ano in CFG.anos:
        for sigla, grupo in base.groupby("sigla_uf", sort=True):
            alvo, origem = _media_uf_ano(sigla, ano)
            ruido = rng.normal(0, 0.45, size=len(grupo))
            valores = alvo + DISPERSAO_MUNICIPAL * (_z(grupo["latente"]).to_numpy() + ruido)
            pesos = grupo["matriculas_avaliadas"].to_numpy()

            # Reescala até a média ponderada bater com o valor REAL da UF.
            for _ in range(40):
                valores = np.clip(valores, 2.0, 99.0)
                erro = alvo - float(np.average(valores, weights=pesos))
                if abs(erro) < 0.05:
                    break
                valores = valores + erro

            alfabetizados = np.round(pesos * valores / 100).astype(int)
            saida.append(
                pd.DataFrame(
                    {
                        "ano": ano,
                        "id_municipio": grupo["id_municipio"].to_numpy(),
                        "sigla_uf": sigla,
                        "matriculas_avaliadas": pesos,
                        "alunos_alfabetizados": alfabetizados,
                        "indicador_pct": np.round(valores, 1),
                        "meta_pct": [fo.meta_uf(sigla, max(ano, 2024))[0]] * len(grupo),
                        "origem_indicador": origem if origem == fo.PUBLICADO else fo.SIMULADO,
                        "origem_meta": fo.DERIVADO,
                    }
                )
            )

    df = pd.concat(saida, ignore_index=True)
    # O grão municipal é sempre simulação calibrada — o publicado é o agregado da UF.
    df["origem_indicador"] = fo.SIMULADO
    df["fonte"] = "SIMULACAO_CALIBRADA_POR_UF"
    return df.sort_values(["ano", "id_municipio"]).reset_index(drop=True)


def construir_aluno(meta_mun: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Microdados de aluno (pseudonimizados), coerentes com o indicador municipal.

    A proficiência é amostrada de uma normal cujo centro é deslocado de forma que
    P(proficiência >= 743) reproduza o indicador do município no ano — ou seja, o
    ponto de corte de 743 da Pesquisa Alfabetiza Brasil é respeitado por construção.
    """
    amostra = meta_mun.sample(
        n=min(N_ALUNOS_AMOSTRA, len(meta_mun) * 12),
        weights=meta_mun["matriculas_avaliadas"],
        replace=True,
        random_state=CFG.semente,
    ).reset_index(drop=True)

    p = (amostra["indicador_pct"].to_numpy() / 100).clip(0.02, 0.98)
    desvio = 55.0
    media = CFG.ponto_corte_saeb + desvio * _phi_inv(p)
    proficiencia = np.round(rng.normal(media, desvio), 1)

    n = len(amostra)
    df = pd.DataFrame(
        {
            "id_aluno": [
                _hash_aluno(int(m), int(a), i)
                for i, (m, a) in enumerate(
                    zip(amostra["id_municipio"], amostra["ano"], strict=True)
                )
            ],
            "ano": amostra["ano"].to_numpy(),
            "id_municipio": amostra["id_municipio"].to_numpy(),
            "sigla_uf": amostra["sigla_uf"].to_numpy(),
            "rede": rng.choice(["MUNICIPAL", "ESTADUAL"], size=n, p=[0.78, 0.22]),
            "localizacao": rng.choice(["URBANA", "RURAL"], size=n, p=[0.86, 0.14]),
            "sexo": rng.choice(["F", "M"], size=n, p=[0.49, 0.51]),
            "idade": rng.choice([7, 8, 9], size=n, p=[0.82, 0.14, 0.04]),
            "proficiencia_saeb": proficiencia,
        }
    )
    df["alfabetizado"] = (df["proficiencia_saeb"] >= CFG.ponto_corte_saeb).astype(int)
    df["fonte"] = "SIMULACAO_CALIBRADA_POR_MUNICIPIO"
    return df


# --------------------------------------------------------------------------- #
# Sujeira controlada — para que a camada Silver tenha o que limpar
# --------------------------------------------------------------------------- #
def sujar(df: pd.DataFrame, rng: np.random.Generator, coluna_texto: str | None = None) -> pd.DataFrame:
    """
    Injeta, de forma determinística, os defeitos que aparecem em base pública real:
    duplicidade de registros, nulos, e padronização inconsistente de texto.
    Documentado em docs/governanca_qualidade.md.
    """
    n = len(df)
    duplicados = df.sample(n=max(int(n * 0.004), 3), random_state=CFG.semente)
    df = pd.concat([df, duplicados], ignore_index=True)

    if coluna_texto and coluna_texto in df.columns:
        idx = rng.choice(df.index, size=max(int(len(df) * 0.02), 5), replace=False)
        df.loc[idx, coluna_texto] = df.loc[idx, coluna_texto].astype(str).str.upper()
        idx2 = rng.choice(df.index, size=max(int(len(df) * 0.01), 3), replace=False)
        df.loc[idx2, coluna_texto] = "  " + df.loc[idx2, coluna_texto].astype(str) + "  "
    return df


# --------------------------------------------------------------------------- #
# Execução
# --------------------------------------------------------------------------- #
def executar() -> dict:
    CFG.dir_raw.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(CFG.semente)

    LOG.info("[RAW] Construindo entidades territoriais (IBGE)")
    uf = construir_uf()
    municipio = construir_municipio(uf)
    contexto = construir_contexto()

    LOG.info("[RAW] Construindo metas e indicadores")
    meta_brasil = construir_meta_brasil()
    meta_uf = construir_meta_uf(uf)
    meta_municipio = construir_meta_municipio(municipio, contexto, rng)

    LOG.info("[RAW] Construindo microdados de aluno")
    aluno = construir_aluno(meta_municipio, rng)

    LOG.info("[RAW] Injetando defeitos controlados (duplicidade, nulos, texto)")
    municipio_raw = sujar(municipio.copy(), rng, coluna_texto="nome_municipio")
    idx_nulo = rng.choice(municipio_raw.index, size=12, replace=False)
    municipio_raw.loc[idx_nulo, "latitude"] = np.nan
    meta_municipio_raw = sujar(meta_municipio.copy(), rng)
    aluno_raw = sujar(aluno.copy(), rng)
    idx_prof = rng.choice(aluno_raw.index, size=40, replace=False)
    aluno_raw.loc[idx_prof, "proficiencia_saeb"] = np.nan  # ausência de resultado
    idx_abs = rng.choice(aluno_raw.index, size=15, replace=False)
    aluno_raw.loc[idx_abs, "proficiencia_saeb"] = -1.0  # valor impossível

    arquivos = {
        "uf.csv": uf,
        "municipio.csv": municipio_raw,
        "contexto_socioeconomico_municipio.csv": contexto,
        "meta_alfabetizacao_brasil.csv": meta_brasil,
        "meta_alfabetizacao_uf.csv": meta_uf,
        "meta_alfabetizacao_municipio.csv": meta_municipio_raw,
        "aluno.csv": aluno_raw,
    }

    manifesto = {
        "gerado_em": datetime.now(UTC).isoformat(timespec="seconds"),
        "semente": CFG.semente,
        "anos": CFG.anos,
        "ponto_corte_saeb": CFG.ponto_corte_saeb,
        "arquivos": {},
    }
    for nome, df in arquivos.items():
        destino = CFG.dir_raw / nome
        df.to_csv(destino, index=False, encoding="utf-8")
        manifesto["arquivos"][nome] = {"registros": int(len(df)), "colunas": list(df.columns)}
        LOG.info("[RAW] %-42s %7d registros", nome, len(df))

    manifesto["proveniencia"] = {
        "uf.csv": "REAL (IBGE)",
        "municipio.csv": "REAL (IBGE) + defeitos controlados",
        "contexto_socioeconomico_municipio.csv": "REAL (Atlas do Desenvolvimento Humano, Censo 2010)",
        "meta_alfabetizacao_brasil.csv": "REAL (INEP/MEC) + interpolação da trajetória oficial",
        "meta_alfabetizacao_uf.csv": "REAL (INEP/MEC) para o indicador; metas derivadas por regra",
        "meta_alfabetizacao_municipio.csv": "SIMULADO calibrado pela média real de cada UF",
        "aluno.csv": "SIMULADO calibrado pelo indicador municipal (pseudonimizado)",
    }
    (CFG.dir_raw / "_manifesto.json").write_text(
        json.dumps(manifesto, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifesto


if __name__ == "__main__":  # pragma: no cover
    from src.logging_conf import configurar

    configurar()
    executar()
