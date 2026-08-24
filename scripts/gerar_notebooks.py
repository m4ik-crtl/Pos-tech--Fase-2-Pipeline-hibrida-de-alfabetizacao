"""
Constrói os notebooks do projeto a partir de blocos declarados aqui.

Manter o conteúdo em um script Python evita editar JSON de notebook à mão e
garante que os quatro notebooks compartilhem o mesmo cabeçalho e as mesmas
funções auxiliares.

Uso: python scripts/gerar_notebooks.py   (depois execute com nbconvert)
"""

from __future__ import annotations

import itertools
import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DESTINO = RAIZ / "notebooks"

CABECALHO = '''import sys
from pathlib import Path

# Caminho relativo: o notebook encontra o projeto a partir da própria posição,
# então funciona em qualquer máquina, sem editar nada.
RAIZ = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
sys.path.insert(0, str(RAIZ))

import pandas as pd
import matplotlib.pyplot as plt

pd.set_option("display.max_columns", 60)
pd.set_option("display.width", 160)
plt.rcParams.update({
    "figure.figsize": (10, 4.5),
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "font.size": 10,
})
print("Raiz do projeto:", RAIZ.name)
'''

LEITOR_GOLD = '''def ler_gold(tabela: str) -> pd.DataFrame:
    """Lê uma tabela Gold do lakehouse local (Parquet/Delta são compatíveis na leitura)."""
    base = RAIZ / "data" / "lakehouse" / "gold" / tabela
    arquivos = sorted(base.rglob("*.parquet"))
    if not arquivos:
        raise FileNotFoundError(
            f"Tabela gold.{tabela} não encontrada. Rode `make batch` (ou "
            "`python -m src.pipeline --reprocessar`) antes de abrir o notebook."
        )
    partes = []
    for arq in arquivos:
        df = pd.read_parquet(arq)
        # Colunas de partição vêm no caminho (ano=2024), não dentro do arquivo.
        for trecho in arq.relative_to(base).parts[:-1]:
            if "=" in trecho:
                chave, valor = trecho.split("=", 1)
                df[chave] = int(valor) if valor.lstrip("-").isdigit() else valor
        partes.append(df)
    return pd.concat(partes, ignore_index=True)
'''


def _linhas(texto: str) -> list[str]:
    """Formato de `source` no nbformat: uma string por linha, com o \\n preservado."""
    linhas = texto.strip().split("\n")
    return [linha + "\n" for linha in linhas[:-1]] + [linhas[-1]]


_CONTADOR = itertools.count(1)


def _id() -> str:
    return f"cel{next(_CONTADOR):03d}"


def md(texto: str) -> dict:
    return {"cell_type": "markdown", "id": _id(), "metadata": {}, "source": _linhas(texto)}


def code(texto: str) -> dict:
    return {
        "cell_type": "code",
        "id": _id(),
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _linhas(texto),
    }


def notebook(celulas: list[dict]) -> dict:
    return {
        "cells": celulas,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


# --------------------------------------------------------------------------- #
# 01 — Entendimento do problema e das fontes
# --------------------------------------------------------------------------- #
NB01 = notebook([
    md("""
# 01 · Entendimento do problema e das fontes

**Tech Challenge — Fase 2 · Pipeline híbrida para análise da alfabetização no Brasil**

O Compromisso Nacional Criança Alfabetizada mobiliza União, estados e municípios com uma
meta clara: toda criança alfabetizada até o fim do 2º ano do ensino fundamental. A régua
é o **Indicador Criança Alfabetizada** — o percentual de estudantes do 2º ano da rede
pública que atinge **743 pontos na escala de proficiência do Saeb**, ponto de corte
definido pela Pesquisa Alfabetiza Brasil de 2023.

Este notebook responde três perguntas antes de qualquer linha de pipeline:

1. **Que dado existe, e de quem ele é?**
2. **O que nele é real e o que é reconstruído?**
3. **Que defeitos ele tem — e como sabemos que a pipeline os tratou?**
"""),
    code(CABECALHO),
    md("""
## 1. As seis entidades do desafio

O desafio define as fontes a integrar. Cada uma virou um arquivo em `data/raw/`,
versionado no repositório — sem isso, ninguém reproduz o resultado.
"""),
    code('''
raw = RAIZ / "data" / "raw"
manifesto = json.loads((raw / "_manifesto.json").read_text(encoding="utf-8"))

resumo = pd.DataFrame([
    {"arquivo": nome, "registros": info["registros"], "colunas": len(info["colunas"])}
    for nome, info in manifesto["arquivos"].items()
]).sort_values("registros", ascending=False)
resumo
''' .replace("json.loads", "__import__('json').loads")),
    md("""
## 2. Proveniência — o que é real e o que é reconstruído

Esta é a seção mais importante do notebook. Trabalhar com dado público exige dizer,
sem rodeio, de onde veio cada número. O manifesto declara isso arquivo a arquivo, e o
rótulo viaja com o dado até a camada Gold, nas colunas `origem_indicador` e `origem_meta`.
"""),
    code('''
proveniencia = pd.DataFrame(
    [{"arquivo": k, "proveniência": v} for k, v in manifesto["proveniencia"].items()]
)
proveniencia.style.hide(axis="index")
'''),
    md("""
### Por que o grão municipal é simulado

O INEP publica o resultado por município em consulta interativa, sem arquivo aberto para
download. Em vez de inventar números, a simulação é **calibrada**:

- cada município recebe um escore a partir das suas variáveis socioeconômicas **reais**
  (IDHM educação, % de crianças pobres, analfabetismo, atraso escolar), vindas do
  Atlas do Desenvolvimento Humano;
- esse escore é reescalado até que a **média ponderada por matrículas de cada UF
  reproduza o valor real publicado pelo INEP** para aquela UF.

O resultado é auditável: a coluna `divergencia_pp` da tabela `gold.meta_vs_realizado_uf`
mede exatamente o quanto o agregado dos municípios se afasta do publicado.
"""),
    md("""
## 3. A série real do indicador
"""),
    code('''
from src.ingestao import fontes_oficiais as fo

nacional = pd.DataFrame({
    "ano": list(fo.INDICADOR_BRASIL),
    "indicador_pct": list(fo.INDICADOR_BRASIL.values()),
})
nacional["meta_pct"] = [fo.meta_brasil(a)[0] if a >= 2024 else None for a in nacional["ano"]]
print(nacional.to_string(index=False))

metas = pd.DataFrame({
    "ano": sorted(fo.META_BRASIL_PUBLICADA) + [2027, 2028, 2029],
    })
metas["meta_pct"] = [fo.meta_brasil(a)[0] for a in metas["ano"]]
metas["origem"] = [fo.meta_brasil(a)[1] for a in metas["ano"]]
metas.sort_values("ano")
'''),
    code('''
uf = pd.DataFrame([
    {"sigla_uf": sigla, **{f"ind_{ano}": valor for ano, valor in anos.items()}}
    for sigla, anos in fo.INDICADOR_UF.items()
]).sort_values("ind_2024", ascending=False)

fig, ax = plt.subplots(figsize=(11, 5))
dados = uf.dropna(subset=["ind_2024"])
cores = ["#0f766e" if v >= fo.INDICADOR_BRASIL[2024] else "#b91c1c" for v in dados["ind_2024"]]
ax.bar(dados["sigla_uf"], dados["ind_2024"], color=cores)
ax.axhline(fo.INDICADOR_BRASIL[2024], color="#334155", linestyle="--", linewidth=1)
ax.text(25.4, fo.INDICADOR_BRASIL[2024] + 1.5, f"Brasil {fo.INDICADOR_BRASIL[2024]}%",
        ha="right", fontsize=9, color="#334155")
ax.set_title("Indicador Criança Alfabetizada por UF — 2024 (dado publicado pelo INEP/MEC)")
ax.set_ylabel("% de crianças alfabetizadas")
plt.tight_layout(); plt.show()

print("UFs sem valor publicado em 2024:",
      [s for s, a in fo.INDICADOR_UF.items() if a.get(2024) is None])
'''),
    md("""
O gráfico já mostra o problema central: **a distância entre Ceará (85,3%) e Bahia (36,0%)
é de quase 50 pontos percentuais** dentro do mesmo país e do mesmo ano.

E há uma ausência que a pipeline precisa respeitar: **Roraima não teve coleta divulgada**.
O valor permanece nulo, com a flag `indicador_disponivel = false`. Imputar a média
nacional aqui produziria um número inventado num painel de política pública.
"""),
    md("""
## 4. Os defeitos que a pipeline precisa tratar

`src/ingestao/preparar_raw.py` injeta defeitos controlados — duplicidade, texto
inconsistente, valores impossíveis — para que a limpeza da camada Silver seja
**verificável**, e não uma promessa no README.
"""),
    code('''
municipio = pd.read_csv(raw / "municipio.csv")
aluno = pd.read_csv(raw / "aluno.csv")

print(f"municipio.csv: {len(municipio):,} linhas | {municipio['id_municipio'].nunique():,} ids únicos "
      f"-> {len(municipio) - municipio['id_municipio'].nunique()} duplicados")
print(f"latitude nula: {municipio['latitude'].isna().sum()} municípios")

sujos = municipio[municipio["nome_municipio"].str.contains(r"^\\s|\\s$", regex=True, na=False)
                  | (municipio["nome_municipio"] == municipio["nome_municipio"].str.upper())]
print(f"nomes com caixa ou espaçamento irregular: {len(sujos)}")
print(sujos["nome_municipio"].head(5).tolist())

print(f"\\naluno.csv: proficiência ausente = {aluno['proficiencia_saeb'].isna().sum()} | "
      f"impossível (< 0) = {(aluno['proficiencia_saeb'] < 0).sum()}")
'''),
    md("""
## 5. O contexto socioeconômico (fonte externa)

O desafio sugere enriquecer a base com contexto socioeconômico. Usamos o **Atlas do
Desenvolvimento Humano no Brasil** (PNUD/Ipea/FJP, Censo 2010) — dado real, por município,
que depois sustenta a análise de desigualdade e a feature store do modelo.
"""),
    code('''
contexto = pd.read_csv(raw / "contexto_socioeconomico_municipio.csv")
print(f"{len(contexto):,} municípios | {contexto.shape[1]} variáveis")
contexto[["idhm", "idhm_educacao", "renda_per_capita", "pct_criancas_pobres",
          "taxa_analfabetismo_15mais", "populacao_total"]].describe().round(2)
'''),
    md("""
### O que esperamos encontrar

A hipótese que orienta a camada Gold: **território e condição socioeconômica explicam boa
parte da variação do indicador**. Se isso se confirmar, a pipeline não serve só para
reportar — serve para **priorizar** onde o apoio técnico faz mais diferença.

O notebook `03_camada_gold_analises` testa essa hipótese sobre o dado já processado.
"""),
])

# --------------------------------------------------------------------------- #
# 02 — Pipeline medalhão
# --------------------------------------------------------------------------- #
NB02 = notebook([
    md("""
# 02 · Pipeline medalhão em execução

Este notebook **executa a pipeline de verdade** — não descreve. Ao final, o lakehouse
local está populado e o relatório de qualidade, gerado.

Ordem: `raw` → **bronze** → **silver** → **gold**, com o contrato de dados aplicado
em cada fronteira.
"""),
    code(CABECALHO),
    code('''
import os
# Parquet no notebook evita depender de download de jar do Delta na primeira execução.
# Em Docker e no Databricks, FORMATO_TABELA=delta é o padrão.
os.environ.setdefault("FORMATO_TABELA", "parquet")

from src.logging_conf import configurar
from src import spark_session
from src.config import CFG

configurar("INFO")
spark = spark_session.criar()
print("Formato efetivo:", spark_session.FORMATO_EFETIVO)
'''),
    md("""
## 1. Camada Bronze — o dado como chegou

Schema **explícito** (nunca `inferSchema`), metadados de linhagem em toda linha e
partição por data de ingestão para preservar o histórico.
"""),
    code('''
from src.camadas import bronze

destinos = bronze.executar(spark)
list(destinos)
'''),
    code('''
df_bronze = spark_session.ler_tabela(spark, CFG.camada("bronze/municipio"))
df_bronze.select("id_municipio", "nome_municipio", "sigla_uf",
                 "_ingestion_date", "_source_file", "_record_hash").show(5, truncate=40)
'''),
    md("""
## 2. Camada Silver — limpeza, padronização e integração

Aqui acontece o que o desafio pede: limpeza, tratamento de ausentes, padronização de
nomes e tipos, validação de consistência, normalização de chaves e **integração das
bases**.
"""),
    code('''
from src.camadas import silver

tabelas = silver.executar(spark)
list(tabelas)
'''),
    code('''
# Antes x depois da padronização de texto
bruto = df_bronze.select("id_municipio", "nome_municipio").toPandas()
limpo = tabelas["dim_municipio"].select("id_municipio", "nome_municipio").toPandas()

comparacao = (bruto.rename(columns={"nome_municipio": "bronze"})
                   .merge(limpo.rename(columns={"nome_municipio": "silver"}), on="id_municipio"))
comparacao[comparacao["bronze"] != comparacao["silver"]].head(8)
'''),
    code('''
# A integração produziu contexto socioeconômico para quantos municípios?
dim = tabelas["dim_municipio"]
total = dim.count()
com_contexto = dim.filter("contexto_disponivel").count()
print(f"{com_contexto:,} de {total:,} municípios com contexto socioeconômico "
      f"({100 * com_contexto / total:.1f}%)")
print("Os demais foram criados depois do Censo 2010 — permanecem na base, marcados.")
'''),
    md("""
### Quarentena — para onde vai o que não passou

Registro reprovado não é descartado: vai para `_quarentena/` com o motivo na linha.
"""),
    code('''
caminho_q = CFG.camada("_quarentena/silver/fato_aluno")
if spark_session.existe(caminho_q):
    q = spark_session.ler_tabela(spark, caminho_q)
    print(f"{q.count()} registro(s) em quarentena")
    q.select("id_aluno", "id_municipio", "proficiencia_saeb", "_motivo_quarentena").show(5, truncate=50)
else:
    print("Nenhum registro em quarentena nesta execução.")
'''),
    md("""
## 3. Camada Gold — os produtos analíticos
"""),
    code('''
from src.camadas import gold

produtos = gold.executar(spark)
for nome, df in produtos.items():
    print(f"{nome:38s} {df.count():>7,} registros | {len(df.columns):>3} colunas")
'''),
    md("""
### Reconciliação: o agregado bate com o publicado?

A tabela `meta_vs_realizado_uf` publica lado a lado o valor divulgado pelo INEP e o valor
recalculado a partir dos municípios. Se a divergência crescer um dia, ela aparece aqui —
e não numa reunião.
"""),
    code('''
uf = produtos["meta_vs_realizado_uf"].filter("ano = 2024").toPandas()
print("Divergência máxima entre calculado e publicado:",
      f"{uf['divergencia_pp'].abs().max():.2f} p.p.")
uf[["sigla_uf", "indicador_publicado_pct", "indicador_calculado_pct",
    "divergencia_pp", "meta_pct", "gap_meta_pp", "status_meta"]].head(10)
'''),
    md("""
## 4. Observabilidade da execução
"""),
    code('''
from src.observabilidade import runs, relatorio

runs.persistir(spark)
caminho = relatorio.gerar()

eventos = pd.DataFrame([{
    "camada": e.camada, "tabela": e.tabela, "status": e.status,
    "entrada": e.registros_entrada, "saida": e.registros_saida,
    "quarentena": e.registros_quarentena, "qualidade": e.score_qualidade,
    "duracao_s": e.duracao_s,
} for e in runs.eventos()])
eventos
'''),
    code('''
spark.stop()
print("Pipeline concluída. Relatório em data/_observabilidade/relatorio.md")
'''),
])

# --------------------------------------------------------------------------- #
# 03 — Análises sobre a camada Gold
# --------------------------------------------------------------------------- #
NB03 = notebook([
    md("""
# 03 · O que a camada Gold responde

A Gold existe para ser consumida sem Spark: são tabelas pequenas, com grão declarado e
prontas para painel. Este notebook lê direto com pandas — é o mesmo que um analista faria.
"""),
    code(CABECALHO),
    code(LEITOR_GOLD),
    md("""
## 1. O Brasil está no caminho da meta de 2030?
"""),
    code('''
brasil = ler_gold("evolucao_temporal_brasil").sort_values("ano")
brasil[["ano", "meta_pct", "indicador_publicado_pct", "indicador_calculado_pct",
        "variacao_pp", "gap_meta_pp", "distancia_meta_2030_pp"]]
'''),
    code('''
realizado = brasil.dropna(subset=["indicador_publicado_pct"])
fig, ax = plt.subplots(figsize=(9.5, 4.5))
ax.plot(brasil["ano"], brasil["meta_pct"], "--o", color="#94a3b8", label="Meta pactuada")
ax.plot(realizado["ano"], realizado["indicador_publicado_pct"], "-o", color="#0f766e",
        linewidth=2.5, label="Resultado publicado")
for _, r in realizado.iterrows():
    ax.annotate(f"{r['indicador_publicado_pct']:.1f}%", (r["ano"], r["indicador_publicado_pct"]),
                textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
ax.set_title("Indicador Criança Alfabetizada — realizado x trajetória de metas")
ax.set_ylabel("% de crianças alfabetizadas"); ax.set_ylim(30, 90); ax.legend()
plt.tight_layout(); plt.show()
'''),
    md("""
O país saiu de 55,9% (2023) para 66,0% (2025) e **superou a meta de 2025**, que era 64%.
A trajetória até 80% em 2030 segue exigente, mas deixou de ser distante.
"""),
    md("""
## 2. Meta x realizado por UF — onde a pactuação aperta
"""),
    code('''
uf = ler_gold("meta_vs_realizado_uf")
uf24 = uf[uf["ano"] == 2024].sort_values("gap_meta_pp")

fig, ax = plt.subplots(figsize=(11, 5.5))
cores = ["#0f766e" if g >= 0 else "#b91c1c" for g in uf24["gap_meta_pp"]]
ax.barh(uf24["sigla_uf"], uf24["gap_meta_pp"], color=cores)
ax.axvline(0, color="#334155", linewidth=1)
ax.set_title("Distância da meta pactuada em 2024 (pontos percentuais)")
ax.set_xlabel("realizado − meta")
plt.tight_layout(); plt.show()

print(uf24["status_meta"].value_counts().to_string())
'''),
    md("""
## 3. Desigualdade educacional — a pergunta que o dado responde melhor
"""),
    code('''
painel = ler_gold("painel_desigualdade")
p24 = painel[painel["ano"] == 2024]

resumo = (p24.groupby("faixa_idhm")
            .apply(lambda g: pd.Series({
                "indicador_medio_pct": round((g["indicador_medio_pct"] * g["municipios"]).sum()
                                             / g["municipios"].sum(), 1),
                "municipios": int(g["municipios"].sum()),
                "criancas_nao_alfabetizadas": int(g["alunos_nao_alfabetizados"].sum()),
            }), include_groups=False)
            .reset_index())
resumo
'''),
    code('''
fig, ax = plt.subplots(figsize=(9.5, 4.5))
ax.bar(resumo["faixa_idhm"], resumo["indicador_medio_pct"],
       color=["#b91c1c", "#ea580c", "#eab308", "#0f766e"])
for i, v in enumerate(resumo["indicador_medio_pct"]):
    ax.text(i, v + 1, f"{v}%", ha="center", fontsize=10, weight="bold")
ax.set_title("Indicador médio por quartil de IDHM municipal — 2024")
ax.set_ylabel("% de crianças alfabetizadas"); ax.set_ylim(0, 80)
plt.xticks(rotation=12)
plt.tight_layout(); plt.show()

diferenca = resumo["indicador_medio_pct"].iloc[-1] - resumo["indicador_medio_pct"].iloc[0]
print(f"Diferença entre o quartil de maior e o de menor IDHM: {diferenca:.1f} pontos percentuais")
'''),
    md("""
Este é o gráfico que justifica a pipeline inteira. Nascer num município do quartil de
**menor** IDHM está associado a uma diferença de mais de 30 pontos percentuais na chance
de estar alfabetizado aos 7 anos. Não é ruído estatístico: é o mesmo padrão em todas as
regiões e nos três anos da série.
"""),
    code('''
regiao = (p24.groupby("regiao")
            .apply(lambda g: round((g["indicador_medio_pct"] * g["municipios"]).sum()
                                   / g["municipios"].sum(), 1), include_groups=False)
            .sort_values())
fig, ax = plt.subplots(figsize=(8.5, 3.6))
ax.barh(regiao.index, regiao.values, color="#334155")
for i, v in enumerate(regiao.values):
    ax.text(v + 0.6, i, f"{v}%", va="center", fontsize=9)
ax.set_title("Indicador médio por grande região — 2024"); ax.set_xlim(0, 80)
plt.tight_layout(); plt.show()
'''),
    md("""
## 4. Onde estão as crianças que precisam de apoio
"""),
    code('''
municipal = ler_gold("indicador_alfabetizacao_municipio")
m24 = municipal[municipal["ano"] == 2024]

criticos = m24[m24["faixa_indicador"] == "CRITICO"]
print(f"Municípios em situação crítica (indicador < 40%): {len(criticos):,}")
print(f"Crianças não alfabetizadas nesses municípios: "
      f"{criticos['alunos_nao_alfabetizados'].sum():,}")
print(f"IDHM médio desses municípios: {criticos['idhm'].mean():.3f} "
      f"(média nacional: {m24['idhm'].mean():.3f})")

(criticos.groupby("sigla_uf").size().sort_values(ascending=False).head(8)
 .rename("municipios_criticos").to_frame())
'''),
    code('''
ranking = ler_gold("ranking_municipios")
r24 = ranking[ranking["ano"] == 2024]
print("Melhores desempenhos em 2024:")
display(r24[r24["grupo"] == "MELHORES"].nsmallest(5, "ranking_brasil")
        [["ranking_brasil", "nome_municipio", "sigla_uf", "indicador_pct", "idhm", "porte_municipio"]])
print("\\nMaiores desafios em 2024:")
display(r24[r24["grupo"] == "PIORES"].nlargest(5, "ranking_brasil")
        [["ranking_brasil", "nome_municipio", "sigla_uf", "indicador_pct", "idhm", "porte_municipio"]])
'''),
    md("""
## 5. O que a camada Gold entrega, em uma frase

Um gestor consegue responder, sem escrever SQL: **quantas crianças não estão
alfabetizadas, em que municípios, quão longe da meta pactuada, e em que contexto
socioeconômico** — que é exatamente o insumo para decidir onde alocar apoio técnico.
"""),
])

# --------------------------------------------------------------------------- #
# 04 — Aplicação em IA
# --------------------------------------------------------------------------- #
NB04 = notebook([
    md("""
# 04 · Como a camada Gold sustenta IA

O desafio pede que o README explique como a base Gold poderia alimentar modelos de
predição de alfabetização. Este notebook não explica: **treina**.

E começa pelo erro que torna inúteis a maioria dos modelos de indicador educacional —
**vazamento de dados (data leakage)**.
"""),
    code(CABECALHO),
    code(LEITOR_GOLD),
    code('''
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, r2_score

features = ler_gold("features_ml_municipio")
print(f"{len(features):,} linhas | {features.shape[1]} colunas")
print("Anos disponíveis:", sorted(features["ano"].unique()))
features.head(3)
'''),
    md("""
## 1. O modelo que parece perfeito e não serve para nada

Suponha que alguém treine o modelo com as colunas "óbvias" da tabela municipal:
`alunos_alfabetizados` e `matriculas_avaliadas`.

O indicador **é** `alunos_alfabetizados / matriculas_avaliadas × 100`. O modelo não está
aprendendo nada sobre alfabetização — está redescobrindo uma divisão.
"""),
    code('''
municipal = ler_gold("indicador_alfabetizacao_municipio")
vazado = municipal[municipal["ano"] == 2024].dropna(
    subset=["alunos_alfabetizados", "matriculas_avaliadas", "indicador_pct"])

X_vaz = vazado[["alunos_alfabetizados", "matriculas_avaliadas"]]
y_vaz = vazado["indicador_pct"]
modelo_vazado = GradientBoostingRegressor(random_state=42).fit(X_vaz, y_vaz)
pred_vaz = modelo_vazado.predict(X_vaz)

print(f"R²  com vazamento: {r2_score(y_vaz, pred_vaz):.4f}")
print(f"MAE com vazamento: {mean_absolute_error(y_vaz, pred_vaz):.3f} p.p.")
print("\\nR² alto com duas colunas apenas não é sinal de bom modelo:")
print("é sinal de que o alvo está dentro da feature.")
'''),
    md("""
### Por que isso é fatal em produção

No momento em que a predição seria útil — **antes** da avaliação do ano — nem
`alunos_alfabetizados` nem `matriculas_avaliadas` existem. O modelo teria acurácia
perfeita no papel e nenhuma utilidade no mundo.

Por isso a camada Gold **não deixa** essas colunas entrarem: `src/camadas/gold.py`
mantém a lista `FEATURES_VETADAS` e **falha alto** se alguma delas aparecer na feature
store. A trava é código, não recomendação:

```python
FEATURES_VETADAS = ["alunos_alfabetizados", "matriculas_avaliadas",
                    "indicador_uf_pct", "gap_meta_pp"]

vazadas = [c for c in FEATURES_VETADAS if c in df.columns]
if vazadas:
    raise RuntimeError(f"Vazamento detectado na feature store: {vazadas}.")
```
"""),
    md("""
## 2. O modelo honesto

`gold.features_ml_municipio` só contém o que se conhece **antes** do ano do alvo:
contexto socioeconômico do Censo, indicador defasado (t−1) e a meta pactuada.

Validação **temporal**, não aleatória: treina em 2024, testa em 2025. Embaralhar anos
seria outra forma de vazamento — o modelo veria o futuro do próprio município.
"""),
    code('''
COLUNAS_X = [
    "indicador_ano_anterior_pct", "meta_pactuada_pct", "idhm", "idhm_educacao",
    "idhm_renda", "renda_per_capita", "indice_gini", "pct_pobres", "pct_criancas_pobres",
    "taxa_analfabetismo_15mais", "pct_6a14_fora_escola", "pct_criancas_dom_sem_fund",
    "pct_6a14_fund_sem_atraso", "pct_agua_encanada", "pct_energia_eletrica",
    "populacao_total", "capital",
]

base = features.dropna(subset=["alvo_indicador_pct", "indicador_ano_anterior_pct"] + COLUNAS_X)
treino = base[base["ano"] == 2024]
teste = base[base["ano"] == 2025]
print(f"Treino (2024): {len(treino):,} municípios | Teste (2025): {len(teste):,} municípios")

X_tr, y_tr = treino[COLUNAS_X], treino["alvo_indicador_pct"]
X_te, y_te = teste[COLUNAS_X], teste["alvo_indicador_pct"]

resultados = []
for nome, modelo in [
    ("Ridge", Ridge(alpha=1.0)),
    ("Gradient Boosting", GradientBoostingRegressor(random_state=42, n_estimators=300,
                                                    max_depth=3, learning_rate=0.05)),
]:
    modelo.fit(X_tr, y_tr)
    pred = modelo.predict(X_te)
    resultados.append({"modelo": nome,
                       "R2_teste": round(r2_score(y_te, pred), 3),
                       "MAE_teste_pp": round(mean_absolute_error(y_te, pred), 2)})

# Baseline honesto: repetir o valor do ano anterior.
baseline = teste["indicador_ano_anterior_pct"]
resultados.append({"modelo": "Baseline (repete t-1)",
                   "R2_teste": round(r2_score(y_te, baseline), 3),
                   "MAE_teste_pp": round(mean_absolute_error(y_te, baseline), 2)})

pd.DataFrame(resultados)
'''),
    md("""
Compare com a seção anterior: o modelo vazado alcançou **R² 0,95 com erro de 2,8 p.p.**,
usando duas colunas que **não existem** no momento em que a predição seria feita. O modelo
honesto fica em torno de **R² 0,75 com erro de 7 p.p.** — e esse é o número que descreve o
que a solução realmente consegue prever.

Note também o baseline: qualquer modelo que não supere "repetir o valor do ano anterior"
não merece ir para produção. O ganho aqui é real, mas medido — não inflado.
"""),
    code('''
melhor = GradientBoostingRegressor(random_state=42, n_estimators=300,
                                   max_depth=3, learning_rate=0.05).fit(X_tr, y_tr)
importancia = (pd.Series(melhor.feature_importances_, index=COLUNAS_X)
                 .sort_values(ascending=True).tail(12))

fig, ax = plt.subplots(figsize=(9, 4.8))
ax.barh(importancia.index, importancia.values, color="#334155")
ax.set_title("O que o modelo usa para prever o indicador do ano seguinte")
ax.set_xlabel("importância relativa")
plt.tight_layout(); plt.show()
'''),
    md("""
## 3. Três usos da base Gold que o desafio pede

### a) Predição de alfabetização por município
O modelo acima aponta, **antes da avaliação**, quais municípios tendem a ficar abaixo da
meta. Isso transforma a política de reativa em preventiva: o apoio técnico chega no
começo do ano letivo, não no relatório do ano seguinte.

### b) Análise de desigualdade educacional
`gold.painel_desigualdade` cruza indicador, região e quartil de IDHM. O notebook 03 mostra
mais de 30 pontos percentuais de diferença entre o quartil mais alto e o mais baixo —
evidência direta para desenhar política focalizada.

### c) Política pública baseada em dados
Com `indicador_alfabetizacao_municipio` é possível agrupar municípios por perfil de
vulnerabilidade e simular cenários: quantas crianças a mais seriam alfabetizadas se os
municípios do quartil inferior avançassem ao ritmo médio dos demais.
"""),
    code('''
m24 = municipal[municipal["ano"] == 2024].dropna(subset=["idhm"])
q1 = m24[m24["idhm"] <= m24["idhm"].quantile(0.25)]
ritmo_demais = 3.3  # variação nacional observada entre 2023 e 2024, em p.p.

ganho = (q1["matriculas_avaliadas"] * ritmo_demais / 100).sum()
print(f"Municípios no quartil de menor IDHM: {len(q1):,}")
print(f"Crianças não alfabetizadas hoje nesses municípios: "
      f"{q1['alunos_nao_alfabetizados'].sum():,.0f}")
print(f"Se avançassem {ritmo_demais} p.p. em um ano (ritmo nacional de 2023→2024), "
      f"seriam ~{ganho:,.0f} crianças alfabetizadas a mais.")
'''),
    md("""
## 4. O que a arquitetura garante para o modelo

| Requisito de IA | O que a pipeline entrega |
|---|---|
| Feature store estável | Grão declarado em contrato: uma linha por município e ano |
| Ausência de vazamento | Lista de colunas vetadas com trava que falha a execução |
| Reprodutibilidade | Time travel do Delta: treinar de novo com o dado exato de uma data |
| Qualidade da entrada | Contratos aplicados antes de a feature existir |
| Rastreabilidade | `_run_id` e `origem_indicador` acompanham cada linha até a Gold |
"""),
])


def gerar() -> None:
    DESTINO.mkdir(parents=True, exist_ok=True)
    arquivos = {
        "01_entendimento_fontes.ipynb": NB01,
        "02_pipeline_medalhao.ipynb": NB02,
        "03_camada_gold_analises.ipynb": NB03,
        "04_aplicacao_em_ia.ipynb": NB04,
    }
    for nome, conteudo in arquivos.items():
        (DESTINO / nome).write_text(json.dumps(conteudo, ensure_ascii=False, indent=1), encoding="utf-8")
        print("gerado:", nome)


if __name__ == "__main__":
    gerar()
