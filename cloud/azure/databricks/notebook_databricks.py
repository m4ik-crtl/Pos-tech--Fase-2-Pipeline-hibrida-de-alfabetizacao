# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Alfabetização — execução no Databricks
# MAGIC
# MAGIC Este notebook roda **o mesmo código** do repositório, sem alteração. O que muda
# MAGIC é apenas onde o lakehouse é gravado e o fato de o Delta já vir no runtime.
# MAGIC
# MAGIC **Antes de rodar:** importe o repositório em *Workspace → Repos → Add Repo*
# MAGIC (ou *Git folders*) e abra este notebook de dentro da pasta clonada.

# COMMAND ----------

# MAGIC %pip install -q pyyaml
# MAGIC %restart_python

# COMMAND ----------

import os
import sys
from pathlib import Path

# A raiz do projeto é a pasta do repositório clonado — dois níveis acima daqui.
RAIZ = Path.cwd()
while RAIZ != RAIZ.parent and not (RAIZ / "src" / "pipeline.py").exists():
    RAIZ = RAIZ.parent
sys.path.insert(0, str(RAIZ))
os.chdir(RAIZ)
print("Raiz do projeto:", RAIZ)

# O repositório clonado é somente leitura para escrita volumosa: o lakehouse vai
# para um Volume do Unity Catalog (ou /tmp, se você ainda não criou um Volume).
os.environ["LAKEHOUSE_URI"] = "/tmp/alfabetizacao/lakehouse"
os.environ["FORMATO_TABELA"] = "delta"   # no Databricks o Delta é nativo
os.environ["LOG_FORMATO"] = "json"       # linhas viram campos no Log Analytics

# COMMAND ----------

from src.logging_conf import configurar
from src.pipeline import executar

configurar("INFO")
executar(["raw", "bronze", "silver", "gold"], reprocessar=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferindo a camada Gold

# COMMAND ----------

from src import spark_session
from src.config import CFG

spark = spark_session.criar()

municipal = spark_session.ler_tabela(spark, CFG.camada("gold/indicador_alfabetizacao_municipio"))
print(f"{municipal.count():,} linhas na visão municipal")
display(
    municipal.filter("ano = 2024")
    .select("nome_municipio", "sigla_uf", "indicador_pct", "meta_pct",
            "gap_meta_pp", "status_meta", "idhm")
    .orderBy("indicador_pct", ascending=False)
    .limit(20)
)

# COMMAND ----------

uf = spark_session.ler_tabela(spark, CFG.camada("gold/meta_vs_realizado_uf"))
display(
    uf.filter("ano = 2024")
    .select("sigla_uf", "indicador_publicado_pct", "indicador_calculado_pct",
            "divergencia_pp", "meta_pct", "status_meta")
    .orderBy("gap_meta_pp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registrando as tabelas no Unity Catalog (opcional)
# MAGIC
# MAGIC Com as tabelas registradas, a camada Gold fica consultável por SQL e pelo
# MAGIC Databricks SQL — e é aí que o `OPTIMIZE`/Z-ORDER de
# MAGIC `cloud/azure/databricks/otimizacao.sql` passa a fazer sentido.

# COMMAND ----------

CATALOGO = "workspace"     # ajuste para o seu catálogo
SCHEMA = "alfabetizacao"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")

for tabela in [
    "indicador_alfabetizacao_municipio",
    "meta_vs_realizado_uf",
    "evolucao_temporal_brasil",
    "painel_desigualdade",
    "ranking_municipios",
    "features_ml_municipio",
]:
    caminho = CFG.camada(f"gold/{tabela}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA}.{tabela} "
        f"USING DELTA LOCATION '{caminho}'"
    )
    print(f"registrada: {CATALOGO}.{SCHEMA}.{tabela}")
