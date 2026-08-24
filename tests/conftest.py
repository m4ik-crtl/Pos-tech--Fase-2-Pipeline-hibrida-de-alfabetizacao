"""Fixtures compartilhadas. A sessão Spark é criada uma vez para toda a suíte."""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("FORMATO_TABELA", "parquet")
os.environ.setdefault("SHUFFLE_PARTITIONS", "2")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("testes-alfabetizacao")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()
