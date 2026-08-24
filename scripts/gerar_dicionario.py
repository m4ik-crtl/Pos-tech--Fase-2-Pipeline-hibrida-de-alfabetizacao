"""
Gera `docs/dicionario_dados.md` a partir do schema real do lakehouse.

Documentação escrita à mão envelhece; esta é derivada das tabelas que a última
execução produziu, então nunca descreve uma coluna que não existe.

Uso: python scripts/gerar_dicionario.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import spark_session  # noqa: E402
from src.config import CFG  # noqa: E402
from src.logging_conf import configurar, log  # noqa: E402

LOG = log(__name__)

DESCRICOES = {
    # chaves e território
    "id_municipio": "Código IBGE do município (7 dígitos) — chave territorial",
    "id_municipio_6dig": "Código IBGE de 6 dígitos, formato do Atlas do Desenvolvimento Humano",
    "id_uf": "Código IBGE da UF (2 dígitos)",
    "sigla_uf": "Sigla da Unidade da Federação",
    "nome_uf": "Nome da Unidade da Federação",
    "nome_municipio": "Nome do município padronizado",
    "regiao": "Grande região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul)",
    "capital": "1 se o município é capital estadual",
    "ano": "Ano de referência do indicador",
    # indicador
    "indicador_pct": "% de estudantes do 2º ano com proficiência >= 743 no Saeb",
    "indicador_publicado_pct": "Valor publicado pelo INEP para a UF/Brasil",
    "indicador_calculado_pct": "Valor recalculado a partir do grão municipal",
    "divergencia_pp": "Diferença (p.p.) entre calculado e publicado — reconciliação",
    "meta_pct": "Meta pactuada para o ano",
    "meta_pactuada_pct": "Meta pactuada para o ano (feature conhecida ex-ante)",
    "gap_meta_pp": "Indicador menos meta, em pontos percentuais",
    "status_meta": "ACIMA_DA_META | NA_META | ABAIXO_DA_META",
    "faixa_indicador": "CRITICO (<40) | ATENCAO (<60) | ADEQUADO (<80) | AVANCADO",
    "matriculas_avaliadas": "Estudantes avaliados no 2º ano da rede pública",
    "alunos_alfabetizados": "Estudantes que atingiram o ponto de corte",
    "alunos_nao_alfabetizados": "Estudantes abaixo do ponto de corte",
    "ranking_uf": "Posição do município dentro da sua UF no ano",
    "ranking_brasil": "Posição do município no Brasil no ano",
    "ponto_corte_saeb": "Ponto de corte de alfabetização na escala Saeb (743)",
    "id_aluno": "Identificador pseudonimizado do estudante (SHA-256 truncado, não reversível)",
    "rede": "Rede de ensino: MUNICIPAL | ESTADUAL | FEDERAL | PRIVADA",
    "localizacao": "Localização da escola: URBANA | RURAL",
    "sexo": "Sexo declarado do estudante",
    "idade": "Idade do estudante no momento da avaliação",
    "ano_referencia": "Ano de referência da fonte externa",
    "eventos": "Quantidade de eventos na janela",
    "medicoes": "Eventos do tipo NOVA_MEDICAO na janela",
    "municipios_distintos": "Municípios distintos na janela (contagem aproximada)",
    "indicador_medio_pct": "Indicador médio dos eventos da janela",
    "pct_alfabetizados_janela": "% de medições acima do ponto de corte na janela",
    "municipios_avaliados": "Municípios com resultado no ano",
    "municipios_abaixo_da_meta": "Municípios que não atingiram a meta pactuada",
    "municipios_cobertos": "Municípios cobertos pela apuração",
    "idhm_medio": "IDHM médio do agrupamento",
    "pct_criancas_pobres_medio": "% médio de crianças pobres no agrupamento",
    "indicador_mediano_pct": "Mediana do indicador no agrupamento",
    "variacao_pp": "Variação do indicador em relação ao ano anterior (p.p.)",
    "distancia_meta_2030_pp": "Distância até a meta de 80% em 2030 (p.p.)",
    "grupo": "MELHORES ou PIORES no ranking do ano",
    "municipios": "Quantidade de municípios no agrupamento",
    "alunos_avaliados": "Estudantes avaliados no evento",
    "origem": "Origem do evento no streaming",
    "latitude": "Latitude do centroide",
    "longitude": "Longitude do centroide",
    "ddd": "Código DDD do município",
    "proficiencia_saeb": "Proficiência do estudante na escala Saeb",
    "alfabetizado": "1 se proficiência >= ponto de corte",
    "alvo_indicador_pct": "ALVO do modelo preditivo: indicador do município no ano t",
    "indicador_ano_anterior_pct": "Indicador do município em t-1 (feature sem vazamento)",
    "variacao_ano_anterior_pp": "Variação observada entre t-2 e t-1",
    "matriculas_ano_anterior": "Matrículas avaliadas em t-1",
    # contexto socioeconômico
    "idhm": "IDHM municipal (Atlas/PNUD, Censo 2010)",
    "idhm_educacao": "Subíndice de educação do IDHM",
    "idhm_renda": "Subíndice de renda do IDHM",
    "idhm_longevidade": "Subíndice de longevidade do IDHM",
    "renda_per_capita": "Renda domiciliar per capita (R$, Censo 2010)",
    "indice_gini": "Índice de Gini da renda domiciliar",
    "pct_pobres": "% da população pobre",
    "pct_criancas_pobres": "% de crianças em situação de pobreza",
    "taxa_analfabetismo_15mais": "Taxa de analfabetismo a partir dos 15 anos",
    "expectativa_anos_estudo": "Expectativa de anos de estudo",
    "pct_6a14_na_escola": "% de crianças de 6 a 14 anos na escola",
    "pct_6a14_fora_escola": "% de crianças de 6 a 14 anos fora da escola",
    "pct_6a14_fund_sem_atraso": "% de 6 a 14 anos no fundamental sem atraso escolar",
    "pct_criancas_dom_sem_fund": "% de crianças em domicílio sem ninguém com fundamental completo",
    "pct_agua_encanada": "% da população em domicílio com água encanada",
    "pct_energia_eletrica": "% da população em domicílio com energia elétrica",
    "pct_coleta_lixo": "% da população em domicílio com coleta de lixo",
    "populacao_total": "População residente (Censo 2010)",
    "populacao_urbana": "População urbana (Censo 2010)",
    "populacao_6_anos": "População de 6 anos (Censo 2010)",
    "populacao_6a10_anos": "População de 6 a 10 anos (Censo 2010)",
    "porte_municipio": "PEQUENO (<20 mil) | MEDIO (<100 mil) | GRANDE",
    "contexto_disponivel": "Há contexto socioeconômico para este município?",
    "quartil_idhm": "Quartil de IDHM no ano (1 = mais baixo)",
    "faixa_idhm": "Rótulo legível do quartil de IDHM",
    # proveniência e técnica
    "origem_indicador": "Proveniência do indicador (publicado, simulado, não coletado)",
    "origem_meta": "Proveniência da meta (publicada, interpolada, derivada por regra)",
    "indicador_disponivel": "Houve coleta divulgada para esta UF/ano?",
    "fonte": "Sistema de origem do registro",
    "fonte_territorial": "Fonte da malha territorial (IBGE)",
    "fonte_contexto": "Fonte do contexto socioeconômico (Atlas/PNUD)",
    # streaming
    "evento_id": "Identificador único do evento (chave de deduplicação)",
    "tipo_evento": "ATUALIZACAO_INDICADOR | NOVA_MEDICAO | ATUALIZACAO_META",
    "emitido_em": "Instante em que o evento nasceu na origem (event time)",
    "latencia_s": "Segundos entre a emissão e o processamento",
    "janela_inicio": "Início da janela de agregação do streaming",
    "janela_fim": "Fim da janela de agregação do streaming",
    "latencia_media_s": "Latência média dos eventos da janela",
    "latencia_max_s": "Maior latência observada na janela",
}

TECNICAS = {
    "_ingestion_timestamp": "Momento da ingestão (UTC)",
    "_ingestion_date": "Data da ingestão — coluna de partição",
    "_source_file": "Arquivo de origem",
    "_source_system": "Sistema de origem",
    "_source_entity": "Entidade de origem",
    "_record_hash": "SHA-256 do conteúdo de negócio — detecta mudança de registro",
    "_run_id": "Identificador da execução que gravou a linha",
    "_silver_processed_at": "Momento do processamento na Silver",
    "_gold_processed_at": "Momento do processamento na Gold",
    "_fonte_stream": "kafka ou arquivo",
    "_motivo_quarentena": "Regras de contrato violadas pelo registro",
}


def _tabelas(camada: str) -> list[str]:
    base = CFG.dir_lakehouse
    if isinstance(base, str):
        return []
    caminho = base / camada
    if not caminho.exists():
        return []
    return sorted(p.name for p in caminho.iterdir() if p.is_dir())


def gerar() -> Path:
    spark = spark_session.criar("parquet")
    linhas = [
        "# Dicionário de dados",
        "",
        "Gerado automaticamente a partir do schema real do lakehouse "
        "(`python scripts/gerar_dicionario.py`). Colunas iniciadas por `_` são "
        "metadados técnicos de linhagem, presentes em todas as camadas.",
        "",
    ]

    for camada in ("bronze", "silver", "gold"):
        tabelas = _tabelas(camada)
        if not tabelas:
            continue
        linhas += [f"## Camada {camada.capitalize()}", ""]
        for tabela in tabelas:
            caminho = CFG.camada(f"{camada}/{tabela}")
            try:
                df = spark_session.ler_tabela(spark, caminho)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("[DOC] %s.%s ignorada: %s", camada, tabela, exc)
                continue
            contrato = CFG.contratos(camada).get(tabela, {})
            descricao = contrato.get("descricao", "")
            linhas += [
                f"### `{camada}.{tabela}`",
                "",
                f"{descricao}" if descricao else "",
                "",
                f"- **Registros:** {df.count():,}".replace(",", "."),
                f"- **Colunas:** {len(df.columns)}",
                "",
                "| Coluna | Tipo | Descrição |",
                "|---|---|---|",
            ]
            for nome, tipo in df.dtypes:
                desc = DESCRICOES.get(nome) or TECNICAS.get(nome) or ""
                linhas.append(f"| `{nome}` | {tipo} | {desc} |")
            linhas.append("")

    destino = CFG.raiz / "docs" / "dicionario_dados.md"
    destino.write_text("\n".join(linhas) + "\n", encoding="utf-8")
    spark.stop()
    LOG.info("[DOC] Dicionário gerado em docs/dicionario_dados.md")
    return destino


if __name__ == "__main__":
    configurar()
    gerar()
