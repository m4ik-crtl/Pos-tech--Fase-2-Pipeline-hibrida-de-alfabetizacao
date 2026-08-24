"""
Fontes oficiais — valores reais publicados, com proveniência explícita.

Este módulo é a "fonte da verdade" do que é **dado real publicado** e do que é
**derivado por regra** ou **simulado**. Nada aqui é invenção silenciosa: cada
número carrega a coluna `fonte`/`origem_valor`, e o mesmo rótulo atravessa
bronze → silver → gold, chegando ao consumidor final.

Referências (acessadas em ago/2026):
  - INEP/MEC — Indicador Criança Alfabetizada, resultado 2024 por UF
    https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/avaliacao-da-alfabetizacao/resultados
  - Todos Pela Educação — Notas técnicas ICA 2023, 2024 e 2025
  - Compromisso Nacional Criança Alfabetizada — trajetória de metas nacionais
  - IBGE — malha de municípios e UFs
  - Atlas do Desenvolvimento Humano no Brasil (PNUD/Ipea/FJP), Censo 2010
"""

from __future__ import annotations

# --------------------------------------------------------------------------- #
# 1) Série nacional — resultado publicado do Indicador Criança Alfabetizada
#    (% de estudantes do 2º ano da rede pública com proficiência >= 743 no Saeb)
# --------------------------------------------------------------------------- #
INDICADOR_BRASIL: dict[int, float] = {
    2023: 55.9,
    2024: 59.2,
    2025: 66.0,
}

# --------------------------------------------------------------------------- #
# 2) Metas nacionais do Compromisso Nacional Criança Alfabetizada
#    2024/2025/2026 e 2030 são metas divulgadas; 2027-2029 são interpolação
#    linear da trajetória oficial 2026 -> 2030 (marcado como INTERPOLADO).
# --------------------------------------------------------------------------- #
META_BRASIL_PUBLICADA: dict[int, float] = {
    2024: 59.9,
    2025: 64.0,
    2026: 67.0,
    2030: 80.0,
}

# --------------------------------------------------------------------------- #
# 3) Resultado por UF. `None` = não publicado / não coletado.
#    Roraima não teve coleta divulgada em 2024 — o valor ausente é mantido de
#    propósito, para exercitar o tratamento de nulos na camada Silver.
# --------------------------------------------------------------------------- #
INDICADOR_UF: dict[str, dict[int, float | None]] = {
    "AC": {2023: None, 2024: 51.4, 2025: None},
    "AL": {2023: None, 2024: 48.6, 2025: None},
    "AM": {2023: 52.0, 2024: 49.2, 2025: None},
    "AP": {2023: None, 2024: 46.6, 2025: None},
    "BA": {2023: 37.0, 2024: 36.0, 2025: None},
    "CE": {2023: 85.0, 2024: 85.3, 2025: 84.0},
    "DF": {2023: None, 2024: 59.1, 2025: None},
    "ES": {2023: 68.0, 2024: 71.7, 2025: None},
    "GO": {2023: None, 2024: 72.7, 2025: 80.0},
    "MA": {2023: 56.0, 2024: 59.6, 2025: None},
    "MG": {2023: None, 2024: 72.1, 2025: None},
    "MS": {2023: None, 2024: 55.9, 2025: None},
    "MT": {2023: None, 2024: 60.6, 2025: None},
    "PA": {2023: None, 2024: 48.2, 2025: None},
    "PB": {2023: None, 2024: 56.0, 2025: None},
    "PE": {2023: None, 2024: 60.8, 2025: None},
    "PI": {2023: 52.0, 2024: 59.8, 2025: None},
    "PR": {2023: 73.0, 2024: 70.4, 2025: 80.0},
    "RJ": {2023: 52.0, 2024: 55.3, 2025: None},
    "RN": {2023: 37.0, 2024: 39.3, 2025: None},
    "RO": {2023: None, 2024: 62.6, 2025: None},
    "RR": {2023: None, 2024: None, 2025: None},  # sem coleta divulgada
    "RS": {2023: 63.4, 2024: 44.7, 2025: None},
    "SC": {2023: None, 2024: 62.0, 2025: 59.0},
    "SE": {2023: 31.0, 2024: 38.4, 2025: None},
    "SP": {2023: 52.0, 2024: 58.1, 2025: None},
    "TO": {2023: None, 2024: 50.1, 2025: None},
}

# Rótulo de proveniência usado em todo o pipeline.
PUBLICADO = "INEP_PUBLICADO"
INTERPOLADO = "INTERPOLADO_TRAJETORIA_OFICIAL"
DERIVADO = "DERIVADO_REGRA_TRAJETORIA"
SIMULADO = "SIMULADO_CALIBRADO"
NAO_COLETADO = "NAO_COLETADO"

META_FINAL_2030 = 80.0


def meta_brasil(ano: int) -> tuple[float, str]:
    """Meta nacional do ano + proveniência."""
    if ano in META_BRASIL_PUBLICADA:
        return META_BRASIL_PUBLICADA[ano], PUBLICADO
    # interpolação linear entre 2026 (67,0) e 2030 (80,0)
    inicio, fim = 2026, 2030
    v_ini, v_fim = META_BRASIL_PUBLICADA[inicio], META_BRASIL_PUBLICADA[fim]
    passo = (v_fim - v_ini) / (fim - inicio)
    return round(v_ini + passo * (ano - inicio), 1), INTERPOLADO


ANO_BASE_TRAJETORIA = 2023


def linha_de_base_uf(sigla_uf: str) -> float:
    """
    Ponto de partida da trajetória de metas de uma UF (resultado de 2023).

    Ordem de preferência: valor publicado de 2023 -> valor de 2024 descontada a
    variação nacional entre 2023 e 2024 -> valor nacional de 2023. A meta nunca
    é ancorada no próprio resultado do ano avaliado: se fosse, meta e realizado
    coincidiriam e a comparação perderia o sentido.
    """
    valores = INDICADOR_UF.get(sigla_uf, {})
    if valores.get(ANO_BASE_TRAJETORIA) is not None:
        return float(valores[ANO_BASE_TRAJETORIA])
    if valores.get(2024) is not None:
        delta_nacional = INDICADOR_BRASIL[2024] - INDICADOR_BRASIL[ANO_BASE_TRAJETORIA]
        return float(valores[2024]) - delta_nacional
    return INDICADOR_BRASIL[ANO_BASE_TRAJETORIA]


def meta_uf(sigla_uf: str, ano: int) -> tuple[float, str]:
    """
    Meta anual por UF.

    O MEC pactua metas por UF e município, mas a planilha pactuada não é
    publicada em formato aberto. Aplicamos a regra de trajetória do próprio
    Compromisso: partir da linha de base de 2023 e convergir linearmente para
    80% em 2030 — o que preserva a lógica "quem está mais longe sobe mais".
    """
    base = linha_de_base_uf(sigla_uf)
    anos_restantes = 2030 - ANO_BASE_TRAJETORIA
    passo = (META_FINAL_2030 - base) / anos_restantes
    valor = base + passo * (ano - ANO_BASE_TRAJETORIA)
    return round(min(max(valor, 0.0), 100.0), 1), DERIVADO
