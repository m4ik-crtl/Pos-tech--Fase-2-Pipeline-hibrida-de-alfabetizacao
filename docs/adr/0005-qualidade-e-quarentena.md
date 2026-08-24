# ADR 0005 — Contratos declarativos, quarentena e tolerância

**Status:** aceito · **Data:** 2026-08

## Contexto

Regra de qualidade escrita dentro da transformação tem três problemas: some no
meio do código, é difícil de auditar e cada engenheiro implementa do seu jeito.
E há uma decisão de produto embutida em toda regra: **o que fazer com o registro
reprovado?**

Três posturas possíveis:

1. **Descartar em silêncio** — a pipeline "funciona" e o número sai errado.
2. **Derrubar o job em qualquer violação** — meia dúzia de alunos sem resultado
   de prova para a apuração de um país inteiro.
3. **Isolar o registro, medir o desvio e derrubar só acima de um limite.**

## Decisão

Contratos **declarativos em YAML** (`config/contratos/<camada>.yml`), aplicados
pela mesma função em todas as camadas (`src/qualidade/contratos.py`), com três
propriedades por check:

- `critico: true|false` — crítico isola o registro na quarentena; aviso apenas
  registra o desvio no relatório e deixa o dado seguir;
- `tolerancia_pct` — fração de registros que pode ser reprovada sem derrubar a
  execução (postura 3);
- `permite_nulo` — se ausência é aceitável naquele campo.

Todo registro reprovado vai para `_quarentena/<camada>/<tabela>` com a coluna
`_motivo_quarentena`, listando as regras violadas. Nada é descartado em
silêncio.

### O detalhe que quase passou

Predicado com valor nulo retorna `NULL`, não `false`. Numa implementação
ingênua, `filter(cond)` e `filter(~cond)` **ambos** excluem a linha nula — e o
registro desaparece dos dois lados sem erro nenhum. Por isso todo predicado é
envolvido em `coalesce(cond, false)` (`src/qualidade/contratos._com_nulo`), e há
um teste dedicado a isso: `validos + quarentena == entrada`, sempre.

## Consequências

- A regra fica legível para quem não programa: o gestor de dados consegue ler o
  YAML e discordar de um limite.
- O score de qualidade por tabela entra na observabilidade e vira série
  histórica — dá para ver a origem degradando antes de virar incidente.
- **Custo:** cada check é uma passada a mais sobre o DataFrame. Em tabela grande,
  os checks somam tempo de cluster. Aceito conscientemente: é mais barato que
  publicar número errado em painel de política pública.
