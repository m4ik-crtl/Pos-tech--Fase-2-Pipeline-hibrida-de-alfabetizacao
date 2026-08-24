# ADR 0002 — Onde usar batch e onde usar streaming

**Status:** aceito · **Data:** 2026-08

## Contexto

"Pipeline híbrida" é fácil de escrever no diagrama e caro de errar na conta.
Streaming custa cluster ligado 24×7; batch custa latência. A pergunta certa não
é "qual é melhor", é **qual fonte pertence a qual regime**.

## Decisão

| Fonte | Regime | Por quê |
|---|---|---|
| Malha de municípios e UFs (IBGE) | Batch anual | Muda quando um município é criado — evento raríssimo |
| Contexto socioeconômico (Censo/Atlas) | Batch decenal | O Censo é de 10 em 10 anos |
| Indicador e metas oficiais (INEP/MEC) | Batch diário | Publicação anual, mas verificação diária capta republicações e correções |
| Microdados de aluno | Batch diário | Volume alto, sem urgência de minuto |
| Atualizações de indicador municipal | **Streaming** | Secretaria corrige e espera ver o efeito no painel |
| Novas medições de prova | **Streaming** | Chegam durante toda a janela de aplicação |
| Repactuação de metas | **Streaming** | Decisão de gestão que precisa refletir de imediato |

O corte é simples: **dado de referência é batch; evento de operação é
streaming.** O que muda uma vez por ano não justifica cluster ligado; o que a
gestão corrige hoje não pode esperar até amanhã de madrugada.

## Consequências

- O job batch roda às 4h (fora do horário de consulta) em cluster efêmero, com
  autoscale de 2 a 8 nós.
- O job de streaming roda contínuo, com cluster mínimo de 2 nós e
  `maxOffsetsPerTrigger` limitando a vazão por microlote — um pico de eventos
  vira mais microlotes, não um cluster maior.
- As duas pernas gravam nas mesmas camadas com os mesmos contratos, então não há
  duas versões da verdade.
- **Custo do híbrido:** o streaming responde por cerca de 80% do custo mensal no
  cenário de produção (ver `docs/estimativa_custos.md`). É um custo consciente:
  se a atualização em minutos deixasse de ser requisito, desligar o job
  contínuo e migrar para micro-batch de hora em hora cortaria a conta em ~70%.
