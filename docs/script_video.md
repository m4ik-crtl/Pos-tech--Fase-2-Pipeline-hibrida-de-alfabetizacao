# Roteiro do vídeo executivo (até 5 minutos)

**Público:** liderança e stakeholders — secretaria de educação, diretoria, patrocinador do
projeto. Linguagem executiva: problema, solução, valor. Detalhe técnico só quando sustenta
uma afirmação de negócio.

**Formato sugerido:** tela compartilhada com o README aberto, alternando para o diagrama,
para o notebook 03 (gráficos) e para o terminal rodando a pipeline.

---

## 0:00 – 0:40 · O problema (abrir com o número, não com a tecnologia)

> "Em 2025, 66% das crianças brasileiras terminaram o 2º ano do fundamental alfabetizadas.
> É a melhor marca da série e superou a meta pactuada, que era 64%.
>
> Mas essa média esconde o que interessa. No Ceará, o índice é de 85%. Na Bahia, 36%.
> Quase 50 pontos de diferença dentro do mesmo país, no mesmo ano.
>
> A pergunta que a gestão precisa responder não é 'como está o Brasil'. É: **onde estão as
> crianças que não estão sendo alfabetizadas, e o que essas cidades têm em comum**."

**Tela:** gráfico do indicador por UF (notebook 01).

---

## 0:40 – 1:20 · Por que isso é um problema de engenharia de dados

> "Responder isso exige cruzar coisas que hoje vivem separadas: as metas nacionais,
> estaduais e municipais; a malha territorial do IBGE; os microdados de avaliação; e o
> contexto socioeconômico de cada município.
>
> São fontes com donos diferentes, formatos diferentes e ritmos diferentes. Algumas mudam
> uma vez por ano. Outras mudam o tempo todo, porque secretaria municipal corrige dado e
> espera ver o efeito no painel.
>
> Foi para isso que construímos esta pipeline: **uma base analítica confiável sobre
> alfabetização, atualizada em dois ritmos**."

---

## 1:20 – 2:30 · A arquitetura, em linguagem de negócio

**Tela:** `docs/diagrama_pipeline.png`.

> "A arquitetura é híbrida e segue o padrão medalhão, em três camadas.
>
> **Bronze** guarda o dado exatamente como chegou, com o registro de origem e horário.
> É o que permite auditar, seis meses depois, um número que foi divulgado.
>
> **Silver** é onde o dado vira confiável: limpeza, padronização e integração das bases.
> Um exemplo real deste projeto: o Atlas do Desenvolvimento Humano usa código de município
> com 6 dígitos e o IBGE usa 7. Sem tratar isso, o cruzamento devolve vazio — sem dar erro.
> Foi exatamente o que aconteceu na nossa primeira execução, e é o tipo de defeito que
> passa despercebido e contamina uma decisão.
>
> **Gold** é o que a gestão consome: indicador por município, meta contra realizado por
> estado, evolução nacional, painel de desigualdade, ranking.
>
> As duas pernas — a carga diária e o fluxo contínuo de eventos — escrevem nas mesmas
> camadas, com as mesmas regras de qualidade. Não existem duas versões da verdade."

---

## 2:30 – 3:20 · O valor: o que a base responde

**Tela:** notebook 03 — gráfico do indicador por quartil de IDHM.

> "Este é o gráfico que justifica o projeto inteiro.
>
> Nos municípios do quartil de **menor** desenvolvimento humano, o indicador médio é de
> **30,6%**. No quartil de maior, **66,2%**. Mais de 30 pontos percentuais associados ao
> território onde a criança nasceu.
>
> Com a base pronta, isso deixa de ser uma intuição e vira uma lista: são **1.399
> municípios**, com **259 mil crianças** não alfabetizadas. E dá para simular o efeito de
> uma política: se esses municípios avançassem no ritmo nacional do último ano, seriam
> **13 mil crianças alfabetizadas a mais em um ano**.
>
> É esse tipo de resposta que muda a conversa de 'como estamos' para 'onde agir primeiro'."

---

## 3:20 – 4:10 · Confiança: qualidade, monitoramento e custo

> "Três coisas garantem que esse número seja confiável.
>
> **Primeiro, qualidade por contrato.** Cada camada tem regras declaradas em arquivo:
> unicidade, integridade referencial, faixas válidas. Registro que não passa não é
> descartado — vai para quarentena, com o motivo registrado. E quando falta dado, a
> pipeline **não inventa**: Roraima não teve coleta divulgada em 2024, e o campo continua
> vazio, sinalizado. Preencher com a média nacional seria fabricar um número de política
> pública.
>
> **Segundo, monitoramento.** Cada execução registra falhas, latência, volume e um score de
> qualidade por tabela. Job que passa de uma hora ou fila de eventos que atrasa mais de
> cinco minutos disparam alerta.
>
> **Terceiro, custo sob controle.** A arquitetura roda por volta de **450 dólares mensais**
> em cenário nacional, com cluster efêmero, instâncias spot e particionamento pensado para
> reduzir leitura. E sabemos onde o dinheiro está: 80% é o fluxo contínuo. Se a atualização
> em minutos deixar de ser necessária, cortamos cerca de 70% da conta."

---

## 4:10 – 4:50 · Inteligência artificial: o que dá e o que não dá

> "A camada Gold já está preparada para modelos preditivos — e aqui vale a parte mais
> importante.
>
> É tentador treinar um modelo com as colunas de alunos avaliados e alunos alfabetizados.
> Ele acerta quase tudo: R² de 0,95. E **não serve para nada**, porque o indicador é
> literalmente a divisão dessas duas colunas — e, na hora em que a previsão seria útil,
> nenhuma delas existe ainda.
>
> Por isso a pipeline **bloqueia essas variáveis por código**: se alguém tentar incluí-las,
> a execução falha. O modelo honesto, usando só o que se sabe antes do ano, chega a R² de
> 0,75, com erro médio de 7 pontos — e supera o baseline de repetir o ano anterior.
>
> É um número mais modesto e infinitamente mais útil: ele antecipa quais municípios tendem
> a ficar abaixo da meta, **antes da avaliação**, para o apoio técnico chegar no começo do
> ano letivo em vez de aparecer no relatório do ano seguinte."

---

## 4:50 – 5:00 · Fechamento

> "Em resumo: uma base confiável, atualizada em dois ritmos, auditável, com custo conhecido,
> que transforma o Indicador Criança Alfabetizada de um número divulgado por ano em um
> instrumento de decisão. O repositório roda com um comando, e os dados estão versionados
> junto — qualquer pessoa reproduz o que mostramos aqui."

---

## Checklist antes de gravar

- [ ] `python -m src.pipeline --reprocessar` rodou sem erro e o relatório está atualizado
- [ ] Notebooks abertos nas células dos gráficos (01 e 03)
- [ ] `docs/diagrama_pipeline.png` aberto em uma aba
- [ ] Números conferidos: 66,0% (2025) · 85,3% CE × 36,0% BA (2024) · 30,6% × 66,2% (quartis de IDHM)
- [ ] Áudio testado; duração total abaixo de 5 minutos
- [ ] Falar em ritmo executivo: uma ideia por bloco, sem jargão desnecessário
