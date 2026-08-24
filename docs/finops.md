# FinOps — como a arquitetura foi otimizada

Em nuvem, arquitetura é orçamento. Cada decisão abaixo tem um efeito mensurável
na fatura; a estimativa numérica, calculada a partir do que a execução local
realmente produziu, está em [`estimativa_custos.md`](estimativa_custos.md) e é
regenerada com `make custos`.

## As sete decisões que reduzem custo

### 1. Jobs Compute em vez de All-Purpose

Cluster interativo (All-Purpose) custa cerca de **3,7x mais por DBU** que
cluster de job. Toda carga agendada — batch e streaming — roda em Jobs Compute
efêmero, criado no início da execução e destruído no fim. All-Purpose fica
reservado para exploração em notebook.

### 2. Cluster efêmero com auto-terminate

Entre a execução das 4h e a do dia seguinte, **não há cluster ligado**. O custo
de computação do batch é proporcional ao tempo de execução, não ao calendário.

### 3. Instâncias Spot nos executores do batch

`SPOT_WITH_FALLBACK_AZURE` com `first_on_demand: 1`: o driver fica em instância
sob demanda (estabilidade) e os executores usam capacidade ociosa do Azure, com
desconto que chega a 60%. Se a capacidade Spot sumir, o cluster cai
automaticamente para sob demanda em vez de falhar.

### 4. Parquet/Delta com Snappy, particionado por ano e Z-ORDER por UF

O painel filtra por ano e UF. Com partição por ano e Z-ORDER por
`(sigla_uf, id_municipio)`, uma consulta típica lê uma fração dos arquivos em
vez da tabela inteira — e o custo de consulta é proporcional a bytes lidos.

O que foi **evitado** conta tanto quanto: particionar também por município
criaria 16 mil diretórios de poucos KB. O *small files problem* faz a listagem
custar mais que a leitura.

### 5. `OPTIMIZE` diário + `VACUUM`

O streaming grava arquivos pequenos a cada microlote. Sem compactação, a Gold
degrada em dias. O `OPTIMIZE` roda ao fim do job batch e o `VACUUM RETAIN 168
HOURS` devolve o armazenamento das versões antigas, mantendo 7 dias de time
travel para auditoria.

### 6. Controle de vazão no streaming

`maxOffsetsPerTrigger = 5000` limita quantos eventos entram por microlote. Um
pico de eventos vira **mais microlotes**, não um cluster maior. Sem isso, o
autoscale reagiria ao pico escalando o cluster — e a fatura seguiria o pico.

### 7. Ciclo de vida do armazenamento

Definido no Terraform: bronze vai para Cool aos 30 dias, Archive aos 180 e é
removido aos 7 anos; a quarentena expira em 90 dias. O histórico continua
auditável a uma fração do custo de armazenamento quente.

## Governança de custo

- **Tags de centro de custo** em todos os recursos e clusters — sem tag, não há
  rateio possível no Azure Cost Management.
- **Orçamento com alerta** em 80% e 100% do teto mensal
  (`azurerm_consumption_budget_resource_group`).
- **Estimativa versionada no repositório**, derivada de medições reais
  (`src/finops/estimativa_custos.py` lê a observabilidade da última execução),
  em vez de um número escrito à mão que ninguém consegue refazer.

## Onde o dinheiro vai

No cenário de produção nacional, o **streaming responde por cerca de 80% do
custo mensal** — cluster contínuo é o item mais caro de qualquer arquitetura
híbrida. Isso é uma escolha, não um acidente: se atualização em minutos deixasse
de ser requisito, trocar o job contínuo por micro-batch de hora em hora cortaria
aproximadamente 70% da conta.

O armazenamento, em contraste, é irrisório: o dado do país inteiro cabe em
poucos GB. Otimizar armazenamento aqui seria economizar centavos enquanto se
gasta em cluster — por isso o esforço de otimização está concentrado em
**computação**, que é onde o custo realmente está.
