# Governança e qualidade de dados

## Como a governança é aplicada por código

Nenhuma regra depende de alguém lembrar de conferir. Todas moram em
`config/contratos/{bronze,silver,gold}.yml` e são aplicadas pela mesma função
(`src/qualidade/contratos.aplicar`) em todas as camadas.

### Tipos de check disponíveis

| Tipo | Escopo | O que verifica |
|---|---|---|
| `min_count` | tabela | Volume mínimo — arquivo truncado na origem não passa |
| `unico` | tabela | Unicidade do grão declarado (`ano` + `id_municipio`, por exemplo) |
| `not_null` | linha | Chave obrigatória presente (string vazia também reprova) |
| `range` | linha | Faixa de negócio: percentual entre 0 e 100, IDHM entre 0 e 1 |
| `valores_permitidos` | linha | Domínio fechado: rede, região, status |
| `regex` | linha | Formato: sigla de UF com exatamente duas maiúsculas |
| `chave_estrangeira` | linha | Integridade referencial contra outra tabela |

Modificadores: `critico`, `tolerancia_pct`, `permite_nulo`.

### O caminho do registro reprovado

```
registro --> checks do contrato --> passou? --> tabela publicada
                                 \
                                  --> _quarentena/<camada>/<tabela>
                                      + _motivo_quarentena = "range:proficiencia_saeb"
```

A quarentena não é lixeira: é fila de investigação. O motivo fica na linha, o
volume vira métrica no relatório de monitoramento, e o Terraform expira a
quarentena em 90 dias.

## Defeitos que a pipeline trata (e que estão nos dados de propósito)

Base pública real vem suja. Para que a Silver tenha o que limpar — e para que a
limpeza seja **verificável** —, `src/ingestao/preparar_raw.py` injeta defeitos
controlados e determinísticos:

| Defeito injetado | Onde | Como é tratado |
|---|---|---|
| ~0,4% de registros duplicados | município, indicador municipal, aluno | Deduplicação pela chave de negócio, mantendo a ingestão mais recente |
| Nomes em caixa alta e com espaços extras | `nome_municipio` | Padronização com tratamento de hífen e conectivos |
| 12 municípios com latitude nula | município | Check de aviso: registra o desvio e mantém a linha |
| 40 alunos sem resultado de prova | aluno | Quarentena (nulo não é permitido em proficiência) |
| 15 alunos com proficiência `-1` | aluno | Convertida em nulo na Silver, depois quarentena |

E dois defeitos que **não** foram injetados — são reais, vindos das fontes:

1. **Roraima sem coleta divulgada em 2024.** O valor permanece nulo com a flag
   `indicador_disponivel = false`. Imputar a média nacional aqui seria fabricar
   um dado de política pública.
2. **Códigos IBGE incompatíveis.** O Atlas do Desenvolvimento Humano publica o
   código de município com 6 dígitos; a malha do IBGE usa 7. O join sem
   normalização devolve **tudo nulo, sem erro nenhum** — foi exatamente o que
   aconteceu na primeira execução deste projeto, e é por isso que a normalização
   de chave virou etapa explícita e comentada na Silver.

## Proveniência: o que é real e o que é simulado

Cada linha carrega de onde veio. `data/raw/_manifesto.json` declara a
proveniência de cada arquivo, e as colunas `origem_indicador` / `origem_meta`
carregam o rótulo até a camada Gold.

| Arquivo | Proveniência |
|---|---|
| `uf.csv`, `municipio.csv` | **REAL** — malha territorial do IBGE |
| `contexto_socioeconomico_municipio.csv` | **REAL** — Atlas do Desenvolvimento Humano (PNUD/Ipea/FJP), Censo 2010 |
| `meta_alfabetizacao_brasil.csv` | **REAL** (INEP/MEC) + interpolação declarada da trajetória oficial 2026→2030 |
| `meta_alfabetizacao_uf.csv` | Indicador **REAL** (INEP/MEC); metas **derivadas** pela regra de trajetória do Compromisso Nacional |
| `meta_alfabetizacao_municipio.csv` | **SIMULADO CALIBRADO** |
| `aluno.csv` | **SIMULADO CALIBRADO** e pseudonimizado |

### Sobre a simulação calibrada

O INEP divulga o resultado municipal em consulta interativa, sem arquivo aberto.
O grão municipal foi então **simulado — mas não sorteado**:

1. cada município recebe um escore latente calculado a partir das suas variáveis
   socioeconômicas **reais** (IDHM educação, % de crianças pobres, taxa de
   analfabetismo, % no fundamental sem atraso);
2. o escore é convertido em percentual e reescalado iterativamente até que a
   **média ponderada por matrículas de cada UF reproduza o valor real publicado
   pelo INEP** para aquela UF;
3. os microdados de aluno são amostrados de uma normal centrada de modo que
   `P(proficiência ≥ 743)` reproduza o indicador do município.

O resultado é verificável: a coluna `divergencia_pp` da tabela
`gold.meta_vs_realizado_uf` mostra **0,0 p.p.** de diferença entre o agregado dos
municípios e o valor publicado pelo INEP em 2024. E o sinal socioeconômico
sobrevive ao agregado — a correlação entre IDHM educação e o indicador municipal
fica em torno de 0,59, na ordem de grandeza do que a literatura observa.

Trocar a simulação pelo arquivo oficial, quando publicado, é substituir o CSV em
`data/externo/` — a pipeline não muda.

## Privacidade

- Não há identificação de estudante: `id_aluno` é um SHA-256 truncado, não
  reversível, gerado na origem.
- A camada Gold publica apenas agregados municipais.
- No Azure, o acesso ao lake usa identidade gerenciada; não há credencial em
  código nem no repositório.
