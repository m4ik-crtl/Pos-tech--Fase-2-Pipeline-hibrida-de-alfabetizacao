# Guia de execução

Passo a passo para rodar o projeto no Windows e (opcionalmente) executar no Databricks.
Se você só quer ver a pipeline funcionando, vá direto para a **Opção A**.

---

## 1. Rodar na sua máquina

### Opção A — Docker Desktop (recomendada)

É a forma mais previsível no Windows: tudo roda dentro de um contêiner Linux, sem
precisar instalar Java nem lidar com o `winutils` que o Spark exige no Windows nativo.

**Pré-requisito:** [Docker Desktop](https://www.docker.com/products/docker-desktop/)
instalado e aberto (o ícone da baleia precisa estar rodando).

Clone o repositório e entre na pasta:

```powershell
git clone https://github.com/m4ik-crtl/Fiap-tech-2.git
cd Fiap-tech-2

# 1) Pipeline batch completa: raw -> bronze -> silver -> gold
docker compose run --rm pipeline
```

A primeira execução baixa a imagem base e instala as dependências — **de 5 a 10
minutos**. As seguintes levam cerca de 2 minutos. Ao final você deve ver:

```
[OBS] Execução run-... | etapas=19 | falhas=0 | ~78s | 12.88 MB | quarentena=55
```

**19 etapas, 0 falhas, 55 registros em quarentena** é o resultado esperado. Os 55 são
alunos sem resultado de prova ou com proficiência impossível — eles vão para a
quarentena de propósito, não são um erro.

```powershell
# 2) Streaming com Kafka de verdade (produtor + consumidor + broker)
docker compose --profile streaming up

# encerre com Ctrl+C quando quiser; para limpar os contêineres:
docker compose --profile streaming down
```

```powershell
# 3) Notebooks no navegador
docker compose --profile notebooks up jupyter
# abra http://localhost:8888  (sem senha)
```

> Se o Docker não conseguir baixar o jar do Delta Lake (rede corporativa, proxy), a
> pipeline **não quebra**: ela registra um aviso no log e continua em Parquet. A
> semântica é a mesma; só se perde ACID e time travel.

**Se o build falhar em `openjdk-17-jre-headless has no installation candidate`,** sua
cópia do `Dockerfile` é anterior à correção. Atualize o repositório e force a
reconstrução:

```powershell
docker compose build --no-cache pipeline
```

A causa: a imagem `python:3.11-slim` acompanha a versão estável do Debian, que passou a
ser o *trixie* — e o trixie removeu o pacote do Java 17. O Dockerfile agora fixa a base
em `bookworm` e aceita Java 17 ou 21 (o Spark 4 roda nos dois).

### Opção B — WSL2 (terminal Linux no Windows)

Mais leve que o Docker para iterar no código, e sem a complicação do `winutils`.

```powershell
wsl --install -d Ubuntu     # só na primeira vez; reinicie a máquina depois
```

Dentro do Ubuntu:

```bash
sudo apt update
# o Spark 4 roda em Java 17 ou 21 — instale o que a sua distro oferecer
sudo apt install -y python3-venv python3-pip \
  && (sudo apt install -y openjdk-17-jre-headless || sudo apt install -y openjdk-21-jre-headless)
cd Fiap-tech-2   # a pasta onde você clonou o repositório

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

export PYTHONPATH=.
python -m src.pipeline --reprocessar
pytest -q                    # 26 testes
```

### Opção C — Windows nativo

Funciona, mas o Spark no Windows exige o `winutils.exe` e o `hadoop.dll` — sem eles a
gravação de Parquet falha com erros de `NativeIO`. Só recomendo se as opções A e B não
forem viáveis.

1. Instale o **Python 3.11** (marque *Add Python to PATH*) e o **JDK 17**
   ([Temurin](https://adoptium.net/temurin/releases/?version=17)).
2. Baixe `winutils.exe` e `hadoop.dll` de uma distribuição compatível com Hadoop 3.x
   (ex.: repositório `cdarlint/winutils`) e coloque em `C:\hadoop\bin`.
3. Configure as variáveis de ambiente:

```powershell
setx HADOOP_HOME "C:\hadoop"
setx PATH "$env:PATH;C:\hadoop\bin"
# feche e reabra o PowerShell
```

4. Rode:

```powershell
cd Fiap-tech-2   # a pasta onde você clonou o repositório
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt

$env:PYTHONPATH = "."
python -m src.pipeline --reprocessar
```

Se aparecer erro de `winutils` mesmo assim, use a Opção A — não vale gastar tempo nisso.

---

## 2. O que analisar depois de rodar

Ordem sugerida para conferir o trabalho:

| Passo | Onde olhar | O que verificar |
|---|---|---|
| 1 | Terminal, ao fim da execução | `falhas=0`, 19 etapas, score de qualidade 100% |
| 2 | `data/_observabilidade/relatorio.md` | Tabela por camada: entrada, saída, quarentena, duração |
| 3 | `notebooks/01_entendimento_fontes.ipynb` | Gráfico do indicador por UF — a distância CE × BA |
| 4 | `notebooks/03_camada_gold_analises.ipynb` | O gráfico de quartis de IDHM (o mais forte do projeto) |
| 5 | `notebooks/04_aplicacao_em_ia.ipynb` | Modelo vazado (R² 0,95) × modelo honesto (R² 0,75) |
| 6 | `data/lakehouse/gold/` | As sete tabelas analíticas materializadas |
| 7 | `data/lakehouse/_quarentena/` | Os 55 registros reprovados, com `_motivo_quarentena` |

Os notebooks **já estão versionados com as saídas executadas** — dá para ler os
resultados sem rodar nada. Rodar de novo serve para provar que reproduz.

Comandos úteis durante a análise:

```bash
python -m src.observabilidade.relatorio        # regenera o relatório de monitoramento
python -m src.finops.estimativa_custos         # regenera a estimativa de custo
python scripts/gerar_dicionario.py             # regenera o dicionário de dados
pytest -q                                      # 26 testes
```

---

## 3. Databricks

O desafio pede que a solução seja *implementada em ambiente de nuvem*. O que já está no
repositório cobre isso: Terraform provisionando ADLS Gen2, workspace Databricks, Event
Hubs, Log Analytics e orçamento, mais os JSONs dos jobs batch e streaming — dá para ler
a arquitetura inteira sem que nada esteja rodando em nuvem paga.

Importar no Databricks (Free Edition serve) mostra o mesmo código rodando na
plataforma-alvo, com o Delta nativo em vez do fallback Parquet usado localmente quando
o jar do Delta não resolve.

### Como importar

1. Crie a conta em [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition).
2. No workspace: **Workspace → Repos → Add Repo** (em algumas versões, *Git folders*).
3. Cole a URL `https://github.com/m4ik-crtl/Fiap-tech-2.git` e clone.
4. Abra `cloud/azure/databricks/notebook_databricks.py` — o Databricks reconhece o
   formato de notebook em script Python e mostra as células.
5. Anexe a um cluster (ou compute serverless) e execute.

O notebook já cuida de:

- localizar a raiz do projeto e ajustar o `sys.path`;
- apontar `LAKEHOUSE_URI` para `/tmp/alfabetizacao/lakehouse` (o repositório clonado não
  é lugar para gravar volume de dados);
- usar `FORMATO_TABELA=delta`, que no Databricks é nativo;
- detectar o runtime via `em_databricks()` e reaproveitar a SparkSession da
  plataforma — sem `.master()`, sem baixar jars via Maven e sem `spark.stop()`, que
  encerraria a sessão compartilhada do notebook;
- opcionalmente registrar as tabelas Gold no Unity Catalog, o que libera consulta SQL e
  o `OPTIMIZE`/Z-ORDER de `cloud/azure/databricks/otimizacao.sql`.

### O que **não** dá para fazer na Free Edition

- **Streaming com Event Hubs**: exige uma assinatura Azure com custo. A perna de
  streaming é demonstrada localmente, com o Kafka do `docker compose` — que fala o
  mesmo protocolo.
- **Terraform aplicado de verdade**: `terraform plan` roda sem criar nada e já mostra o
  que seria provisionado. Não é necessário aplicar.

---

## Problemas comuns

| Sintoma | Causa provável | Solução |
|---|---|---|
| `docker: command not found` | Docker Desktop não está aberto | Abra o Docker Desktop e aguarde o ícone ficar verde |
| `Package 'openjdk-17-jre-headless' has no installation candidate` | Versão antiga do Dockerfile, que seguia a base `python:3.11-slim` — o Debian trixie removeu o pacote | Corrigido: a base agora é fixada em `bookworm` e o build aceita Java 17 **ou** 21. Se sua cópia ainda falhar, atualize o repositório e rode `docker compose build --no-cache pipeline` |
| `manifest for python:3.11-slim-bookworm not found` | Registro sem esse tag (raro) | Troque a linha `FROM` do `Dockerfile` por `python:3.11-slim` — o fallback para Java 21 já está no build |
| `no configuration file provided` | PowerShell em outra pasta | `cd` para a pasta do projeto antes |
| `JAVA_HOME is not set` | Rodando nativo sem JDK | Instale o JDK 17 ou use a Opção A |
| `HADOOP_HOME and hadoop.home.dir are unset` | Windows nativo sem winutils | Opção A ou C, passo 2 |
| `ModuleNotFoundError: No module named 'src'` | `PYTHONPATH` não definido | `$env:PYTHONPATH = "."` antes de rodar |
| `DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE` | Só aparece com Delta real (internet resolvendo o jar via Maven) — `overwriteSchema=true` não é compatível com `partitionOverwriteMode=dynamic` em tabelas particionadas (ex.: `bronze.uf`) | Corrigido em `src/spark_session.py`: a escrita força `partitionOverwriteMode=static` quando `overwriteSchema=true` |
| Pipeline diz `formato=parquet` e você esperava Delta | Jar do Delta não resolveu | Comportamento previsto — veja o aviso no log; no Docker com internet resolve |
