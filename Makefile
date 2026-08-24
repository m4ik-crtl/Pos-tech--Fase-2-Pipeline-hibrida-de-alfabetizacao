# ============================================================================
# Atalhos do projeto. `make ajuda` lista tudo.
# ============================================================================

PYTHON ?= python
export PYTHONPATH := .

.PHONY: ajuda instalar dados batch streaming eventos tudo testes lint relatorio custos limpar docker-batch docker-streaming

ajuda:  ## Lista os alvos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

instalar:  ## Instala as dependências
	$(PYTHON) -m pip install -r requirements-dev.txt

dados:  ## Prepara a camada raw a partir das fontes em data/externo
	$(PYTHON) -m src.ingestao.preparar_raw

batch:  ## Executa raw -> bronze -> silver -> gold
	$(PYTHON) -m src.pipeline --reprocessar

eventos:  ## Publica eventos no stream (Kafka ou arquivo)
	$(PYTHON) -m src.ingestao.produtor_eventos --eventos 500 --intervalo 0.05

streaming:  ## Executa a ingestão em tempo quase real
	$(PYTHON) -m src.pipeline --etapas streaming

tudo: batch eventos streaming relatorio  ## Pipeline completa (batch + streaming + relatório)

testes:  ## Roda os testes automatizados
	$(PYTHON) -m pytest -q

lint:  ## Verifica estilo e erros estáticos
	$(PYTHON) -m ruff check src tests

relatorio:  ## Gera o relatório de monitoramento
	$(PYTHON) -m src.observabilidade.relatorio

custos:  ## Estima o custo mensal da arquitetura na nuvem
	$(PYTHON) -m src.finops.estimativa_custos

limpar:  ## Remove o lakehouse gerado, checkpoints e eventos
	rm -rf data/lakehouse data/_checkpoints data/stream_in data/_observabilidade

docker-batch:  ## Pipeline batch dentro do Docker
	docker compose run --rm pipeline

docker-streaming:  ## Kafka + produtor + consumidor no Docker
	docker compose --profile streaming up --abort-on-container-exit
