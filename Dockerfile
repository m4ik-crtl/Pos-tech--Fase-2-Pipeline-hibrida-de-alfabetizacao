# Imagem única para toda a pipeline (batch, streaming e notebooks).
# Java 17 é a JVM homologada do Spark 4; Python 3.11 é a base do PySpark.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    TZ=America/Sao_Paulo

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless procps curl tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Camada de dependências separada: muda pouco, aproveita o cache do Docker.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "src.pipeline"]
