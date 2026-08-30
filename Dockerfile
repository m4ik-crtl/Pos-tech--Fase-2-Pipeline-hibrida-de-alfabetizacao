# Imagem única para toda a pipeline (batch, streaming e notebooks).
#
# Duas decisões que evitam quebra silenciosa no build:
#
# 1. A base é fixada em **bookworm**, não no `python:3.11-slim` genérico. Esse
#    tag genérico segue a versão estável do Debian: quando ela virou trixie, o
#    pacote `openjdk-17-jre-headless` deixou de existir e o build passou a
#    falhar com "has no installation candidate". Pinar a distro é o mesmo
#    princípio de pinar a versão de uma dependência.
# 2. O `JAVA_HOME` é descoberto em tempo de build, não escrito à mão. O Spark 4
#    roda tanto em Java 17 quanto em 21, então o build aceita o que estiver
#    disponível e aponta um symlink estável para a JVM instalada.
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    JAVA_HOME=/opt/java \
    TZ=America/Sao_Paulo

RUN apt-get update \
    && ( apt-get install -y --no-install-recommends openjdk-17-jre-headless \
      || apt-get install -y --no-install-recommends openjdk-21-jre-headless ) \
    && apt-get install -y --no-install-recommends procps curl tini \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
    && rm -rf /var/lib/apt/lists/* \
    && java -version

WORKDIR /app

# Camada de dependências separada: muda pouco, aproveita o cache do Docker.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "src.pipeline"]
