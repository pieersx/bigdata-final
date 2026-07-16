FROM python:3.11.13-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
       openjdk-17-jre-headless \
       tini \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3 \
    SPARK_LOCAL_DIRS=/workspace/data/tmp \
    PATH="/opt/spark/bin:${PATH}"

WORKDIR /workspace

COPY pyproject.toml README.md requirements.txt ./
COPY src ./src
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[dev]"

COPY config ./config
COPY scripts ./scripts
COPY tests ./tests

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]

