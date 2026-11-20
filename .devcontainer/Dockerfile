FROM mcr.microsoft.com/devcontainers/python:3.11-bookworm

USER root

# Instalacja OpenJDK 17 (wymagane przez Apache Spark 4.1 i Delta Lake 4.1) oraz procps
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Instalacja zależności Pythona dla PySpark 4.1.4 + Delta Lake 4.4.1 + DuckDB
COPY lab_07/requirements_delta.txt /tmp/requirements_delta.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements_delta.txt \
    && rm -f /tmp/requirements_delta.txt

# Pre-caching zależności Maven/Ivy dla Delta Lake 4.4.1 w katalogu domowym użytkownika vscode,
# aby pierwsze uruchomienie w Codespaces nie wymagało pobierania JAR-ów z internetu.
USER vscode
RUN python3 -c "from pyspark.sql import SparkSession; \
from delta import configure_spark_with_delta_pip; \
builder = SparkSession.builder.master('local[1]').appName('precache') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'); \
spark = configure_spark_with_delta_pip(builder).getOrCreate(); \
spark.range(1).write.format('delta').mode('overwrite').save('/tmp/precache_delta'); \
spark.stop()" && rm -rf /tmp/precache_delta
