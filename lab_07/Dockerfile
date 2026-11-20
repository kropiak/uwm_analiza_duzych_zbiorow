FROM spark:4.1.4-scala2.13-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv; \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/spark /opt/spark/work-dir; \
    chown -R spark:spark /home/spark /opt/spark/work-dir

USER spark
WORKDIR /opt/spark/work-dir/lab_07

ENV PATH="/home/spark/.local/bin:${PATH}"
COPY requirements_delta.txt /tmp/requirements_delta.txt
RUN pip3 install --no-cache-dir --user -r /tmp/requirements_delta.txt

# Pre-caching JAR-ów Delta Lake 4.4.1 w ~/.ivy2*
RUN python3 -c "from pyspark.sql import SparkSession; \
from delta import configure_spark_with_delta_pip; \
builder = SparkSession.builder.master('local[1]').appName('precache') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'); \
spark = configure_spark_with_delta_pip(builder).getOrCreate(); \
spark.range(1).write.format('delta').mode('overwrite').save('/tmp/precache_delta'); \
spark.stop()" && rm -rf /tmp/precache_delta

EXPOSE 8888 4040

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''"]
