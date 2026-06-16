# ================================================================
# Boston Spark Image — FROM apache/spark:3.5.1 (Official)
# Spark + JVM có sẵn, thêm Python + connector JARs
# ================================================================

FROM apache/spark:3.5.1

USER root

# ── Thêm Python và tools ─────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# ── Set Python cho PySpark ────────────────────────────────────────
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

# ── Download connector JARs (từ Maven Central) ───────────────────
WORKDIR /opt/spark/jars

# Kafka-Spark connector (Scala 2.12, Spark 3.5.1)
RUN curl -fsSL -O \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar"

RUN curl -fsSL -O \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar"

RUN curl -fsSL -O \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"

RUN curl -fsSL -O \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"

# GCS Connector (shaded)
RUN curl -fsSL -O \
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar"

# BigQuery Spark connector
RUN curl -fsSL -O \
    "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-3.5-bigquery/0.36.1/spark-3.5-bigquery-0.36.1.jar"

# ── Python packages ───────────────────────────────────────────────
# NOTE: pyspark KHÔNG cần pip — đã có trong SPARK_HOME/python
# NOTE: pandas max 2.0.3 vì image dùng Python 3.8
RUN pip3 install --no-cache-dir \
    "google-cloud-storage==2.14.0" \
    "google-cloud-bigquery==3.15.0" \
    "pyarrow==14.0.2" \
    "pandas==2.0.3" \
    "confluent-kafka==2.3.0" \
    "requests==2.31.0"

# ── Spark config ──────────────────────────────────────────────────
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# ── Clear entrypoint — docker-compose set command trực tiếp ──────
ENTRYPOINT []
CMD []

# apache/spark dùng user spark (uid 185)
USER spark
WORKDIR /opt/spark
