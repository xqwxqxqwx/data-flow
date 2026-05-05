FROM apache/spark:3.5.0 as spark_runtime

FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      python3-venv \
      openjdk-17-jre-headless \
      procps \
      curl \
      ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
COPY --from=spark_runtime /opt/spark /opt/spark
RUN chown -R airflow:0 /opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_BUNDLE_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN curl -fsSL -o "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" && \
    curl -fsSL -o "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fsSL -o "/opt/spark/jars/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar" && \
    curl -fsSL -o "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    chown -R airflow:0 /opt/spark/jars

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER airflow
COPY requirements.txt /requirements.txt
COPY requirements-dbt.txt /requirements-dbt.txt
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11
ARG AIRFLOW_CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${AIRFLOW_CONSTRAINT_URL}"

USER root
RUN python -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir -r /requirements-dbt.txt && \
    chown -R airflow:0 /opt/dbt_venv

USER airflow
