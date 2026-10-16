FROM flink:1.20.1-scala_2.12-java17

USER root

ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG FLINK_VERSION=1.20.1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    curl -fsSL -o "/opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" && \
    # Avro + Schema Registry formats: avro-confluent, debezium-avro-confluent
    curl -fsSL -o "/opt/flink/lib/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar" && \
    # s3:// for checkpoints in MinIO
    mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar /opt/flink/plugins/s3-fs-hadoop/

USER flink
