# Debezium Connect + Confluent AvroConverter (Debezium images ship without it).
FROM maven:3.9.9-eclipse-temurin-17 AS avro-deps
COPY avro-converter-pom.xml /build/pom.xml
RUN mvn -q -f /build/pom.xml dependency:copy-dependencies \
      -DincludeScope=runtime -DoutputDirectory=/build/jars

FROM quay.io/debezium/connect:3.0
# own plugin dir: Kafka Connect loads it in isolation from the connectors
COPY --from=avro-deps /build/jars/ /kafka/connect/confluent-avro-converter/
