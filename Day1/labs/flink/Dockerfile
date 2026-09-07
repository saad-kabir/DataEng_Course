# Flink for the Day 2 streaming labs (labs 2.3 and 2.4).
#
# The SQL client cannot read a Kafka topic without the connector jar on the
# cluster classpath, and the jar is version-locked to the Flink minor release:
# flink-sql-connector-kafka:5.0.0-2.2 is the build for Flink 2.2.x. Flink 2.3
# exists but has no matching connector release yet, which is why this image is
# pinned to 2.2.1 - a mismatched pair fails at job submission, not at startup,
# so it would break the lab rather than the build.
FROM apache/flink:2.2.1-java17

ARG KAFKA_CONNECTOR=5.0.0-2.2
RUN curl -fL --retry 3 \
      -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
 && test -s /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar
