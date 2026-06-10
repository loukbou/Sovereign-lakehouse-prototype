# docker/spark-iceberg/Dockerfile
FROM apache/spark:3.5.5

ENV SPARK_HOME=/opt/spark
ENV SPARK_JARS_DIR=/opt/spark/jars

USER root
RUN apt-get update && apt-get install -y curl gettext-base && rm -rf /var/lib/apt/lists/*

RUN rm -f /opt/spark/jars/iceberg-spark-runtime-*.jar \
          /opt/spark/jars/iceberg-aws-bundle-*.jar \
          /opt/spark/jars/nessie-*.jar

          # ===== KAFKA STREAMING CONNECTOR =====
RUN curl --fail -L \
  "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar" \
  -o "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar"
# =====================================
RUN curl --fail -L \
  "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar" \
  -o "/opt/spark/jars/kafka-clients-3.5.1.jar"
RUN curl --fail -L \
  "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar" \
  -o "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar"

# Iceberg Components (Stable 1.6.1 pairing)
RUN curl --fail -L \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar" \
  -o "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"

RUN curl --fail -L \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar" \
  -o "/opt/spark/jars/iceberg-aws-bundle-1.6.1.jar"

# Nessie Spark Extensions (Updated to verified 0.107.9)
RUN curl --fail -L \
  "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.107.9/nessie-spark-extensions-3.5_2.12-0.107.9.jar" \
  -o "/opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.107.9.jar"

# ===== CRITICAL ADDITIONS FOR S3A =====
# Hadoop AWS connector (provides S3AFileSystem class)
RUN curl --fail -L \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
  -o "/opt/spark/jars/hadoop-aws-3.3.4.jar"

# AWS SDK bundle (required by hadoop-aws)
RUN curl --fail -L \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
  -o "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
# =====================================

RUN chown -R spark:spark /opt/spark/jars

COPY docker/spark-iceberg/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Install the missing YAML package globally
RUN pip install --no-cache-dir pyyaml

# Drop back down to the unprivileged spark user safely
USER spark