#!/bin/bash
KAFKA_TOPIC=${1:-bronze.sensors.alerts}

echo "Submitting streaming pipeline for topic: ${KAFKA_TOPIC}"

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "streaming-${KAFKA_TOPIC}" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-avro_2.12:3.5.5 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
  --conf spark.driver.extraClassPath=/home/spark/.ivy2/jars/* \
  --conf spark.executor.extraClassPath=/home/spark/.ivy2/jars/* \
  --conf spark.executorEnv.KAFKA_TOPIC=${KAFKA_TOPIC} \
  --conf spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=kafka1:9092 \
  --conf spark.executorEnv.APICURIO_URL=http://apicurio:8080 \
  --conf spark.executorEnv.SCHEMA_REGISTRY_URL=http://apicurio:8080/apis/ccompat/v7 \
  --conf spark.driverEnv.KAFKA_TOPIC=${KAFKA_TOPIC} \
  --conf spark.driverEnv.KAFKA_BOOTSTRAP_SERVERS=kafka1:9092 \
  --conf spark.driverEnv.APICURIO_URL=http://apicurio:8080 \
  --conf spark.driverEnv.SCHEMA_REGISTRY_URL=http://apicurio:8080/apis/ccompat/v7 \
  --conf spark.driverEnv.TRIGGER_INTERVAL="30 seconds" \
  --py-files /opt/spark_jobs/contract_engine.py \
  /opt/spark_jobs/streaming_pipeline.py