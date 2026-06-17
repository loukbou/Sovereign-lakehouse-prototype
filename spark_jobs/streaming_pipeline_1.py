"""
Generic High-Throughput Shift-Left Governance Streaming Pipeline.
Fully optimized to run computations natively on distributed worker nodes.
Supports Avro (Schema Registry) and JSON formats.
Schema drift detection removed — Schema Registry enforces structure at the producer level.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    LongType, BooleanType, TimestampType,
)

from contract_engine_1 import ContractEngine

# ── Cluster Routing Configs ──────────────────────────────────────────────────
KAFKA_BROKERS       = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
KAFKA_TOPIC         = os.environ.get("KAFKA_TOPIC", "bronze.sensors.alerts")
CONTRACTS_DIR       = os.environ.get("CONTRACTS_DIR", "/opt/data_contracts")
TRIGGER_INTERVAL    = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


def get_spark_schema_from_contract(engine: ContractEngine) -> StructType:
    """Build a Spark StructType from the contract field definitions (JSON mode only)."""
    type_map = {
        "string":    StringType(),
        "int":       IntegerType(),
        "integer":   IntegerType(),
        "long":      LongType(),
        "float":     DoubleType(),
        "double":    DoubleType(),
        "boolean":   BooleanType(),
        "timestamp": TimestampType(),
    }
    fields = []
    for field_name, field_def in engine._fields.items():
        spark_type = type_map.get(field_def.get("type", "string").lower(), StringType())
        fields.append(StructField(field_name, spark_type, field_def.get("nullable", True)))
    return StructType(fields)


def make_batch_processor(engine: ContractEngine, target_schema: StructType = None):
    """
    Returns a foreachBatch processor closure.
    - Avro: from_avro() deserializes the payload before validation.
    - JSON: ContractEngine handles parsing internally via target_schema.
    Schema drift is NOT checked — Schema Registry guarantees structure upstream.
    """
    iceberg_table    = engine.iceberg_table
    quarantine_table = engine.quarantine_table
    engine_version   = engine.version
    defined_fields   = list(engine._fields.keys())
    format_type      = engine.format
    schema_reg_url   = SCHEMA_REGISTRY_URL

    def process_batch(batch_df, batch_id: int):
        if batch_df.isEmpty():
            return

        now_ts = F.current_timestamp()

        if format_type == "avro":
            # Deserialize Avro bytes using Schema Registry subject convention
            from pyspark.sql.avro.functions import from_avro
            subject = f"{KAFKA_TOPIC}-value"
            schema_registry_options = {
                "mode": "PERMISSIVE",
                "schema.registry.url": schema_reg_url,
            }
            deserialized_df = batch_df.withColumn(
                "parsed_payload",
                from_avro(F.col("value"), subject, schema_registry_options)
            ).withColumn(
                "raw_payload",
                F.col("value").cast("string")  # hex representation for quarantine tracing
            )
            validated_df = engine.build_native_validation_df(deserialized_df)

        else:  # JSON
            validated_df = engine.build_native_validation_df(batch_df, target_schema)

        # Project flat fields from parsed_payload struct
        projected_columns = [
            F.col(f"parsed_payload.{field_name}").alias(field_name)
            for field_name in defined_fields
        ]

        main_df = validated_df.select(
            *projected_columns,
            now_ts.alias("ingested_at"),
            F.lit(engine_version).alias("schema_version"),
            F.lit(format_type).alias("source_format"),
            F.col("is_valid"),
            F.col("validation_errors"),
            F.col("raw_payload"),
        )

        # Route valid records → Main Iceberg Bronze table
        valid_df = main_df.filter(F.col("is_valid") == True)
        if not valid_df.isEmpty():
            print(f"🚀 Batch {batch_id}: writing {valid_df.count()} valid records → {iceberg_table}")
            valid_df.drop("raw_payload").writeTo(iceberg_table).append()

        # Route invalid records → Quarantine table
        quarantine_df = (
            validated_df
            .filter(~F.col("is_valid"))
            .select(
                F.col("raw_payload"),
                now_ts.alias("ingested_at"),
                F.lit(engine_version).alias("schema_version"),
                F.lit(format_type).alias("source_format"),
                F.col("validation_errors"),
            )
        )
        if not quarantine_df.isEmpty():
            print(f"⚠️  Batch {batch_id}: {quarantine_df.count()} invalid records → {quarantine_table}")
            quarantine_df.writeTo(quarantine_table).append()

        total = batch_df.count()
        valid_count     = valid_df.count()     if not valid_df.isEmpty()     else 0
        invalid_count   = quarantine_df.count() if not quarantine_df.isEmpty() else 0
        print(f"✅ Batch {batch_id}: Total={total}, Valid={valid_count}, Invalid={invalid_count}")

    return process_batch


def main():
    # Load contract — sr_client param accepted but Schema Registry enforcement
    # already happens at the Avro producer serialization level.
    engine = ContractEngine.for_topic(KAFKA_TOPIC, CONTRACTS_DIR)

    target_schema = None
    if engine.format == "json":
        target_schema = get_spark_schema_from_contract(engine)
        print(f"✅ JSON mode: generated Spark schema with {len(engine._fields)} fields")
    else:
        print(f"✅ Avro mode: structure guaranteed by Schema Registry — no drift detection needed")

    print("=" * 60)
    for k, v in engine.summary().items():
        print(f"  {k:<20}: {v}")
    print("=" * 60)

    spark_builder = (
        SparkSession.builder
        .appName(f"streaming-{engine.name}-v{engine.version}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if engine.format == "avro":
        spark_builder = spark_builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.4.0"
        )

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"📡 Subscribing to topic : {KAFKA_TOPIC}")
    print(f"🔗 Kafka brokers        : {KAFKA_BROKERS}")
    print(f"📋 Format               : {engine.format.upper()}")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "earliest")
        .load()
    )

    query = (
        raw_df.writeStream
        .foreachBatch(make_batch_processor(engine, target_schema))
        .option("checkpointLocation", engine.checkpoint_path)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("🔄 Streaming pipeline started. Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
