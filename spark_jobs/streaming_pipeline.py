"""
Shift-Left Governance Streaming Pipeline.

Generic pipeline that:
  1. Loads the active data contract from Apicurio Registry (once at startup)
  2. Subscribes to a Kafka topic via Spark Structured Streaming
  3. Deserializes Avro (via from_avro + Apicurio) or JSON per micro-batch
  4. Compiles and applies contract quality rules as native Spark expressions
  5. Routes valid records to the Bronze Iceberg table
  6. Routes invalid records to the Quarantine Iceberg table

Trigger interval: 30 seconds (configurable via TRIGGER_INTERVAL env var)
Checkpointing:    Ceph S3 (s3a://) for exactly-once semantics
"""

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro

from contract_engine import ContractEngine

# ── Environment Config ────────────────────────────────────────────────────────
KAFKA_BROKERS      = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092")
KAFKA_TOPIC        = os.environ.get("KAFKA_TOPIC", "bronze.sensors.alerts")
TRIGGER_INTERVAL   = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
APICURIO_URL       = os.environ.get("APICURIO_URL", "http://apicurio:8080")

# Avro deserialization options — points Spark's from_avro() to Apicurio


import requests

def get_avro_schema_str(group_id: str, artifact_id: str) -> str:
    """Fetch raw Avro schema JSON string from Apicurio."""
    resp = requests.get(
        f"{APICURIO_URL}/apis/registry/v3/groups/{group_id}/artifacts/{artifact_id}/versions/branch=latest/content",
        headers={"Accept": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.text


def make_batch_processor(engine: ContractEngine, target_schema=None):
    iceberg_table = engine.iceberg_table
    quarantine_table = engine.quarantine_table
    engine_version = engine.version
    format_type = engine.format
    defined_fields = list(engine._fields.keys())

    # Fetch Avro schema ONCE here, not inside process_batch
    avro_schema_str = None
    if format_type == "avro":
        parts = engine.kafka_topic.split(".")
        avro_schema_str = get_avro_schema_str(parts[1], f"{parts[2]}-schema")

    def process_batch(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        now_ts = F.current_timestamp()

        # ── Step 1: Deserialize + validate ────────────────────────────────────
        if format_type == "avro":
            batch_df = (
                batch_df
                .withColumn("raw_payload", F.col("value").cast("string"))
                .withColumn(
                    "parsed_payload",
                    from_avro(
                        F.col("value"),
                        avro_schema_str,
                        {"mode": "PERMISSIVE"}   # don't crash on bad records
                    )
                )
            )
        validated_df = engine.build_native_validation_df(batch_df, target_schema)

        projected = [
            F.col(f"parsed_payload.{f}").alias(f)
            for f in defined_fields
        ]
        enriched_df = validated_df.select(
            *projected,
            now_ts.alias("ingested_at"),
            F.lit(engine_version).alias("schema_version"),
            F.lit(format_type).alias("source_format"),
            F.col("is_valid"),
            F.col("validation_errors"),
            F.col("raw_payload"),
        )

        # ── Step 3: Route valid → Bronze ──────────────────────────────────────
        valid_df = enriched_df.filter(F.col("is_valid"))
        if not valid_df.isEmpty():
            valid_df.writeTo(iceberg_table).append()
            print(f"[Batch {batch_id}] ✅ {valid_df.count()} valid → {iceberg_table}")

        # ── Step 4: Route invalid → Quarantine ────────────────────────────────
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
            quarantine_df.writeTo(quarantine_table).append()
            print(
                f"[Batch {batch_id}] ⚠️  {quarantine_df.count()} invalid "
                f"→ {quarantine_table}"
            )

        total = batch_df.count()
        valid_n = valid_df.count() if not valid_df.isEmpty() else 0
        inv_n   = quarantine_df.count() if not quarantine_df.isEmpty() else 0
        print(f"[Batch {batch_id}] Total={total} | Valid={valid_n} | Invalid={inv_n}")

    return process_batch


def main() -> None:
    # ── Load contract from Apicurio (single HTTP call) ────────────────────────
    engine = ContractEngine.for_topic(KAFKA_TOPIC)
    print(f"Rules loaded: {len(engine._rules)}")
    for r in engine._rules:
        print(f"  - {r.get('name')}: {r.get('expr')}")

    print("=" * 60)
    for k, v in engine.summary().items():
        print(f"  {k:<22}: {v}")
    print("=" * 60)

    # For JSON-format topics, build StructType from contract fields
    target_schema = None
    if engine.format == "json":
        target_schema = engine.get_spark_schema()
        print(f"JSON mode: schema built with {len(engine._fields)} fields")
    else:
        print("Avro mode: schema enforced by Apicurio Registry")

    # ── Build SparkSession ────────────────────────────────────────────────────
    builder = (
        SparkSession.builder
        .appName(f"pipeline-{engine.name}-v{engine.version}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "nessie")
        .config("spark.sql.catalog.nessie",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",
                "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.nessie.warehouse",
                "s3a://warehouse/")
        .config("spark.sql.catalog.nessie.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.s3.endpoint",
                os.environ.get("CEPH_ENDPOINT", "http://192.168.122.246:80"))
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id",
                os.environ.get("CEPH_ACCESS_KEY", "lakehouse"))
        .config("spark.sql.catalog.nessie.s3.secret-access-key",
                os.environ.get("CEPH_SECRET_KEY", "lakehouse123"))
        .config("spark.sql.catalog.nessie.s3.region",
                os.environ.get("CEPH_REGION", "lakehouse-zg"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if engine.format == "avro":
        print("Avro packages resolved via --packages at spark-submit time")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Subscribe to Kafka topic ──────────────────────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Start streaming query ─────────────────────────────────────────────────
    query = (
        raw_df.writeStream
        .foreachBatch(make_batch_processor(engine, target_schema))
        .option("checkpointLocation", engine.checkpoint_path)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"Pipeline running. Topic: {KAFKA_TOPIC} | Trigger: {TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()