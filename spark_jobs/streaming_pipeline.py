"""
Generic High-Throughput Shift-Left Governance Streaming Pipeline.
Fully optimized to run computations natively on distributed worker nodes.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
    BooleanType,
    TimestampType,
    MapType
)
from contract_engine import ContractEngine

# ── Cluster Routing Configs ──────────────────────────────────────────────────
KAFKA_BROKERS    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC",             "sensors-alerts")
CONTRACTS_DIR    = os.environ.get("CONTRACTS_DIR",           "/opt/data_contracts")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL",        "30 seconds")


def get_spark_schema_from_contract(engine: ContractEngine) -> StructType:

    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "long": LongType(),
        "float": DoubleType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType()
    }

    fields = []

    for field_name, field_def in engine._fields.items():

        spark_type = type_map.get(
            field_def.get("type", "string").lower(),
            StringType()
        )

        fields.append(
            StructField(
                field_name,
                spark_type,
                field_def.get("nullable", True)
            )
        )

    return StructType(fields)

def make_batch_processor(engine: ContractEngine, target_schema: StructType):
    
    def process_batch(batch_df, batch_id: int):
        if batch_df.isEmpty():
            return

        spark = SparkSession.getActiveSession()
        now_ts = F.current_timestamp()

        # 1. Parse string raw payload into explicit columns
        parsed_df = batch_df.withColumn("parsed_payload", F.from_json(F.col("value_str"), target_schema))
        
        # 2. Schema Drift Catch-All: Capture any field inside payload NOT explicitly defined in the contract
        all_json_map = F.from_json(F.col("value_str"), MapType(StringType(), StringType()))
        defined_fields = list(engine._fields.keys())
        
        # Drop contract-defined keys out of the map to capture only the rogue/drifted fields
        drift_map = F.map_filter(all_json_map, lambda k, v: ~k.isin(defined_fields))

        # 3. Apply compiled native expressions check rules
        validated_df = engine.build_native_validation_df(parsed_df)

        # 4. Project and prepare Main Table Data Structure
        projected_columns = []

        for field_name in engine._fields.keys():

            projected_columns.append(
                F.col(f"parsed_payload.{field_name}")
                .alias(field_name)
            )

        main_df = validated_df.select(
            *projected_columns,
            now_ts.alias("ingested_at"),
            F.lit(engine.version).alias("schema_version"),
            F.col("is_valid"),
            F.col("validation_errors"),
            drift_map.alias("additional_attributes")
        )
        # 5. Route records natively using Iceberg engine features
        # Append valid or partially valid records into your primary repository
        main_df.writeTo(engine.iceberg_table).append()

        # Isolate and route invalid records into Quarantine
        quarantine_df = (
        validated_df
        .filter(~F.col("is_valid"))
        .select(
            F.col("value_str").alias("raw_payload"),
            now_ts.alias("ingested_at"),
            F.lit(engine.version).alias("schema_version"),
            F.col("validation_errors")
        )
        )

        if not quarantine_df.isEmpty():

            quarantine_df.writeTo(
                engine.quarantine_table
            ).append()

        return process_batch

def main():
    engine = ContractEngine.for_topic(KAFKA_TOPIC, CONTRACTS_DIR)
    target_schema = get_spark_schema_from_contract(engine)

    print("=" * 60)
    for k, v in engine.summary().items():
        print(f"  {k:<20}: {v}")
    print("=" * 60)

    spark = (SparkSession.builder
             .appName(f"streaming-{engine.name}-v{engine.version}")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print(f"KAFKA_TOPIC={KAFKA_TOPIC}")
    print(f"KAFKA_BROKERS={KAFKA_BROKERS}")
    # Read binary stream out of Kafka cluster topology
    raw_df = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BROKERS)
              .option("subscribe",               engine.kafka_topic)
              .option("startingOffsets",         "latest")
              .option("failOnDataLoss",          "false")
              .load()
              .selectExpr("CAST(value AS STRING) as value_str"))

    # Execute processing micro-batch loops
    query = (raw_df.writeStream
             .foreachBatch(make_batch_processor(engine, target_schema))
             .option("checkpointLocation", engine.checkpoint_path)
             .trigger(processingTime=TRIGGER_INTERVAL)
             .start())
    
    query.awaitTermination()

if __name__ == "__main__":
    main()