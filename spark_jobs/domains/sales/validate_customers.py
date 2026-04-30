from pyspark.sql.functions import col, current_timestamp, concat_ws, lit, when, to_json, struct
from common.avro_kafka import decode_confluent_avro

def run_customer_validation(spark, bootstrap_servers: str):
    with open("/opt/schemas/sales_customer_event.avsc", "r") as f:
        avro_schema = f.read()

    bronze = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "bronze.sales.customers")
        .option("startingOffsets", "latest")
        .load()
    )

    # Use the reusable decode function
    decoded = decode_confluent_avro(bronze, avro_schema)

    flattened = decoded.select(
        "kafka_key",
        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),
        col("event.payload.customer_id").alias("customer_id"),
        col("event.payload.full_name").alias("full_name"),
        col("event.payload.email").alias("email"),
        col("event.payload.city").alias("city"),
        col("event.payload.customer_segment").alias("customer_segment"),
        col("event.payload.registration_time").alias("registration_time"),
    )

    checked = flattened.withColumn(
        "error_reason",
        concat_ws(
            "; ",
            when(col("customer_id").isNull(), lit("customer_id is null")),
            when(~col("customer_segment").isin("RETAIL", "PREMIUM", "ENTERPRISE"), lit("customer_segment is invalid")),
            when(col("email").isNull(), lit("email is null")),
        )
    )

    valid = checked.filter(col("error_reason") == "").withColumn("validated_at", current_timestamp())
    invalid = checked.filter(col("error_reason") != "").withColumn("failed_at", current_timestamp())

    q1 = (
        valid.select(col("customer_id").alias("key"), to_json(struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", "silver.sales.customers")
        .option("checkpointLocation", "/tmp/checkpoints/silver_sales_customers_avro")
        .outputMode("append")
        .start()
    )

    q2 = (
        invalid.select(col("event_id").alias("key"), to_json(struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", "dlq.sales.customers")
        .option("checkpointLocation", "/tmp/checkpoints/dlq_sales_customers_avro")
        .outputMode("append")
        .start()
    )

    print("Sales customers silver stream started:", q1.id)
    print("Sales customers DLQ stream started:", q2.id)

    return [q1, q2]