from pyspark.sql.functions import (
    col,
    current_timestamp,
    concat_ws,
    lit,
    when,
    to_json,
    struct,
    expr
)
from pyspark.sql.avro.functions import from_avro


def run_sales_validation(spark, bootstrap_servers: str):
    with open("/opt/schemas/sales_transaction_event.avsc", "r") as f:
        avro_schema = f.read()

    bronze = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "bronze.sales.transactions")
        .option("startingOffsets", "latest")
        .load()
    )

    # Confluent Avro format = 5-byte header + Avro payload
    # byte 1 = magic byte
    # bytes 2-5 = schema id
    # payload starts at byte 6
    decoded = (
        bronze
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").alias("raw_binary_value"),
            expr("substring(value, 6, length(value) - 5)").alias("avro_payload")
        )
        .select(
            "kafka_key",
            "raw_binary_value",
            from_avro(col("avro_payload"), avro_schema).alias("event")
        )
    )

    flattened = decoded.select(
        "kafka_key",
        "raw_binary_value",

        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),

        col("event.payload.transaction_id").alias("transaction_id"),
        col("event.payload.customer_id").alias("customer_id"),
        col("event.payload.store_id").alias("store_id"),
        col("event.payload.amount").alias("amount"),
        col("event.payload.currency").alias("currency"),
        col("event.payload.payment_method").alias("payment_method"),
        col("event.payload.transaction_time").alias("transaction_time"),
    )

    checked = flattened.withColumn(
        "error_reason",
        concat_ws(
            "; ",
            when(col("transaction_id").isNull(), lit("transaction_id is null")),
            when(col("amount") <= 0, lit("amount must be positive")),
            when(~col("currency").isin("MAD", "EUR", "USD"), lit("currency is invalid")),
            when(~col("payment_method").isin("CARD", "CASH", "TRANSFER"), lit("payment_method is invalid")),
        )
    )

    valid = (
        checked
        .filter(col("error_reason") == "")
        .withColumn("validated_at", current_timestamp())
    )

    invalid = (
        checked
        .filter(col("error_reason") != "")
        .withColumn("failed_at", current_timestamp())
    )

    silver_output = valid.select(
        col("transaction_id").alias("key"),
        to_json(struct(
            "event_id",
            "domain",
            "event_type",
            "produced_at",
            "transaction_id",
            "customer_id",
            "store_id",
            "amount",
            "currency",
            "payment_method",
            "transaction_time",
            "validated_at"
        )).alias("value")
    )

    dlq_output = invalid.select(
        col("event_id").alias("key"),
        to_json(struct(
            "event_id",
            "domain",
            "event_type",
            "produced_at",
            "transaction_id",
            "customer_id",
            "store_id",
            "amount",
            "currency",
            "payment_method",
            "transaction_time",
            "error_reason",
            "failed_at"
        )).alias("value")
    )

    q1 = (
        silver_output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", "silver.sales.transactions")
        .option("checkpointLocation", "/tmp/checkpoints/silver_sales_transactions_avro")
        .outputMode("append")
        .start()
    )

    q2 = (
        dlq_output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", "dlq.sales.transactions")
        .option("checkpointLocation", "/tmp/checkpoints/dlq_sales_transactions_avro")
        .outputMode("append")
        .start()
    )

    print("Sales Avro silver stream started:", q1.id)
    print("Sales Avro DLQ stream started:", q2.id)

    return [q1, q2]