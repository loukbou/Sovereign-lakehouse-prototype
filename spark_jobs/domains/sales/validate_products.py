from pyspark.sql.functions import col, current_timestamp, concat_ws, lit, when, to_json, struct, expr
from pyspark.sql.avro.functions import from_avro


def run_product_validation(spark, bootstrap_servers: str):
    with open("/opt/schemas/sales_product_event.avsc", "r") as f:
        avro_schema = f.read()

    bronze = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "bronze.sales.products")
        .option("startingOffsets", "latest")
        .load()
    )

    decoded = (
        bronze
        .select(
            col("key").cast("string").alias("kafka_key"),
            expr("substring(value, 6, length(value) - 5)").alias("avro_payload")
        )
        .select(
            "kafka_key",
            from_avro(col("avro_payload"), avro_schema).alias("event")
        )
    )

    flattened = decoded.select(
        "kafka_key",
        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),
        col("event.payload.product_id").alias("product_id"),
        col("event.payload.product_name").alias("product_name"),
        col("event.payload.category").alias("category"),
        col("event.payload.price").alias("price"),
        col("event.payload.currency").alias("currency"),
        col("event.payload.updated_at").alias("updated_at"),
    )

    checked = flattened.withColumn(
        "error_reason",
        concat_ws(
            "; ",
            when(col("product_id").isNull(), lit("product_id is null")),
            when(col("price") <= 0, lit("price must be positive")),
            when(~col("currency").isin("MAD", "EUR", "USD"), lit("currency is invalid")),
            when(~col("category").isin("ELECTRONICS", "ACCESSORIES", "COMPUTING"), lit("category is invalid")),
        )
    )

    valid = checked.filter(col("error_reason") == "").withColumn("validated_at", current_timestamp())
    invalid = checked.filter(col("error_reason") != "").withColumn("failed_at", current_timestamp())

    q1 = (
        valid.select(col("product_id").alias("key"), to_json(struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", "silver.sales.products")
        .option("checkpointLocation", "/tmp/checkpoints/silver_sales_products_avro")
        .outputMode("append")
        .start()
    )

    q2 = (
        invalid.select(col("event_id").alias("key"), to_json(struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", "dlq.sales.products")
        .option("checkpointLocation", "/tmp/checkpoints/dlq_sales_products_avro")
        .outputMode("append")
        .start()
    )

    print("Sales products silver stream started:", q1.id)
    print("Sales products DLQ stream started:", q2.id)

    return [q1, q2]