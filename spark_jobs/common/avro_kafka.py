from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro


def decode_confluent_avro(kafka_df, avro_schema: str):
    return (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            expr("substring(value, 6, length(value) - 5)").alias("avro_payload")
        )
        .select(
            "kafka_key",
            from_avro(col("avro_payload"), avro_schema).alias("event")
        )
    )