from pyspark.sql.functions import col, current_timestamp, concat_ws, lit, when, to_json, struct

from common.avro_kafka import decode_confluent_avro


def read_kafka(spark, bootstrap_servers, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )


def write_to_kafka(df, bootstrap_servers, topic, checkpoint):
    return (
        df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )


def run_shipments_validation(spark, bootstrap_servers):
    with open("/opt/schemas/logistics_shipment_event.avsc") as f:
        schema = f.read()

    decoded = decode_confluent_avro(
        read_kafka(spark, bootstrap_servers, "bronze.logistics.shipments"),
        schema
    )

    flat = decoded.select(
        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),
        col("event.payload.shipment_id").alias("shipment_id"),
        col("event.payload.vehicle_id").alias("vehicle_id"),
        col("event.payload.origin_city").alias("origin_city"),
        col("event.payload.destination_city").alias("destination_city"),
        col("event.payload.status").alias("status"),
        col("event.payload.weight_kg").alias("weight_kg"),
        col("event.payload.event_time").alias("event_time"),
    )

    checked = flat.withColumn(
        "error_reason",
        concat_ws("; ",
            when(col("shipment_id").isNull(), lit("shipment_id is null")),
            when(col("vehicle_id").isNull(), lit("vehicle_id is null")),
            when(~col("status").isin("CREATED", "IN_TRANSIT", "DELIVERED", "CANCELLED"), lit("status is invalid")),
            when(col("weight_kg") <= 0, lit("weight_kg must be positive")),
        )
    )

    valid = checked.filter(col("error_reason") == "").withColumn("validated_at", current_timestamp())
    invalid = checked.filter(col("error_reason") != "").withColumn("failed_at", current_timestamp())

    q1 = write_to_kafka(
        valid.select(col("shipment_id").alias("key"), to_json(struct("*")).alias("value")),
        bootstrap_servers,
        "silver.logistics.shipments",
        "/tmp/checkpoints/silver_logistics_shipments"
    )

    q2 = write_to_kafka(
        invalid.select(col("event_id").alias("key"), to_json(struct("*")).alias("value")),
        bootstrap_servers,
        "dlq.logistics.shipments",
        "/tmp/checkpoints/dlq_logistics_shipments"
    )

    return [q1, q2]


def run_vehicles_validation(spark, bootstrap_servers):
    with open("/opt/schemas/logistics_vehicle_event.avsc") as f:
        schema = f.read()

    decoded = decode_confluent_avro(
        read_kafka(spark, bootstrap_servers, "bronze.logistics.vehicles"),
        schema
    )

    flat = decoded.select(
        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),
        col("event.payload.vehicle_id").alias("vehicle_id"),
        col("event.payload.vehicle_type").alias("vehicle_type"),
        col("event.payload.capacity_kg").alias("capacity_kg"),
        col("event.payload.status").alias("status"),
        col("event.payload.updated_at").alias("updated_at"),
    )

    checked = flat.withColumn(
        "error_reason",
        concat_ws("; ",
            when(col("vehicle_id").isNull(), lit("vehicle_id is null")),
            when(col("capacity_kg") <= 0, lit("capacity_kg must be positive")),
            when(~col("status").isin("ACTIVE", "MAINTENANCE", "INACTIVE"), lit("status is invalid")),
        )
    )

    valid = checked.filter(col("error_reason") == "").withColumn("validated_at", current_timestamp())
    invalid = checked.filter(col("error_reason") != "").withColumn("failed_at", current_timestamp())

    return [
        write_to_kafka(valid.select(col("vehicle_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "silver.logistics.vehicles", "/tmp/checkpoints/silver_logistics_vehicles"),
        write_to_kafka(invalid.select(col("event_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "dlq.logistics.vehicles", "/tmp/checkpoints/dlq_logistics_vehicles"),
    ]


def run_locations_validation(spark, bootstrap_servers):
    with open("/opt/schemas/logistics_location_event.avsc") as f:
        schema = f.read()

    decoded = decode_confluent_avro(
        read_kafka(spark, bootstrap_servers, "bronze.logistics.locations"),
        schema
    )

    flat = decoded.select(
        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),
        col("event.payload.location_id").alias("location_id"),
        col("event.payload.city").alias("city"),
        col("event.payload.country").alias("country"),
        col("event.payload.zone_type").alias("zone_type"),
        col("event.payload.updated_at").alias("updated_at"),
    )

    checked = flat.withColumn(
        "error_reason",
        concat_ws("; ",
            when(col("location_id").isNull(), lit("location_id is null")),
            when(col("city").isNull(), lit("city is null")),
            when(~col("zone_type").isin("WAREHOUSE", "STORE", "HUB", "CUSTOMER_ZONE"), lit("zone_type is invalid")),
        )
    )

    valid = checked.filter(col("error_reason") == "").withColumn("validated_at", current_timestamp())
    invalid = checked.filter(col("error_reason") != "").withColumn("failed_at", current_timestamp())

    return [
        write_to_kafka(valid.select(col("location_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "silver.logistics.locations", "/tmp/checkpoints/silver_logistics_locations"),
        write_to_kafka(invalid.select(col("event_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "dlq.logistics.locations", "/tmp/checkpoints/dlq_logistics_locations"),
    ]