from pyspark.sql.functions import col, current_timestamp, concat_ws, lit, when, to_json, struct

from common.avro_kafka import decode_confluent_avro
from domains.logistics.validate_logistics import read_kafka, write_to_kafka


def run_readings_validation(spark, bootstrap_servers):
    with open("/opt/schemas/sensor_reading_event.avsc") as f:
        schema = f.read()

    decoded = decode_confluent_avro(
        read_kafka(spark, bootstrap_servers, "bronze.sensors.readings"),
        schema
    )

    flat = decoded.select(
        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),
        col("event.payload.reading_id").alias("reading_id"),
        col("event.payload.sensor_id").alias("sensor_id"),
        col("event.payload.machine_id").alias("machine_id"),
        col("event.payload.temperature").alias("temperature"),
        col("event.payload.vibration_level").alias("vibration_level"),
        col("event.payload.event_time").alias("event_time"),
    )

    checked = flat.withColumn(
        "error_reason",
        concat_ws("; ",
            when(col("reading_id").isNull(), lit("reading_id is null")),
            when(col("sensor_id").isNull(), lit("sensor_id is null")),
            when(col("machine_id").isNull(), lit("machine_id is null")),
            when((col("temperature") < -20) | (col("temperature") > 100), lit("temperature out of range")),
            when((col("vibration_level") < 0) | (col("vibration_level") > 20), lit("vibration_level out of range")),
        )
    )

    valid = checked.filter(col("error_reason") == "").withColumn("validated_at", current_timestamp())
    invalid = checked.filter(col("error_reason") != "").withColumn("failed_at", current_timestamp())

    return [
        write_to_kafka(valid.select(col("reading_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "silver.sensors.readings", "/tmp/checkpoints/silver_sensors_readings"),
        write_to_kafka(invalid.select(col("event_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "dlq.sensors.readings", "/tmp/checkpoints/dlq_sensors_readings"),
    ]


def run_machines_validation(spark, bootstrap_servers):
    with open("/opt/schemas/sensor_machine_event.avsc") as f:
        schema = f.read()

    decoded = decode_confluent_avro(
        read_kafka(spark, bootstrap_servers, "bronze.sensors.machines"),
        schema
    )

    flat = decoded.select(
        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),
        col("event.payload.machine_id").alias("machine_id"),
        col("event.payload.machine_type").alias("machine_type"),
        col("event.payload.site_id").alias("site_id"),
        col("event.payload.status").alias("status"),
        col("event.payload.installed_at").alias("installed_at"),
    )

    checked = flat.withColumn(
        "error_reason",
        concat_ws("; ",
            when(col("machine_id").isNull(), lit("machine_id is null")),
            when(col("site_id").isNull(), lit("site_id is null")),
            when(~col("status").isin("ACTIVE", "MAINTENANCE", "STOPPED"), lit("status is invalid")),
        )
    )

    valid = checked.filter(col("error_reason") == "").withColumn("validated_at", current_timestamp())
    invalid = checked.filter(col("error_reason") != "").withColumn("failed_at", current_timestamp())

    return [
        write_to_kafka(valid.select(col("machine_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "silver.sensors.machines", "/tmp/checkpoints/silver_sensors_machines"),
        write_to_kafka(invalid.select(col("event_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "dlq.sensors.machines", "/tmp/checkpoints/dlq_sensors_machines"),
    ]


def run_alerts_validation(spark, bootstrap_servers):
    with open("/opt/schemas/sensor_alert_event.avsc") as f:
        schema = f.read()

    decoded = decode_confluent_avro(
        read_kafka(spark, bootstrap_servers, "bronze.sensors.alerts"),
        schema
    )

    flat = decoded.select(
        col("event.event_id").alias("event_id"),
        col("event.domain").alias("domain"),
        col("event.event_type").alias("event_type"),
        col("event.produced_at").alias("produced_at"),
        col("event.payload.alert_id").alias("alert_id"),
        col("event.payload.sensor_id").alias("sensor_id"),
        col("event.payload.machine_id").alias("machine_id"),
        col("event.payload.severity").alias("severity"),
        col("event.payload.alert_type").alias("alert_type"),
        col("event.payload.event_time").alias("event_time"),
    )

    checked = flat.withColumn(
        "error_reason",
        concat_ws("; ",
            when(col("alert_id").isNull(), lit("alert_id is null")),
            when(col("sensor_id").isNull(), lit("sensor_id is null")),
            when(~col("severity").isin("LOW", "MEDIUM", "HIGH", "CRITICAL"), lit("severity is invalid")),
            when(~col("alert_type").isin("TEMPERATURE", "VIBRATION", "CONNECTIVITY", "POWER"), lit("alert_type is invalid")),
        )
    )

    valid = checked.filter(col("error_reason") == "").withColumn("validated_at", current_timestamp())
    invalid = checked.filter(col("error_reason") != "").withColumn("failed_at", current_timestamp())

    return [
        write_to_kafka(valid.select(col("alert_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "silver.sensors.alerts", "/tmp/checkpoints/silver_sensors_alerts"),
        write_to_kafka(invalid.select(col("event_id").alias("key"), to_json(struct("*")).alias("value")), bootstrap_servers, "dlq.sensors.alerts", "/tmp/checkpoints/dlq_sensors_alerts"),
    ]