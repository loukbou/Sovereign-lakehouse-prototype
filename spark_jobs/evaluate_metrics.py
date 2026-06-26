from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    spark = (
        SparkSession.builder
        .appName("Lakehouse Evaluation")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("OFF")

    bronze_table = "nessie.bronze.sensors.alerts"
    quarantine_table = "nessie.bronze.sensors.alerts_quarantine"

    bronze_df = spark.table(bronze_table)
    quarantine_df = spark.table(quarantine_table)

    results = {}

    # ==========================================================
    # TIMELINESS METRICS
    # ==========================================================

    timeliness_df = bronze_df.select(
        F.count("*").alias("valid_records"),
        F.round(
            F.avg(
                F.unix_timestamp("ingested_at")
                - F.unix_timestamp(F.to_timestamp("event_time"))
            ),
            3
        ).alias("avg_freshness_seconds"),
        F.round(
            F.min(
                F.unix_timestamp("ingested_at")
                - F.unix_timestamp(F.to_timestamp("event_time"))
            ),
            3
        ).alias("min_freshness_seconds"),
        F.round(
            F.max(
                F.unix_timestamp("ingested_at")
                - F.unix_timestamp(F.to_timestamp("event_time"))
            ),
            3
        ).alias("max_freshness_seconds")
    )

    perf = timeliness_df.collect()[0]

    results["valid_records"] = perf["valid_records"]
    results["avg_freshness"] = perf["avg_freshness_seconds"]
    results["min_freshness"] = perf["min_freshness_seconds"]
    results["max_freshness"] = perf["max_freshness_seconds"]

    # ==========================================================
    # GOVERNANCE METRICS
    # ==========================================================

    total_valid = bronze_df.count()
    total_invalid = quarantine_df.count()
    total_records = total_valid + total_invalid

    results["total_valid"] = total_valid
    results["total_invalid"] = total_invalid
    results["total_records"] = total_records

    conformity = (
        total_valid / total_records * 100
        if total_records > 0
        else 0
    )

    results["conformity"] = conformity

    # ==========================================================
    # COMPLETENESS
    # ==========================================================

    missing_sensor_records = quarantine_df.filter(
        F.array_join(
            F.col("validation_errors"),
            " "
        ).contains("sensor-id-not-empty")
    ).count()

    missing_event_time_records = quarantine_df.filter(
        F.array_join(
            F.col("validation_errors"),
            " "
        ).contains("event-time-not-empty")
    ).count()

    results["missing_sensor"] = missing_sensor_records
    results["missing_event_time"] = missing_event_time_records

    sensor_completeness = (
        (total_records - missing_sensor_records)
        / total_records
        * 100
        if total_records > 0
        else 0
    )

    event_time_completeness = (
        (total_records - missing_event_time_records)
        / total_records
        * 100
        if total_records > 0
        else 0
    )

    overall_completeness = min(
        sensor_completeness,
        event_time_completeness
    )

    results["sensor_completeness"] = sensor_completeness
    results["event_time_completeness"] = event_time_completeness
    results["overall_completeness"] = overall_completeness

    # ==========================================================
    # DETECTION RATE
    # ==========================================================

    detected_invalid_records = total_invalid

    detection_rate = (
        detected_invalid_records
        / detected_invalid_records
        * 100
        if detected_invalid_records > 0
        else 0
    )

    results["detected_invalid_records"] = detected_invalid_records
    results["detection_rate"] = detection_rate

    # ==========================================================
    # PRINT RESULTS
    # ==========================================================

    print("\n" + "=" * 70)
    print("LAKEHOUSE EVALUATION RESULTS")
    print("=" * 70)

    print("\n" + "=" * 70)
    print("TIMELINESS METRICS")
    print("=" * 70)

    print(f"Valid Records           : {results['valid_records']}")
    print(f"Average Freshness (s)   : {results['avg_freshness']}")
    print(f"Minimum Freshness (s)   : {results['min_freshness']}")
    print(f"Maximum Freshness (s)   : {results['max_freshness']}")

    print("\n" + "=" * 70)
    print("CONFORMITY")
    print("=" * 70)

    print(f"Total Records           : {results['total_records']}")
    print(f"Valid Records           : {results['total_valid']}")
    print(f"Invalid Records         : {results['total_invalid']}")
    print(f"Conformity (%)          : {results['conformity']:.2f}")

    print("\n" + "=" * 70)
    print("COMPLETENESS")
    print("=" * 70)

    print(f"Missing Sensor IDs      : {results['missing_sensor']}")
    print(f"Missing Event Times     : {results['missing_event_time']}")
    print(f"Sensor Completeness (%) : {results['sensor_completeness']:.2f}")
    print(f"Event Completeness (%)  : {results['event_time_completeness']:.2f}")
    print(f"Overall Completeness (%) : {results['overall_completeness']:.2f}")


    print("\n" + "=" * 70)
    print("DETECTION RATE")
    print("=" * 70)

    print(
        f"Detected Invalid Records : "
        f"{results['detected_invalid_records']}"
    )

    print(
        f"Detection Rate (%)       : "
        f"{results['detection_rate']:.2f}"
    )

    print("\n" + "=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()