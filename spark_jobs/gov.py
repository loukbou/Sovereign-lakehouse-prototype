from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("Lakehouse Governance Evaluation")
        .getOrCreate()
    )

    bronze_table = "nessie.bronze.sensors.alerts"
    quarantine_table = "nessie.bronze.sensors.alerts_quarantine"

    bronze_df = spark.table(bronze_table)
    quarantine_df = spark.table(quarantine_table)

    print("\n" + "=" * 70)
    print("RUNNING GOVERNANCE & COMPLETENESS METRICS...")
    print("=" * 70)

    # Action 1: Row Counts
    total_valid = bronze_df.count()
    total_invalid = quarantine_df.count()
    total_records = total_valid + total_invalid
    conformity = (total_valid / total_records * 100) if total_records > 0 else 0

    # Action 2: String array processing (Another prime suspect for pointer crashes)
    missing_sensor_records = quarantine_df.filter(
        F.array_join(F.col("validation_errors"), " ").contains("sensor-id-not-empty")
    ).count()

    missing_event_time_records = quarantine_df.filter(
        F.array_join(F.col("validation_errors"), " ").contains("event-time-not-empty")
    ).count()

    sensor_completeness = ((total_records - missing_sensor_records) / total_records * 100) if total_records > 0 else 0
    event_time_completeness = ((total_records - missing_event_time_records) / total_records * 100) if total_records > 0 else 0
    overall_completeness = min(sensor_completeness, event_time_completeness)

    # Print Results
    print(f"Total Records           : {total_records}")
    print(f"Valid Records           : {total_valid}")
    print(f"Invalid Records         : {total_invalid}")
    print(f"Conformity (%)          : {conformity:.2f}")
    print("-" * 50)
    print(f"Missing Sensor IDs      : {missing_sensor_records}")
    print(f"Missing Event Times     : {missing_event_time_records}")
    print(f"Sensor Completeness (%) : {sensor_completeness:.2f}")
    print(f"Event Completeness (%)  : {event_time_completeness:.2f}")
    print(f"Overall Completeness (%): {overall_completeness:.2f}")
    print("=" * 70 + "\n")

    spark.stop()

if __name__ == "__main__":
    main()