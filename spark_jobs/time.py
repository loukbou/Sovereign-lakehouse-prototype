from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("Lakehouse Timeliness Evaluation")
        .getOrCreate()
    )

    # Leave log level enabled to catch errors if this part crashes
    # spark.sparkContext.setLogLevel("OFF")

    bronze_table = "nessie.bronze.sensors.alerts"
    bronze_df = spark.table(bronze_table)

    print("\n" + "=" * 70)
    print("RUNNING TIMELINESS METRICS...")
    print("=" * 70)

    timeliness_df = bronze_df.select(
        F.count("*").alias("valid_records"),
        F.round(F.avg(F.unix_timestamp("ingested_at") - F.unix_timestamp(F.to_timestamp("event_time"))), 3).alias("avg_freshness_seconds"),
        F.round(F.min(F.unix_timestamp("ingested_at") - F.unix_timestamp(F.to_timestamp("event_time"))), 3).alias("min_freshness_seconds"),
        F.round(F.max(F.unix_timestamp("ingested_at") - F.unix_timestamp(F.to_timestamp("event_time"))), 3).alias("max_freshness_seconds")
    )

    # Action 1: This is a prime suspect for the pointer error due to timestamp conversions
    perf = timeliness_df.collect()[0]

    print(f"Valid Records           : {perf['valid_records']}")
    print(f"Average Freshness (s)   : {perf['avg_freshness_seconds']}")
    print(f"Minimum Freshness (s)   : {perf['min_freshness_seconds']}")
    print(f"Maximum Freshness (s)   : {perf['max_freshness_seconds']}")
    print("=" * 70 + "\n")

    spark.stop()

if __name__ == "__main__":
    main()