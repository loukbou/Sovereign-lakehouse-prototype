from pyspark.sql import SparkSession

from domains.sales.validate_sales import run_sales_validation
from domains.sales.validate_customers import run_customer_validation
from domains.sales.validate_products import run_product_validation
from domains.logistics.validate_logistics import (
    run_shipments_validation,
    run_vehicles_validation,
    run_locations_validation,
)

from domains.sensors.validate_sensors import (
    run_readings_validation,
    run_machines_validation,
    run_alerts_validation,
)


def main():
    spark = (
        SparkSession.builder
        .appName("shift-left-governance-spark")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bootstrap_servers = "kafka1:9092,kafka2:9092,kafka3:9092"

    queries = []
    queries += run_sales_validation(spark, bootstrap_servers)
    queries += run_customer_validation(spark, bootstrap_servers)
    queries += run_product_validation(spark, bootstrap_servers)
    queries += run_shipments_validation(spark, bootstrap_servers)
    queries += run_vehicles_validation(spark, bootstrap_servers)
    queries += run_locations_validation(spark, bootstrap_servers)

    queries += run_readings_validation(spark, bootstrap_servers)
    queries += run_machines_validation(spark, bootstrap_servers)
    queries += run_alerts_validation(spark, bootstrap_servers)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()