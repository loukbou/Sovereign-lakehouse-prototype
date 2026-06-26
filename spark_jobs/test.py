from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.table("nessie.bronze.sensors.alerts")

print(df.count())

spark.stop()