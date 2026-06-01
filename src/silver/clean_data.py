from src.common.spark_session import get_spark

spark = get_spark()

customers = spark.read.parquet(
    "bronze/customers"
)

customers = customers.dropDuplicates()

customers.write.mode("overwrite") \
    .parquet("silver/customer_sales")

print("Silver Layer Completed")

spark.stop()