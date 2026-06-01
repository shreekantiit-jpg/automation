from src.common.spark_session import get_spark
from pyspark.sql.functions import col, sum as spark_sum
import glob
import shutil
import os

spark = get_spark()

customers = spark.read.parquet("silver/customer_sales")
orders = spark.read.parquet("bronze/orders")
products = spark.read.parquet("bronze/products")

sales = (
    orders
    .join(customers, "customer_id")
    .join(products, "product_id")
)

sales = sales.withColumn(
    "revenue",
    col("quantity").cast("int") *
    col("price").cast("int")
)

# Aggregate by product
gold_df = sales.groupBy(
    "product_id"
).agg(
    spark_sum("revenue").alias("total_sales")
)

gold_output = "gold/customer_sales"

gold_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(gold_output)

csv_files = glob.glob(
    f"{gold_output}/part-*.csv"
)

if csv_files:
    shutil.copy(
        csv_files[0],
        "customer_sales.csv"
    )

gold_df.show()

print("Gold Layer Completed")

spark.stop()