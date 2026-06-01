from src.common.spark_session import get_spark
from pyspark.sql.functions import col, sum as spark_sum
import glob
import shutil
import os

spark = get_spark()

# Read Data
customers = spark.read.parquet(
    "silver/customer_sales"
)

orders = spark.read.parquet(
    "bronze/orders"
)

products = spark.read.parquet(
    "bronze/products"
)

# Join Data
sales = orders.join(
    customers,
    "customer_id"
).join(
    products,
    "product_id"
)

# Calculate Revenue
sales = sales.withColumn(
    "revenue",
    col("quantity").cast("int") *
    col("price").cast("int")
)

# Aggregate for Power BI
gold_df = sales.groupBy(
    "category"
).agg(
    spark_sum("revenue").alias("total_sales")
)

gold_output = "gold/customer_sales"

gold_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(gold_output)

# Create single CSV file
csv_files = glob.glob(
    f"{gold_output}/part-*.csv"
)

if len(csv_files) > 0:

    if os.path.exists("customer_sales.csv"):
        os.remove("customer_sales.csv")

    shutil.copy(
        csv_files[0],
        "customer_sales.csv"
    )

print("Gold Layer Completed")

gold_df.show()

spark.stop()