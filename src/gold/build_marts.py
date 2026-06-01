from pyspark.sql import SparkSession
import glob
import shutil
import os

spark = SparkSession.builder \
    .appName("Gold Layer") \
    .master("local[*]") \
    .getOrCreate()

silver_df = spark.read.option("header", True) \
    .csv("silver/customer_sales")

gold_df = silver_df.groupBy("category") \
    .sum("sales_amount")

gold_output = "gold/customer_sales"

gold_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(gold_output)

csv_file = glob.glob(
    f"{gold_output}/part-*.csv"
)[0]

if os.path.exists("customer_sales.csv"):
    os.remove("customer_sales.csv")

shutil.copy(
    csv_file,
    "customer_sales.csv"
)

print("Gold Layer Completed")