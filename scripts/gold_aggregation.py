from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder \
    .appName("RetailGoldETL") \
    .getOrCreate()

silver_path = "hdfs:///retail/silver/fact_orders"
gold_path = "hdfs:///retail/gold"

df = spark.read.parquet(silver_path)

# -----------------------------------
# Daily Store Sales
# -----------------------------------
daily_store_sales = df.groupBy(
    "order_date", "store_id"
).agg(
    sum(col("quantity") * col("unit_price")).alias("daily_sales")
)

daily_store_sales.write \
    .mode("append") \
    .partitionBy("order_date") \
    .parquet(f"{gold_path}/daily_store_sales")

# -----------------------------------
# Daily Category Sales
# -----------------------------------
daily_category_sales = df.groupBy(
    "order_date", "category_id"
).agg(
    sum(col("quantity") * col("unit_price")).alias("total_sales")
)

daily_category_sales.write \
    .mode("append") \
    .partitionBy("order_date") \
    .parquet(f"{gold_path}/daily_category_sales")

spark.stop()
