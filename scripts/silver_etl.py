from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_date

spark = SparkSession.builder \
    .appName("RetailSilverETL") \
    .getOrCreate()

# -----------------------------
# Paths
# -----------------------------
bronze_path = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/bronze/date=*/order_*.json"

silver_path = "hdfs:///retail/silver/fact_orders"

dim_product_path = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data/dim_product.csv"
dim_store_path = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data/dim_store.csv"
dim_date_path = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data/dim_date.csv"

# -----------------------------
# Read Bronze
# -----------------------------
df_orders = spark.read.json(bronze_path)

df_orders = df_orders.withColumn("item", explode(col("items"))) \
    .withColumn("order_date", to_date(col("order_timestamp")))

df_orders = df_orders.select(
    col("order_id"),
    col("store_id"),
    col("order_date"),
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity"),
    col("item.unit_price").alias("unit_price")
)

# -----------------------------
# Read Dimensions
# -----------------------------
dim_product = spark.read.csv(dim_product_path, header=True)
dim_store = spark.read.csv(dim_store_path, header=True)
dim_date = spark.read.csv(dim_date_path, header=True)

# -----------------------------
# Join with Dimensions
# -----------------------------
df_silver = df_orders \
    .join(dim_product, "product_id", "left") \
    .join(dim_store, "store_id", "left") \
    .join(dim_date, df_orders.order_date == dim_date.date, "left")

# -----------------------------
# Write Silver to HDFS
# -----------------------------
df_silver.write \
    .mode("append") \
    .parquet(silver_path)

spark.stop()
