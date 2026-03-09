# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df=spark.read.format("parquet")\
    .load("abfss://bronze@databrickseteneha2.dfs.core.windows.net/products")


# COMMAND ----------

df.display()

# COMMAND ----------

df=df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.createOrReplaceTempView("products")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.discount_func(P_price double) 
# MAGIC RETURNS double 
# MAGIC RETURN P_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id,price,databricks_cata.bronze.discount_func(price) as discounted_price 
# MAGIC FROM products 

# COMMAND ----------

df=df.withColumn("discounted_price",expr("databricks_cata.bronze.discount_func(price)"))
df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.upper_func(p_brand STRING) 
# MAGIC RETURNS STRING 
# MAGIC LANGUAGE PYTHON 
# MAGIC AS
# MAGIC $$
# MAGIC  return p_brand.upper()
# MAGIC $$
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT product_id,databricks_cata.bronze.upper_func(brand) as brand_upper 
# MAGIC FROM products

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@databrickseteneha2.dfs.core.windows.net/products")\
        .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickseteneha2.dfs.core.windows.net/products'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_cata.silver.products_silver

# COMMAND ----------

