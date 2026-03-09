# Databricks notebook source
df=spark.read.table("databricks_cata.bronze.regions")
df.display()

# COMMAND ----------

df=df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.write.format("delta")\
.mode("overwrite")\
.save("abfss://silver@databrickseteneha2.dfs.core.windows.net/regions")

# COMMAND ----------

df1=spark.read.format("delta").load("abfss://silver@databrickseteneha2.dfs.core.windows.net/products")
df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver(
# MAGIC   
# MAGIC )

# COMMAND ----------

