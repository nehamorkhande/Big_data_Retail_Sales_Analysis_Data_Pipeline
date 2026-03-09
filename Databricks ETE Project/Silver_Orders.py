# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df=spark.read.format("parquet")\
    .load("abfss://bronze@databrickseteneha2.dfs.core.windows.net/orders")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumnRenamed("_rescued_data","rescued_data")
df.display()


# COMMAND ----------

df=df.drop("rescued_data")

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql.functions import to_timestamp, col

df = df.withColumn("order_date", to_timestamp(col('order_date')))
df.display(10)

# COMMAND ----------

df=df.withColumn("year",year(col('order_date')))
display(df)

# COMMAND ----------

df1=df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

df1=df1.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

df1=df1.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

class windows:
    def dense_rank(self,df):
        df_dense_rank=df.withColumn("dense_rank",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_dense_rank
    
    def rank(self,df):
        df_rank=df.withColumn("rank_flag",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_rank
    
    def row_number(self,df):
        df_row_number=df.withColumn("row_number",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_row_number


# COMMAND ----------

df_new=df
df_new.display()

# COMMAND ----------

obj=windows()
df_result=obj.dense_rank(df_new)

# COMMAND ----------

df_result=obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databrickseteneha2.dfs.core.windows.net/orders")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickseteneha2.dfs.core.windows.net/orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_cata.silver.orders_silver

# COMMAND ----------

