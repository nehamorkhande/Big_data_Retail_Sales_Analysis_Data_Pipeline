# Databricks notebook source
import dlt
from pyspark.sql.functions import *


# COMMAND ----------

my_rules={
    "rule1":"Products_id is not null",
    "rule2":"Products is not null"
}

# COMMAND ----------

@dlt.table()
def DimProducts_stage():
    df=spark.readStream.table("databricks_cata.silver.products_silver")
    return df

# COMMAND ----------

@dlt.view
def DimProducts_view():
    df=spark.readStream.table("Live.DimProducts_stage")
    return df
@dlt.table()
def DimProducts():
    df=spark.readStream.table("databricks_cata.silver.products_silver")
    return df


# COMMAND ----------

dlt.apply_changes(
    target="DimProducts",
    source="Live.DimProducts_view",
    keys=["product_id"],
    sequence_by="product_id",
    stored_as_scd_type=2
)

# COMMAND ----------

