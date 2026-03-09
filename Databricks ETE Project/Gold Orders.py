# Databricks notebook source
df=spark.sql('select * from databricks_cata.silver.orders_silver')
df.display()

# COMMAND ----------

df_dimcus=spark.sql("select DimCustomerKey,customer_id as dim_customer_id from databricks_cata.gold.dimcustomers")
df_dimcus.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.dimproducts;

# COMMAND ----------

df_dimpro=spark.sql("select DimProductKey,product_id as dim_product_id from databricks_cata.gold.dimproducts")
df_dimpro.display()


# COMMAND ----------

