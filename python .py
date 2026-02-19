import os

BASE_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big data project/retail_analytics"

dirs = [
    f"{BASE_PATH}/data/master",
    f"{BASE_PATH}/data/bronze/orders",

    f"{BASE_PATH}/spark_jobs",

    f"{BASE_PATH}/output/silver/fact_orders",
    f"{BASE_PATH}/output/silver/dim_product",
    f"{BASE_PATH}/output/silver/dim_store",
    f"{BASE_PATH}/output/silver/dim_customer",
    f"{BASE_PATH}/output/silver/dim_date",

    f"{BASE_PATH}/output/gold/daily_product_sales",
    f"{BASE_PATH}/output/gold/daily_region_revenue",

    f"{BASE_PATH}/airflow"
]

for d in dirs:
    os.makedirs(d, exist_ok=True)

# Create empty files
files = [
    f"{BASE_PATH}/spark_jobs/bronze_to_silver.py",
    f"{BASE_PATH}/spark_jobs/silver_to_gold.py",
    f"{BASE_PATH}/spark_jobs/create_date_dim.py",
    f"{BASE_PATH}/airflow/retail_dag.py"
]

for f in files:
    open(f, "a").close()

print("✅ Retail analytics folder structure created successfully")
