# File: airflow_dags/retail_etl_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import shutil
import pandas as pd
import random

# -------------------------------
# Paths
# -------------------------------
BASE_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data"
BRONZE_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/bronze"
SILVER_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/silver"
GOLD_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/gold"

# -------------------------------
# DAG default args
# -------------------------------
default_args = {
    'owner': 'sunbeam',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
}

dag = DAG(
    'retail_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# -------------------------------
# Task Functions
# -------------------------------

def generate_dimension_data():
    """Generate dim_category, dim_date, dim_product, dim_store CSVs."""
    # Category
    dim_category = [
        ("C01", "Grocery Staples"),
        ("C02", "Beverages"),
        ("C03", "Snacks & Packaged Foods"),
        ("C04", "Personal Care"),
        ("C05", "Household Cleaning"),
        ("C06", "Stationery & Office Supplies"),
        ("C07", "Electronics Accessories"),
        ("C08", "Home & Kitchen"),
        ("C09", "Baby Care"),
        ("C10", "Health & Wellness")
    ]
    pd.DataFrame(dim_category, columns=["category_id","category_name"]).to_csv(os.path.join(BASE_PATH,"dim_category.csv"), index=False)

    # Store
    dim_store = [("S01","Mumbai"),("S02","Pune"),("S03","Bangalore"),("S04","Delhi"),("S05","Chennai")]
    pd.DataFrame(dim_store, columns=["store_id","city"]).to_csv(os.path.join(BASE_PATH,"dim_store.csv"), index=False)

    # Product
    dim_product = [
        ("P101","Milk","C01",45), ("P102","Bread","C01",30), ("P201","Tea","C02",120),
        ("P202","Coffee","C02",180)
    ]  # You can extend as needed
    pd.DataFrame(dim_product, columns=["product_id","product_name","category_id","unit_price"]).to_csv(os.path.join(BASE_PATH,"dim_product.csv"), index=False)

    # Date
    dates = pd.date_range("2026-01-01","2026-12-31")
    records = []
    for d in dates:
        records.append([
            int(d.strftime("%Y%m%d")),
            d.strftime("%Y-%m-%d"),
            d.day,
            d.weekofyear-1,
            d.month,
            f"Q{((d.month-1)//3)+1}",
            d.year,
            "Yes" if d.weekday()>=5 else "No"
        ])
    pd.DataFrame(records, columns=["date_id","date","day","week","month","quarter","year","is_weekend"]).to_csv(os.path.join(BASE_PATH,"dim_date.csv"), index=False)

def generate_fact_orders():
    """Generate synthetic orders."""
    dim_date = pd.read_csv(os.path.join(BASE_PATH,"dim_date.csv"))
    dim_product = pd.read_csv(os.path.join(BASE_PATH,"dim_product.csv"))
    dim_store = pd.read_csv(os.path.join(BASE_PATH,"dim_store.csv"))

    orders = []
    for i in range(1, 1001):  # reduce for testing
        d = dim_date.sample(1).iloc[0]
        p = dim_product.sample(1).iloc[0]
        s = dim_store.sample(1).iloc[0]
        qty = random.randint(1,5)
        orders.append({
            "order_id": f"O{i:05d}",
            "date_id": d["date_id"],
            "product_id": p["product_id"],
            "store_id": s["store_id"],
            "quantity": qty,
            "order_amount": qty*p["unit_price"]
        })
    pd.DataFrame(orders).to_csv(os.path.join(BASE_PATH,"fact_orders.csv"), index=False)

def move_to_bronze():
    """Move raw data to bronze layer."""
    for file in os.listdir(BASE_PATH):
        if file.endswith(".csv"):
            shutil.copy(os.path.join(BASE_PATH, file), BRONZE_PATH)

def silver_etl():
    """Simple Silver ETL: join orders with product and store."""
    orders = pd.read_csv(os.path.join(BRONZE_PATH,"fact_orders.csv"))
    product = pd.read_csv(os.path.join(BRONZE_PATH,"dim_product.csv"))
    store = pd.read_csv(os.path.join(BRONZE_PATH,"dim_store.csv"))

    merged = orders.merge(product, on="product_id").merge(store, on="store_id")
    merged.to_csv(os.path.join(SILVER_PATH,"orders_silver.csv"), index=False)

def gold_aggregation():
    """Simple Gold aggregation: total sales per category."""
    orders = pd.read_csv(os.path.join(SILVER_PATH,"orders_silver.csv"))
    agg = orders.groupby("category_id").agg(total_sales=pd.NamedAgg(column="order_amount", aggfunc="sum")).reset_index()
    agg.to_csv(os.path.join(GOLD_PATH,"sales_gold.csv"), index=False)

# -------------------------------
# DAG Tasks
# -------------------------------
task_generate_dim = PythonOperator(
    task_id="generate_dimension_data",
    python_callable=generate_dimension_data,
    dag=dag
)

task_generate_fact = PythonOperator(
    task_id="generate_fact_orders",
    python_callable=generate_fact_orders,
    dag=dag
)

task_bronze = PythonOperator(
    task_id="bronze_layer",
    python_callable=move_to_bronze,
    dag=dag
)

task_silver = PythonOperator(
    task_id="silver_etl",
    python_callable=silver_etl,
    dag=dag
)

task_gold = PythonOperator(
    task_id="gold_aggregation",
    python_callable=gold_aggregation,
    dag=dag
)

# -------------------------------
# DAG Flow
# -------------------------------
task_generate_dim >> task_generate_fact >> task_bronze >> task_silver >> task_gold
