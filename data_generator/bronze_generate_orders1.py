# bronze_generate_orders.py
import pandas as pd
import random
import os
import json

BASE_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/bronze"
os.makedirs(BASE_PATH, exist_ok=True)

dim_date = pd.read_csv("/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data/dim_date.csv")
dim_product = pd.read_csv("/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data/dim_product.csv")
dim_store = pd.read_csv("/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data/dim_store.csv")

for i in range(1, 1001):
    d = dim_date.sample(1).iloc[0]
    p_list = dim_product.sample(random.randint(1,4))
    s = dim_store.sample(1).iloc[0]

    items = [{"product_id": row["product_id"], "quantity": random.randint(1,5), "unit_price": row["unit_price"]} 
             for _, row in p_list.iterrows()]

    order = {
        "order_id": f"O{i:05d}",
        "store_id": s["store_id"],
        "order_timestamp": str(d["date"]) + "T10:00:00",
        "items": items
    }

    with open(os.path.join(BASE_PATH, f"order_{i:05d}.json"), "w") as f:
        json.dump(order, f)
