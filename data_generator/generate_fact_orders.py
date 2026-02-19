# generate_fact_orders.py
import pandas as pd
import random
import os

BASE_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data"

dim_date = pd.read_csv(os.path.join(BASE_PATH, "dim_date.csv"))
dim_product = pd.read_csv(os.path.join(BASE_PATH, "dim_product.csv"))
dim_store = pd.read_csv(os.path.join(BASE_PATH, "dim_store.csv"))

orders = []
for i in range(1, 5001):  # 5000 orders
    d = dim_date.sample(1).iloc[0]
    p = dim_product.sample(1).iloc[0]
    s = dim_store.sample(1).iloc[0]

    qty = random.randint(1, 5)
    amount = qty * p["unit_price"]

    orders.append({
        "order_id": f"O{i:05d}",
        "date_id": d["date_id"],
        "product_id": p["product_id"],
        "store_id": s["store_id"],
        "quantity": qty,
        "order_amount": amount
    })

df = pd.DataFrame(orders)
df.to_csv(os.path.join(BASE_PATH, "fact_orders.csv"), index=False)
print("✅ fact_orders.csv generated")
