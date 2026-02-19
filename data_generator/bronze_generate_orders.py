import pandas as pd
import random
import os
import json
from datetime import datetime

# -------------------------
# Config
# -------------------------
BASE_BRONZE_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/bronze"
RUN_DATE = datetime.now().strftime("%Y-%m-%d")
DAILY_BRONZE_PATH = f"{BASE_BRONZE_PATH}/date={RUN_DATE}"

ORDERS_PER_DAY = 1000

# Create daily partition
os.makedirs(DAILY_BRONZE_PATH, exist_ok=True)

# -------------------------
# Load dimensions
# -------------------------
dim_product = pd.read_csv(
    "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data/dim_product.csv"
)
dim_store = pd.read_csv(
    "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data/dim_store.csv"
)

# -------------------------
# Generate Orders
# -------------------------
for i in range(1, ORDERS_PER_DAY + 1):
    products = dim_product.sample(random.randint(1, 4))
    store = dim_store.sample(1).iloc[0]

    items = [
        {
            "product_id": row["product_id"],
            "quantity": random.randint(1, 5),
            "unit_price": row["unit_price"]
        }
        for _, row in products.iterrows()
    ]

    order = {
        "order_id": f"{RUN_DATE.replace('-', '')}_O{i:05d}",
        "store_id": store["store_id"],
        "order_timestamp": f"{RUN_DATE}T10:00:00",
        "items": items
    }

    output_file = os.path.join(
        DAILY_BRONZE_PATH, f"order_{i:05d}.json"
    )

    with open(output_file, "w") as f:
        json.dump(order, f)
