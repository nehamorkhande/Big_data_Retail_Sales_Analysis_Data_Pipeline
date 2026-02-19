import pandas as pd
import os

# Target directory
base_path = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data"
file_path = os.path.join(base_path, "dim_category.csv")

# Ensure directory exists
os.makedirs(base_path, exist_ok=True)

data = [
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

df = pd.DataFrame(data, columns=["category_id", "category_name"])
df.to_csv(file_path, index=False)

print(f"✅ dim_category.csv created at: {file_path}")
