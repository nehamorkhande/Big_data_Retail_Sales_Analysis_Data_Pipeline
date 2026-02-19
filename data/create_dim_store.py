import pandas as pd
import os

# Target directory
base_path = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data"
file_path = os.path.join(base_path, "dim_store.csv")

# Ensure directory exists
os.makedirs(base_path, exist_ok=True)

data = [
    ("S01", "Mumbai"),
    ("S02", "Pune"),
    ("S03", "Bangalore"),
    ("S04", "Delhi"),
    ("S05", "Chennai")
]

df = pd.DataFrame(data, columns=["store_id", "city"])

df.to_csv(file_path, index=False)

print(f"✅ dim_store.csv created successfully at:\n{file_path}")
