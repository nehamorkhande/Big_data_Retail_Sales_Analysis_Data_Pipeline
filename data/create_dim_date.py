import pandas as pd
from datetime import datetime

# Target path
base_path = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data"
file_path = f"{base_path}/dim_date.csv"

# Create date range for full year 2026
dates = pd.date_range(start="2026-01-01", end="2026-12-31")

records = []

for d in dates:
    date_id = int(d.strftime("%Y%m%d"))
    date_str = d.strftime("%Y-%m-%d")
    day = d.day
    week = d.weekofyear - 1        # matches your sample (week starts from 0)
    month = d.month
    quarter = f"Q{((d.month - 1)//3) + 1}"
    year = d.year
    is_weekend = "Yes" if d.weekday() >= 5 else "No"

    records.append([
        date_id,
        date_str,
        day,
        week,
        month,
        quarter,
        year,
        is_weekend
    ])

# Create DataFrame
df = pd.DataFrame(records, columns=[
    "date_id", "date", "day", "week", "month", "quarter", "year", "is_weekend"
])

# Save CSV
df.to_csv(file_path, index=False)

print(f"✅ dim_date.csv created successfully at:\n{file_path}")
