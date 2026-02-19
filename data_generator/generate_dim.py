# generate_dim.py
import pandas as pd
import os
from datetime import datetime

BASE_PATH = "/home/sunbeam/Desktop/SUNBEAM/Big/big_data_project/data"
os.makedirs(BASE_PATH, exist_ok=True)

# ---------------- DIM_CATEGORY ----------------
category_data = [
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
df_category = pd.DataFrame(category_data, columns=["category_id", "category_name"])
df_category.to_csv(os.path.join(BASE_PATH, "dim_category.csv"), index=False)

# ---------------- DIM_PRODUCT ----------------
product_data = [
    ("P101","Milk","C01",45), ("P102","Bread","C01",30),
    ("P103","Rice","C01",60), ("P104","Wheat Flour","C01",55),
    ("P105","Sugar","C01",40), ("P106","Cooking Oil","C01",150),
    ("P107","Eggs","C01",70), ("P108","Butter","C01",95),

    ("P201","Tea","C02",120), ("P202","Coffee","C02",180),
    ("P203","Fruit Juice","C02",90), ("P204","Soft Drink","C02",60),
    ("P205","Packaged Water","C02",20), ("P206","Green Tea","C02",160),
    ("P207","Milkshake","C02",80),

    ("P301","Chips","C03",50), ("P302","Biscuits","C03",40),
    ("P303","Cookies","C03",60), ("P304","Chocolate","C03",70),
    ("P305","Namkeen","C03",55), ("P306","Instant Noodles","C03",45),
    ("P307","Popcorn","C03",35), ("P308","Energy Bar","C03",90),

    ("P401","Shampoo","C04",180), ("P402","Conditioner","C04",190),
    ("P403","Soap","C04",40), ("P404","Body Wash","C04",160),
    ("P405","Toothpaste","C04",95), ("P406","Toothbrush","C04",60),
    ("P407","Hair Oil","C04",120),

    ("P501","Detergent Powder","C05",220), ("P502","Detergent Liquid","C05",240),
    ("P503","Dishwash Liquid","C05",110), ("P504","Floor Cleaner","C05",150),
    ("P505","Toilet Cleaner","C05",130), ("P506","Garbage Bags","C05",90),

    ("P601","Notebook","C06",60), ("P602","Pen","C06",10), ("P603","Pencil","C06",8),
    ("P604","Marker","C06",35), ("P605","Highlighter","C06",45), ("P606","File Folder","C06",50),
    ("P607","Calculator","C06",450),

    ("P701","Headphones","C07",3000), ("P702","Earphones","C07",1200),
    ("P703","Mobile Charger","C07",800), ("P704","Power Bank","C07",2200),
    ("P705","USB Cable","C07",250), ("P706","Bluetooth Speaker","C07",3500),

    ("P801","Water Bottle","C08",180), ("P802","Lunch Box","C08",250),
    ("P803","Storage Containers","C08",320), ("P804","Frying Pan","C08",900),
    ("P805","Spatula","C08",120), ("P806","Knife Set","C08",650),

    ("P901","Baby Diapers","C09",450), ("P902","Baby Wipes","C09",220),
    ("P903","Baby Soap","C09",90), ("P904","Baby Shampoo","C09",180),
    ("P905","Baby Lotion","C09",200),

    ("P1001","Face Mask","C10",40), ("P1002","Hand Sanitizer","C10",70),
    ("P1003","Vitamin Tablets","C10",350), ("P1004","Protein Powder","C10",1800),
    ("P1005","First Aid Kit","C10",500)
]
df_product = pd.DataFrame(product_data, columns=["product_id","product_name","category_id","unit_price"])
df_product.to_csv(os.path.join(BASE_PATH, "dim_product.csv"), index=False)

# ---------------- DIM_STORE ----------------
store_data = [
    ("S01", "Mumbai"), ("S02", "Pune"), ("S03", "Bangalore"),
    ("S04", "Delhi"), ("S05", "Chennai")
]
df_store = pd.DataFrame(store_data, columns=["store_id", "city"])
df_store.to_csv(os.path.join(BASE_PATH, "dim_store.csv"), index=False)

# ---------------- DIM_DATE ----------------
dates = pd.date_range(start="2026-01-01", end="2026-12-31")
records = []
for d in dates:
    date_id = int(d.strftime("%Y%m%d"))
    date_str = d.strftime("%Y-%m-%d")
    day = d.day
    week = d.isocalendar()[1]
    month = d.month
    quarter = f"Q{((d.month - 1)//3) + 1}"
    year = d.year
    is_weekend = "Yes" if d.weekday() >= 5 else "No"
    records.append([date_id, date_str, day, week, month, quarter, year, is_weekend])
df_date = pd.DataFrame(records, columns=["date_id","date","day","week","month","quarter","year","is_weekend"])
df_date.to_csv(os.path.join(BASE_PATH, "dim_date.csv"), index=False)

print("✅ Dimension CSVs generated")
