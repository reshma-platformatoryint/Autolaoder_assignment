"""
Generates the demo dataset for the Databricks Auto Loader + Declarative Pipelines demo.
Deterministic (fixed seed) so the demo is reproducible.
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

BASE = Path(__file__).parent / "data"
LANDING = BASE / "landing" / "orders"
REF = BASE / "reference"
LANDING.mkdir(parents=True, exist_ok=True)
REF.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------- reference
STORES = [
    ("ST001", "Koramangala Flagship", "Bengaluru", "South", "2019-03-14"),
    ("ST002", "Indiranagar 100ft Rd",  "Bengaluru", "South", "2020-07-02"),
    ("ST003", "Bandra Linking Road",   "Mumbai",    "West",  "2018-11-20"),
    ("ST004", "Powai Hiranandani",     "Mumbai",    "West",  "2021-01-11"),
    ("ST005", "Connaught Place",       "Delhi",     "North", "2017-05-30"),
    ("ST006", "Cyber Hub Gurugram",    "Delhi",     "North", "2022-02-18"),
    ("ST007", "Park Street",           "Kolkata",   "East",  "2020-09-09"),
    ("ST008", "Banjara Hills",         "Hyderabad", "South", "2023-04-25"),
]

MENU = [
    ("IT100", "Chickenjoy 2pc",        "Fried Chicken", 249.0),
    ("IT101", "Chickenjoy 6pc Bucket", "Fried Chicken", 649.0),
    ("IT102", "Spicy Chicken Burger",  "Burgers",       179.0),
    ("IT103", "Classic Beef Burger",   "Burgers",       199.0),
    ("IT104", "Jolly Spaghetti",       "Pasta",         129.0),
    ("IT105", "Palabok Fiesta",        "Pasta",         159.0),
    ("IT106", "Jolly Crispy Fries L",  "Sides",          99.0),
    ("IT107", "Peach Mango Pie",       "Desserts",       79.0),
    ("IT108", "Halo-Halo",             "Desserts",      139.0),
    ("IT109", "Iced Latte",            "Beverages",      129.0),
    ("IT110", "Cola 500ml",            "Beverages",      69.0),
    ("IT111", "Veg Burger Deluxe",     "Burgers",       169.0),
]

with open(REF / "stores.csv", "w") as f:
    f.write("store_id,store_name,city,region,opened_date\n")
    for r in STORES:
        f.write(",".join(r) + "\n")

with open(REF / "menu_items.csv", "w") as f:
    f.write("item_id,item_name,category,base_price\n")
    for r in MENU:
        f.write(f"{r[0]},{r[1]},{r[2]},{r[3]}\n")

# ---------------------------------------------------------------- orders
CHANNELS = ["DINE_IN", "TAKEAWAY", "DELIVERY", "DRIVE_THRU", "KIOSK"]
PAYMENTS = ["UPI", "CARD", "CASH", "WALLET", "NET_BANKING"]
PROMOS = ["FLAT50", "BOGO", "WEEKEND20", "APPONLY10"]

seq = 1000


def make_order(day: datetime, with_new_cols: bool, corrupt: str | None = None):
    """Build one order event. `corrupt` injects a specific data-quality defect."""
    global seq
    seq += 1
    n_lines = random.randint(1, 4)
    items = []
    for _ in range(n_lines):
        it = random.choice(MENU)
        qty = random.randint(1, 3)
        items.append({"item_id": it[0], "qty": qty, "unit_price": it[3]})

    total = round(sum(i["qty"] * i["unit_price"] for i in items), 2)
    ts = day + timedelta(
        hours=random.randint(8, 22),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    rec = {
        "order_id": f"ORD{seq}",
        "store_id": random.choice(STORES)[0],
        "order_ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "channel": random.choice(CHANNELS),
        "payment_method": random.choice(PAYMENTS),
        "customer_id": f"CUST{random.randint(1, 400):04d}",
        "items": items,
        "order_total": total,
        "currency": "INR",
    }

    # Batch 3 onwards: two brand-new fields appear upstream -> schema evolution
    if with_new_cols:
        rec["loyalty_id"] = (
            f"LOY{random.randint(1, 250):05d}" if random.random() < 0.6 else None
        )
        rec["promo_code"] = random.choice(PROMOS) if random.random() < 0.25 else None

    # Deliberate data-quality defects for the expectations demo
    if corrupt == "null_store":
        rec["store_id"] = None
    elif corrupt == "negative_total":
        rec["order_total"] = -abs(rec["order_total"])
    elif corrupt == "zero_qty":
        rec["items"][0]["qty"] = 0
    elif corrupt == "unknown_channel":
        rec["channel"] = "UNKNOWN"
    elif corrupt == "future_ts":
        rec["order_ts"] = "2099-01-01T12:00:00"

    return rec


BATCHES = [
    # (filename,               date,         n,   new_cols, n_corrupt)
    ("orders_2026_08_01.json", "2026-08-01", 100, False, 0),
    ("orders_2026_08_02.json", "2026-08-02", 120, False, 0),
    ("orders_2026_08_03.json", "2026-08-03", 150, True,  0),   # schema evolution
    ("orders_2026_08_04.json", "2026-08-04", 140, True,  12),  # dirty data
    ("orders_2026_08_05.json", "2026-08-05", 130, True,  6),   # hold back for live drop
]

DEFECTS = ["null_store", "negative_total", "zero_qty", "unknown_channel", "future_ts"]

for fname, datestr, n, new_cols, n_corrupt in BATCHES:
    day = datetime.strptime(datestr, "%Y-%m-%d")
    rows = [make_order(day, new_cols) for _ in range(n - n_corrupt)]
    for i in range(n_corrupt):
        rows.append(make_order(day, new_cols, corrupt=DEFECTS[i % len(DEFECTS)]))
    random.shuffle(rows)
    with open(LANDING / fname, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"{fname:28s} {n:4d} rows  new_cols={new_cols}  corrupt={n_corrupt}")

print("\nReference: stores.csv (8), menu_items.csv (12)")
