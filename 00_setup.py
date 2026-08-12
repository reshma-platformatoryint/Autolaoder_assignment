# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Catalog, Schema, Volumes
# MAGIC
# MAGIC Run this **once** before the demo. It creates the Unity Catalog objects and the
# MAGIC landing zone that Auto Loader will watch.
# MAGIC
# MAGIC **Compute:** any serverless or classic cluster on DBR 15.4 LTS+ (16.x recommended).

# COMMAND ----------

# DBTITLE 1,Configuration — change these two if needed
CATALOG = "main"          # any catalog you can CREATE SCHEMA in
SCHEMA  = "qsr_demo"
VOLUME  = "landing"

BASE      = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ORDERS    = f"{BASE}/orders"          # Auto Loader source directory
REFERENCE = f"{BASE}/reference"       # static dimension CSVs
CHECKPOINT= f"{BASE}/_checkpoints"    # streaming checkpoints (notebook 01 only)
SCHEMA_LOC= f"{BASE}/_schemas"        # Auto Loader inferred-schema store

spark.conf.set("demo.catalog", CATALOG)
spark.conf.set("demo.schema", SCHEMA)
spark.conf.set("demo.orders_path", ORDERS)

print(f"Landing zone : {ORDERS}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

for p in (ORDERS, REFERENCE, CHECKPOINT, SCHEMA_LOC):
    dbutils.fs.mkdirs(p)

display(dbutils.fs.ls(BASE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload the data
# MAGIC
# MAGIC In the left nav: **Catalog → `main` → `qsr_demo` → Volumes → `landing`**, then **Upload to this volume**.
# MAGIC
# MAGIC | Upload into | Files |
# MAGIC |---|---|
# MAGIC | `landing/reference/` | `stores.csv`, `menu_items.csv` |
# MAGIC | `landing/orders/` | `orders_2026_08_01.json`, `orders_2026_08_02.json` **only** |
# MAGIC
# MAGIC ⚠️ **Hold back batches 03, 04 and 05.** You drop them in live during the demo —
# MAGIC that is the whole point of showing incremental ingestion, schema evolution and
# MAGIC data-quality expectations.
# MAGIC
# MAGIC | File | What it demonstrates |
# MAGIC |---|---|
# MAGIC | `orders_2026_08_01.json` | baseline schema inference |
# MAGIC | `orders_2026_08_02.json` | incremental pickup — only the new file is read |
# MAGIC | `orders_2026_08_03.json` | **schema evolution** — adds `loyalty_id`, `promo_code` |
# MAGIC | `orders_2026_08_04.json` | **12 bad records** — expectations fire |
# MAGIC | `orders_2026_08_05.json` | spare batch for the continuous-mode finale |

# COMMAND ----------

# DBTITLE 1,Verify the upload
display(dbutils.fs.ls(ORDERS))
display(dbutils.fs.ls(REFERENCE))

# COMMAND ----------

# DBTITLE 1,Load the two static dimensions as Delta tables
for name in ("stores", "menu_items"):
    (spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{REFERENCE}/{name}.csv")
        .write.mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    print(f"created {CATALOG}.{SCHEMA}.{name}")

display(spark.table(f"{CATALOG}.{SCHEMA}.stores"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reset helper
# MAGIC Run this between rehearsals to get back to a clean slate.
# MAGIC Uncomment before running — it deletes checkpoints and pipeline tables.

# COMMAND ----------

# dbutils.fs.rm(CHECKPOINT, True)
# dbutils.fs.rm(SCHEMA_LOC, True)
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA}_pipeline CASCADE")
# for t in ["bronze_orders_raw","orders_by_channel"]:
#     spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")
# print("reset done")
