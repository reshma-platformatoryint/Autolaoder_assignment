# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Auto Loader + Structured Streaming
# MAGIC
# MAGIC Auto Loader is a **Structured Streaming source** (`format("cloudFiles")`) that
# MAGIC incrementally discovers new files in cloud object storage.
# MAGIC
# MAGIC What we prove in this notebook:
# MAGIC 1. Schema inference and the `_rescued_data` safety net
# MAGIC 2. Exactly-once, incremental pickup driven by the checkpoint
# MAGIC 3. `Trigger.AvailableNow` (batch-like) vs `processingTime` (continuous)
# MAGIC 4. **Schema evolution** — a new upstream column arrives mid-stream
# MAGIC 5. A real streaming aggregation with a watermark

# COMMAND ----------

CATALOG, SCHEMA, VOLUME = "main", "qsr_demo", "landing"
BASE       = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ORDERS     = f"{BASE}/orders"
CHECKPOINT = f"{BASE}/_checkpoints"
SCHEMA_LOC = f"{BASE}/_schemas"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
display(dbutils.fs.ls(ORDERS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · The minimal Auto Loader read
# MAGIC
# MAGIC Three options do the heavy lifting:
# MAGIC
# MAGIC | Option | Why it matters |
# MAGIC |---|---|
# MAGIC | `cloudFiles.format` | the format of the files landing in the directory |
# MAGIC | `cloudFiles.schemaLocation` | where the **inferred schema** is persisted and versioned |
# MAGIC | `cloudFiles.inferColumnTypes` | infer real types instead of treating everything as string |
# MAGIC
# MAGIC Note this cell returns instantly — nothing has been read yet. Streaming is lazy.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_LOC}/bronze")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "order_total DOUBLE, order_ts TIMESTAMP")
        .load(ORDERS)
        # file metadata is free — always capture it in bronze
        .select(
            "*",
            F.col("_metadata.file_name").alias("source_file"),
            F.col("_metadata.file_modification_time").alias("file_modified_at"),
            F.current_timestamp().alias("ingested_at"),
        )
)

bronze_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Point out `_rescued_data`.** Auto Loader adds this column automatically. Any field
# MAGIC that doesn't match the inferred schema — a type mismatch, a column that appears in
# MAGIC one file but not another — lands here as JSON instead of being silently dropped.
# MAGIC This is the difference between a data-quality *incident* and a data-quality *record*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Write with `Trigger.AvailableNow`
# MAGIC
# MAGIC `availableNow` processes every file available right now, then **stops**. You get
# MAGIC streaming's incremental bookkeeping with batch's cost profile — no cluster idling
# MAGIC overnight waiting for a file. This is the default choice for most ingestion jobs.

# COMMAND ----------

q = (bronze_stream.writeStream
        .option("checkpointLocation", f"{CHECKPOINT}/bronze")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.bronze_orders_raw"))

q.awaitTermination()
print("batch complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, count(*) AS rows, min(ingested_at) AS first_seen
# MAGIC FROM main.qsr_demo.bronze_orders_raw
# MAGIC GROUP BY source_file ORDER BY source_file;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Idempotency — re-run and nothing happens
# MAGIC
# MAGIC Run the write cell above a second time. Row count is **unchanged**. The checkpoint
# MAGIC (specifically the RocksDB file registry inside it) knows which files it has already
# MAGIC consumed. No `MERGE`, no watermark table, no "load date" column, no manual bookmark.
# MAGIC
# MAGIC 👉 This is the single biggest reason to use Auto Loader over `spark.read.json(path)`
# MAGIC in a scheduled job.

# COMMAND ----------

display(spark.sql("SELECT count(*) AS total_rows FROM main.qsr_demo.bronze_orders_raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · 🎬 LIVE MOMENT — incremental pickup
# MAGIC
# MAGIC **Now upload `orders_2026_08_03.json`** into the volume, then re-run the write cell.
# MAGIC
# MAGIC Two things happen at once:
# MAGIC - Only the *new* file is read — batches 01 and 02 are not touched
# MAGIC - The stream **fails** with `UnknownFieldException`
# MAGIC
# MAGIC That failure is a feature. Batch 03 introduced `loyalty_id` and `promo_code`
# MAGIC upstream. The default `cloudFiles.schemaEvolutionMode` is `addNewColumns`:
# MAGIC Auto Loader records the new schema in the schema location and stops, so a
# MAGIC downstream consumer never sees a schema change mid-micro-batch.
# MAGIC **Simply re-run the write** — the second run starts with the widened schema and
# MAGIC continues from exactly where it stopped.
# MAGIC
# MAGIC In production, `retries` on the Lakeflow job (or a Declarative Pipeline) makes this
# MAGIC self-healing and you never see it.
# MAGIC
# MAGIC | `schemaEvolutionMode` | Behaviour |
# MAGIC |---|---|
# MAGIC | `addNewColumns` *(default)* | fail once, add columns, succeed on retry |
# MAGIC | `rescue` | never fail; new fields go to `_rescued_data` |
# MAGIC | `failOnNewColumns` | fail until a human updates the schema |
# MAGIC | `none` | ignore new columns entirely |

# COMMAND ----------

# DBTITLE 1,After the retry — the new columns are there
# MAGIC %sql
# MAGIC SELECT source_file,
# MAGIC        count(*)                       AS rows,
# MAGIC        count(loyalty_id)              AS with_loyalty,
# MAGIC        count(promo_code)              AS with_promo
# MAGIC FROM main.qsr_demo.bronze_orders_raw
# MAGIC GROUP BY source_file ORDER BY source_file;

# COMMAND ----------

# DBTITLE 1,Inspect the versioned schema store
display(dbutils.fs.ls(f"{SCHEMA_LOC}/bronze/_schemas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Structured Streaming proper — a continuous aggregation
# MAGIC
# MAGIC Same source, different trigger. `processingTime` keeps the query alive and
# MAGIC re-evaluates every N seconds. Add a **watermark** so state doesn't grow forever.

# COMMAND ----------

import time
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# ---------------------------------------------------------
# 1. Create completely fresh locations for every demo
# ---------------------------------------------------------

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_ORDERS = ORDERS.rstrip("/")

DEMO_INBOX = f"{BASE_ORDERS}_live_demo/{RUN_ID}"
DEMO_SCHEMA_LOC = f"{SCHEMA_LOC}/live_demo/{RUN_ID}"
DEMO_CHECKPOINT = f"{CHECKPOINT}/live_demo/{RUN_ID}"

TARGET_TABLE = "live_channel_sales_demo"

dbutils.fs.mkdirs(DEMO_INBOX)

print("Fresh live inbox:", DEMO_INBOX)


# ---------------------------------------------------------
# 2. Files that will arrive during the demonstration
# ---------------------------------------------------------

FILES_TO_ARRIVE = [
    (
        f"{BASE_ORDERS}/orders_2026_08_04.json",
        "orders_2026_08_04_live.json"
    ),
    (
        f"{BASE_ORDERS}/orders_2026_08_05.json",
        "orders_2026_08_05_live.json"
    )
]


# ---------------------------------------------------------
# 3. Provide schema because the live inbox starts empty
# ---------------------------------------------------------

order_schema = StructType([
    StructField("order_ts", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("order_total", DoubleType(), True)
])


# ---------------------------------------------------------
# 4. Define Auto Loader aggregation
# ---------------------------------------------------------

agg_demo = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", DEMO_SCHEMA_LOC)
         .option("cloudFiles.schemaEvolutionMode", "rescue")
         .option("cloudFiles.maxFilesPerTrigger", 1)
         .schema(order_schema)
         .load(DEMO_INBOX)

         .withColumn(
             "order_ts",
             F.col("order_ts").cast("timestamp")
         )

         # Prevent incorrect dates such as 2099
         .filter(
             (F.col("order_ts") >= F.to_timestamp(
                 F.lit("2026-08-01 00:00:00")
             )) &
             (F.col("order_ts") < F.to_timestamp(
                 F.lit("2026-09-01 00:00:00")
             ))
         )

         .withWatermark("order_ts", "1 day")

         .groupBy(
             F.window("order_ts", "1 day"),
             "channel"
         )

         .agg(
             F.count("*").alias("orders"),
             F.round(F.sum("order_total"), 2).alias("revenue")
         )

         .select(
             F.col("window.start").cast("date").alias("order_date"),
             "channel",
             "orders",
             "revenue"
         )
)


# ---------------------------------------------------------
# 5. Persist every updated aggregation
# ---------------------------------------------------------

def save_demo_aggregation(batch_df, batch_id):

    (batch_df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable(TARGET_TABLE)
    )


# ---------------------------------------------------------
# 6. Simulate files arriving one by one
# ---------------------------------------------------------

print("\nLive simulation started...")

for file_number, (staged_file, filename) in enumerate(
    FILES_TO_ARRIVE,
    start=1
):

    print(f"\nWaiting for file {file_number} to arrive...")
    time.sleep(10)

    destination = f"{DEMO_INBOX}/{filename}"

    # This represents an external system uploading a new file
    dbutils.fs.cp(staged_file, destination)

    print(f"New file arrived: {filename}")

    query = (
        agg_demo.writeStream
                .outputMode("complete")
                .foreachBatch(save_demo_aggregation)
                .option(
                    "checkpointLocation",
                    DEMO_CHECKPOINT
                )
                .trigger(availableNow=True)
                .start()
    )

    query.awaitTermination()

    input_rows = sum(
        (progress.get("numInputRows") or 0)
        for progress in (query.recentProgress or [])
    )

    print(f"Rows processed: {input_rows}")
    print("Updated live aggregation:")

    (spark.table(TARGET_TABLE)
          .orderBy(
              F.col("order_date").desc(),
              F.col("revenue").desc()
          )
          .show(truncate=False)
    )

print("\nLive file-arrival simulation completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC 🎬 **LIVE MOMENT:** with the query above still running, upload
# MAGIC `orders_2026_08_04.json` and then `orders_2026_08_05.json`.
# MAGIC Re-run the cell below every few seconds and watch the numbers move — no restart,
# MAGIC no redeploy. Also open the **stream monitor** rendered under the running cell:
# MAGIC input rows/sec, processing rate, and batch duration are the three numbers you tune.

# COMMAND ----------

display(spark.sql("""
  SELECT order_date, channel, orders, revenue
  FROM live_channel_sales
  ORDER BY order_date DESC, revenue DESC
"""))

# COMMAND ----------

# DBTITLE 1,Always stop your streams
for s in spark.streams.active:
    print("stopping", s.name)
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Where this breaks down
# MAGIC
# MAGIC Everything above is one table. A real medallion architecture needs bronze → silver →
# MAGIC gold, and hand-written streaming gives you:
# MAGIC
# MAGIC - a checkpoint path to invent and manage per table
# MAGIC - explicit ordering between tables (which stream starts first?)
# MAGIC - no data-quality contract — bad rows just land
# MAGIC - no lineage graph, no per-table observability
# MAGIC - backfill and full-refresh logic you write yourself
# MAGIC
# MAGIC That is exactly the gap Declarative Pipelines close → **notebook 02**.