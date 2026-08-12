# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Spark Declarative Pipeline (Python)
# MAGIC
# MAGIC ⚠️ **Do not run this notebook interactively.** The `pyspark.pipelines` module only
# MAGIC resolves inside a pipeline run. Attach it as **source code** to a Lakeflow pipeline
# MAGIC (see the runbook), then click *Start*.
# MAGIC
# MAGIC The same file runs unmodified on open-source Apache Spark 4.1+ — except for
# MAGIC `expect_*` and `create_auto_cdc_flow`, which are Databricks extensions.
# MAGIC
# MAGIC ```
# MAGIC   landing/orders/*.json
# MAGIC          │  Auto Loader
# MAGIC          ▼
# MAGIC   bronze_orders          (streaming table — append only, raw + metadata)
# MAGIC          │  explode line items, cast, quarantine bad rows
# MAGIC          ▼
# MAGIC   silver_order_lines     (streaming table + expectations)
# MAGIC          │  join dimensions, aggregate
# MAGIC          ▼
# MAGIC   gold_daily_store_sales / gold_top_items   (materialized views)
# MAGIC ```

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Read from the pipeline config so the same code promotes across environments.
SOURCE_PATH = spark.conf.get("demo.source_path")
REF_SCHEMA  = spark.conf.get("demo.ref_schema")   # e.g. main.qsr_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — ingest, don't judge
# MAGIC
# MAGIC No filtering, no casting beyond hints. Bronze is a faithful, replayable record of
# MAGIC what arrived. `@dp.table` + a streaming DataFrame = a **streaming table**.

# COMMAND ----------

@dp.table(
    name="bronze_orders",
    comment="Raw QSR order events landed by Auto Loader. Append-only.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", "order_total DOUBLE, order_ts TIMESTAMP")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(SOURCE_PATH)
            .select(
                "*",
                F.col("_metadata.file_name").alias("source_file"),
                F.current_timestamp().alias("ingested_at"),
            )
    )


# NOTE: no schemaLocation and no checkpointLocation above. The pipeline owns both.
# That is one of the quieter but bigger wins of the declarative model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — flatten and enforce the contract
# MAGIC
# MAGIC Expectations are the headline feature. Three flavours:
# MAGIC
# MAGIC | Decorator | On violation |
# MAGIC |---|---|
# MAGIC | `@dp.expect` | keep the row, count it, warn |
# MAGIC | `@dp.expect_or_drop` | drop the row, count it |
# MAGIC | `@dp.expect_or_fail` | **abort the run** |
# MAGIC
# MAGIC Every violation is counted per-run and charted in the pipeline UI. That is
# MAGIC observability you'd otherwise build yourself.

# COMMAND ----------

@dp.temporary_view(name="orders_exploded")
def orders_exploded():
    return (
        spark.readStream.table("bronze_orders")
            .withColumn("item", F.explode("items"))
            .select(
                "order_id", "store_id", "customer_id", "channel", "payment_method",
                F.col("order_ts").cast("timestamp").alias("order_ts"),
                F.col("order_ts").cast("date").alias("order_date"),
                F.col("item.item_id").alias("item_id"),
                F.col("item.qty").cast("int").alias("qty"),
                F.col("item.unit_price").cast("double").alias("unit_price"),
                (F.col("item.qty") * F.col("item.unit_price")).alias("line_amount"),
                F.col("order_total").cast("double").alias("order_total"),
                "currency", "source_file", "ingested_at",
            )
    )


@dp.table(
    name="silver_order_lines",
    comment="One row per order line. Validated and typed.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_store":    "store_id IS NOT NULL",
    "positive_qty":   "qty > 0",
})
@dp.expect_all({
    "non_negative_amount": "line_amount >= 0",
    "known_channel":       "channel IN ('DINE_IN','TAKEAWAY','DELIVERY','DRIVE_THRU','KIOSK')",
    "plausible_timestamp": "order_ts <= current_timestamp()",
})
@dp.expect_or_fail("currency_is_inr", "currency = 'INR'")
def silver_order_lines():
    return spark.readStream.table("orders_exploded")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — business-ready aggregates
# MAGIC
# MAGIC `@dp.materialized_view` with a batch read. The pipeline decides how to refresh it —
# MAGIC incrementally where it can, full recompute where it must. You do not write
# MAGIC that logic.

# COMMAND ----------

@dp.materialized_view(
    name="gold_daily_store_sales",
    comment="Daily revenue, orders and basket size per store.",
    table_properties={"quality": "gold"},
)
def gold_daily_store_sales():
    lines  = spark.read.table("silver_order_lines")
    stores = spark.read.table(f"{REF_SCHEMA}.stores")
    return (
        lines.join(stores, "store_id", "left")
            .groupBy("order_date", "store_id", "store_name", "city", "region")
            .agg(
                F.countDistinct("order_id").alias("orders"),
                F.round(F.sum("line_amount"), 2).alias("revenue"),
                F.sum("qty").alias("units_sold"),
                F.round(F.sum("line_amount") / F.countDistinct("order_id"), 2)
                 .alias("avg_basket_value"),
            )
    )


@dp.materialized_view(
    name="gold_top_items",
    comment="Item-level performance with menu category rollup.",
    table_properties={"quality": "gold"},
)
def gold_top_items():
    lines = spark.read.table("silver_order_lines")
    menu  = spark.read.table(f"{REF_SCHEMA}.menu_items")
    return (
        lines.join(menu, "item_id", "left")
            .groupBy("order_date", "item_id", "item_name", "category")
            .agg(
                F.sum("qty").alias("units_sold"),
                F.round(F.sum("line_amount"), 2).alias("revenue"),
                F.countDistinct("order_id").alias("orders_containing_item"),
            )
    )


@dp.materialized_view(
    name="gold_channel_mix",
    comment="Revenue split by order channel and payment method.",
    table_properties={"quality": "gold"},
)
def gold_channel_mix():
    return (
        spark.read.table("silver_order_lines")
            .groupBy("order_date", "channel", "payment_method")
            .agg(
                F.countDistinct("order_id").alias("orders"),
                F.round(F.sum("line_amount"), 2).alias("revenue"),
            )
    )
