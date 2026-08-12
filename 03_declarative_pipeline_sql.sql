-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · The same pipeline in SQL
-- MAGIC
-- MAGIC Use **either** notebook 02 (Python) **or** this one as the pipeline source — not
-- MAGIC both, or you'll get duplicate dataset names. Attach this one when your audience is
-- MAGIC analysts rather than engineers; it's the more persuasive version for that room.
-- MAGIC
-- MAGIC ⚠️ Not runnable in an interactive SQL editor. `STREAM`, `read_files` with a
-- MAGIC checkpoint, and `CONSTRAINT ... EXPECT` are resolved by the pipeline runtime.

-- COMMAND ----------

-- MAGIC %md ## Bronze — Auto Loader via `read_files`

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT "Raw QSR order events landed by Auto Loader. Append-only."
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
    *,
    _metadata.file_name AS source_file,
    current_timestamp() AS ingested_at
  FROM STREAM read_files(
    '${demo.source_path}',
    format               => 'json',
    inferColumnTypes     => true,
    schemaHints          => 'order_total DOUBLE, order_ts TIMESTAMP',
    schemaEvolutionMode  => 'addNewColumns'
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver — expectations as table constraints
-- MAGIC
-- MAGIC | Clause | On violation |
-- MAGIC |---|---|
-- MAGIC | `EXPECT (...)` | keep row, count it |
-- MAGIC | `EXPECT (...) ON VIOLATION DROP ROW` | drop row, count it |
-- MAGIC | `EXPECT (...) ON VIOLATION FAIL UPDATE` | abort the run |

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_order_lines (
  CONSTRAINT valid_order_id      EXPECT (order_id IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT valid_store         EXPECT (store_id IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT positive_qty        EXPECT (qty > 0)                     ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_amount EXPECT (line_amount >= 0),
  CONSTRAINT known_channel       EXPECT (channel IN ('DINE_IN','TAKEAWAY','DELIVERY','DRIVE_THRU','KIOSK')),
  CONSTRAINT plausible_timestamp EXPECT (order_ts <= current_timestamp()),
  CONSTRAINT currency_is_inr     EXPECT (currency = 'INR')            ON VIOLATION FAIL UPDATE
)
COMMENT "One row per order line. Validated and typed."
TBLPROPERTIES ("quality" = "silver")
AS SELECT
    order_id,
    store_id,
    customer_id,
    channel,
    payment_method,
    CAST(order_ts AS TIMESTAMP)      AS order_ts,
    CAST(order_ts AS DATE)           AS order_date,
    item.item_id                     AS item_id,
    CAST(item.qty AS INT)            AS qty,
    CAST(item.unit_price AS DOUBLE)  AS unit_price,
    item.qty * item.unit_price       AS line_amount,
    CAST(order_total AS DOUBLE)      AS order_total,
    currency,
    source_file,
    ingested_at
  FROM (
    SELECT *, EXPLODE(items) AS item
    FROM STREAM bronze_orders
  );

-- COMMAND ----------

-- MAGIC %md ## Gold — materialized views

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_store_sales
COMMENT "Daily revenue, orders and basket size per store."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    l.order_date,
    l.store_id,
    s.store_name,
    s.city,
    s.region,
    COUNT(DISTINCT l.order_id)                                     AS orders,
    ROUND(SUM(l.line_amount), 2)                                   AS revenue,
    SUM(l.qty)                                                     AS units_sold,
    ROUND(SUM(l.line_amount) / COUNT(DISTINCT l.order_id), 2)      AS avg_basket_value
  FROM silver_order_lines l
  LEFT JOIN ${demo.ref_schema}.stores s USING (store_id)
  GROUP BY l.order_date, l.store_id, s.store_name, s.city, s.region;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_top_items
COMMENT "Item-level performance with menu category rollup."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    l.order_date,
    l.item_id,
    m.item_name,
    m.category,
    SUM(l.qty)                        AS units_sold,
    ROUND(SUM(l.line_amount), 2)      AS revenue,
    COUNT(DISTINCT l.order_id)        AS orders_containing_item
  FROM silver_order_lines l
  LEFT JOIN ${demo.ref_schema}.menu_items m USING (item_id)
  GROUP BY l.order_date, l.item_id, m.item_name, m.category;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_channel_mix
COMMENT "Revenue split by order channel and payment method."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    order_date,
    channel,
    payment_method,
    COUNT(DISTINCT order_id)      AS orders,
    ROUND(SUM(line_amount), 2)    AS revenue
  FROM silver_order_lines
  GROUP BY order_date, channel, payment_method;
