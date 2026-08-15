-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Verify the results & mine the event log
-- MAGIC
-- MAGIC Run this **after** the pipeline completes. Attach any SQL warehouse or cluster.
-- MAGIC Replace `main.qsr_demo_pipeline` if you targeted a different schema.

-- COMMAND ----------

USE CATALOG main;
USE SCHEMA qsr_demo_pipeline;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md ## Did the data land?

-- COMMAND ----------

SELECT 'bronze_orders' AS tbl, count(*) AS rows FROM bronze_orders
UNION ALL SELECT 'silver_order_lines', count(*) FROM silver_order_lines
UNION ALL SELECT 'gold_daily_store_sales', count(*) FROM gold_daily_store_sales
UNION ALL SELECT 'gold_top_items', count(*) FROM gold_top_items
UNION ALL SELECT 'gold_channel_mix', count(*) FROM gold_channel_mix;

-- COMMAND ----------

SELECT region, city, store_name,
       SUM(orders) AS orders, ROUND(SUM(revenue),2) AS revenue,
       ROUND(AVG(avg_basket_value),2) AS avg_basket
FROM gold_daily_store_sales
GROUP BY region, city, store_name
ORDER BY revenue DESC;

-- COMMAND ----------

SELECT category, item_name, SUM(units_sold) AS units, ROUND(SUM(revenue),2) AS revenue
FROM gold_top_items
GROUP BY category, item_name
ORDER BY revenue DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The event log — the payoff slide
-- MAGIC
-- MAGIC Every pipeline writes a structured event log. Data quality is **queryable**, not
-- MAGIC just a chart someone glances at. Wire this into alerting and you have a real SLA.
-- MAGIC
-- MAGIC Get the exact `event_log` reference from the pipeline UI (**Settings → Event log**),
-- MAGIC or use `event_log(TABLE(main.qsr_demo_pipeline.silver_order_lines))`.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW pipeline_events AS
SELECT * FROM event_log(TABLE(main.qsr_demo_pipeline.silver_order_lines));

SELECT timestamp, event_type, message
FROM pipeline_events
ORDER BY timestamp DESC
LIMIT 30;

-- COMMAND ----------

-- DBTITLE 1,Expectation results per run
SELECT
  e.timestamp,
  e.details:flow_progress.status                       AS status,
  exp.name                                             AS expectation,
  CAST(exp.passed_records AS BIGINT)                   AS passed,
  CAST(exp.failed_records AS BIGINT)                   AS failed
FROM pipeline_events e
LATERAL VIEW EXPLODE(
  FROM_JSON(e.details:flow_progress.data_quality.expectations,
            'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')
) t AS exp
WHERE e.event_type = 'flow_progress'
  AND e.details:flow_progress.data_quality.expectations IS NOT NULL
ORDER BY e.timestamp DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Expected after uploading `orders_2026_08_04.json`:** roughly 12 failures spread
-- MAGIC across `valid_store`, `non_negative_amount`, `positive_qty`, `known_channel` and
-- MAGIC `plausible_timestamp`. Rows failing the three `DROP ROW` constraints are gone from
-- MAGIC silver; the rest are retained and *counted*.
-- MAGIC
-- MAGIC Close on this: the gold numbers are correct, the bad rows are accounted for, and
-- MAGIC nobody wrote a single line of quality-check code.

-- COMMAND ----------

-- DBTITLE 1,Bonus — lineage is free
DESCRIBE TABLE EXTENDED gold_daily_store_sales;