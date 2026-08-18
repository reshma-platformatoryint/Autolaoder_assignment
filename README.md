# Databricks Auto Loader Assignment

This repository is a **Databricks data-engineering assignment** that demonstrates how to ingest restaurant order events from JSON files, process them incrementally, validate data quality, and produce business-ready sales tables.

The assignment compares two approaches:

1. **Manually managed Auto Loader + Structured Streaming**
2. **Declarative Pipelines using Python or SQL**

The scenario is a quick-service restaurant chain with stores, menu items, and order events.

## Stack

- **Languages:** Python and SQL
- **Runtime:** Databricks Runtime 15.4 LTS or later
- **Technologies:** Apache Spark Structured Streaming, Auto Loader, Unity Catalog Volumes, Delta tables, Lakeflow/Spark Declarative Pipelines
- **Concepts:** Schema inference, schema evolution, checkpoints, watermarks, expectations, materialized views, and event logs

## Repository structure

```text
00_setup.py
  Creates the Unity Catalog catalog, schema, volume, and reference tables.

01_autoloader_structured_streaming.py
  Demonstrates manual Auto Loader and Structured Streaming.

02_declarative_pipeline_python.py
  Defines the complete bronze-to-silver-to-gold pipeline in Python.

03_declarative_pipeline_sql.sql
  SQL equivalent of the Python pipeline. Use this instead of notebook 02.

04_verify_and_event_log.sql
  Verifies output tables and queries pipeline data-quality events.

generate_data.py
  Generates deterministic stores, menu, and order-event data.

stores.csv
  Eight reference stores across Indian cities.

menu_items.csv
  Twelve menu items with categories and prices.

orders_2026_08_01.json
orders_2026_08_02.json
orders_2026_08_03.json
orders_2026_08_04.json
orders_2026_08_05.json
  Daily newline-delimited JSON order batches.

RUNBOOK.md
  Detailed instructions for running the Databricks demonstration.
```

## Pipeline architecture

```text
JSON order files in cloud storage
              │
              ▼
       Auto Loader ingestion
              │
              ▼
    bronze_orders / raw events
              │
       Explode nested items,
       cast fields, validate data
              ▼
       silver_order_lines
              │
       Join store/menu dimensions
              ▼
 ┌──────────────────┬────────────────┬─────────────────┐
 │                  │                │                 │
 ▼                  ▼                ▼                 
Daily store sales   Top items       Channel mix
```

### Bronze layer

The bronze layer preserves the incoming order events and adds metadata such as:

- Source filename
- Ingestion timestamp
- Auto Loader metadata
- Newly discovered columns

The goal is to maintain a replayable record of the source data.

### Silver layer

The silver layer converts the nested `items` array into one row per order line. It calculates fields such as:

- `order_date`
- `qty`
- `unit_price`
- `line_amount`
- `order_total`

It also applies data-quality rules:

- `order_id` must be present
- `store_id` must be present
- Quantity must be positive
- Amount must not be negative
- Channel must be recognized
- Timestamp must not be in the future
- Currency must be INR

Some rules drop invalid rows, some retain and count violations, and the currency rule fails the pipeline if violated.

### Gold layer

The pipeline creates three business-facing outputs:

- `gold_daily_store_sales`
  - Revenue and orders by date, store, city, and region
  - Units sold
  - Average basket value

- `gold_top_items`
  - Units and revenue by menu item and category
  - Number of orders containing each item

- `gold_channel_mix`
  - Orders and revenue by channel and payment method

## Data batches

The files are intentionally staged to demonstrate different behaviors:

| File | Purpose |
|---|---|
| `orders_2026_08_01.json` | Baseline schema inference; 100 rows |
| `orders_2026_08_02.json` | Incremental pickup; 120 rows |
| `orders_2026_08_03.json` | Schema evolution with `loyalty_id` and `promo_code`; 150 rows |
| `orders_2026_08_04.json` | 12 deliberately corrupted records; 140 rows |
| `orders_2026_08_05.json` | Spare batch for the continuous-streaming finale; 130 rows |

The corrupted records include:

- Null `store_id`
- Negative `order_total`
- Zero quantity
- Unknown channel
- A timestamp in the year 2099

## How to run it

### Prerequisites

You need:

- A Databricks workspace with Unity Catalog enabled
- Databricks Runtime 15.4 LTS or later
- Permission to create a schema and volume
- A catalog where you can create objects

### 1. Generate the data

The repository already includes the generated CSV and JSON files. To recreate them deterministically:

```bash
python generate_data.py
```

The generator uses a fixed random seed, so repeated runs produce the same data.

### 2. Run setup

Import `00_setup.py` into Databricks and run it. It creates the catalog objects and volume, including:

```text
main.qsr_demo
/Volumes/main/qsr_demo/landing/orders
/Volumes/main/qsr_demo/landing/reference
/Volumes/main/qsr_demo/landing/_checkpoints
/Volumes/main/qsr_demo/landing/_schemas
```

Upload the reference files to `reference/`:

```text
stores.csv
menu_items.csv
```

Initially upload only these order files to `orders/`:

```text
orders_2026_08_01.json
orders_2026_08_02.json
```

Hold back batches 3–5 because they are used during the live demonstration.

### 3. Run the manual streaming demonstration

Open `01_autoloader_structured_streaming.py`. It demonstrates:

1. Reading JSON files using Auto Loader
2. Saving progress with a checkpoint
3. Running with `availableNow`
4. Re-running without duplicating processed files
5. Schema evolution when batch 3 is uploaded
6. Continuous aggregation with `processingTime`
7. Watermarks for controlling streaming state

When batch 3 is uploaded, the stream is expected to fail once with `UnknownFieldException` because it introduces:

```text
loyalty_id
promo_code
```

Run the write cell again. Auto Loader updates the schema and processes the new file.

### 4. Create the declarative pipeline

Create a Databricks ETL pipeline using either:

```text
02_declarative_pipeline_python.py
```

or:

```text
03_declarative_pipeline_sql.sql
```

Do not use both because they define tables with the same names.

Add these pipeline configuration values:

```text
demo.source_path = /Volumes/main/qsr_demo/landing/orders
demo.ref_schema   = main.qsr_demo
```

Use a pipeline schema such as:

```text
main.qsr_demo_pipeline
```

The pipeline infers the dependency graph from table references:

```text
bronze_orders
      ↓
silver_order_lines
      ↓
 ┌──────────────┬──────────────┬──────────────┐
 │              │              │              │
 ▼              ▼              ▼              
gold_daily     gold_top       gold_channel
store_sales    items          mix
```

### 5. Test data quality

Upload:

```text
orders_2026_08_04.json
```

Restart the pipeline and inspect the **Data quality** tab for `silver_order_lines`.

You should observe that:

- Rows violating `DROP ROW` rules do not reach silver
- Other violations are retained and counted
- Gold tables are recomputed using valid data
- Quality results are written to the pipeline event log

### 6. Verify the results

Run `04_verify_and_event_log.sql` to check:

- Row counts for bronze, silver, and gold tables
- Revenue by region and store
- Top-performing menu items
- Channel and payment-method performance
- Pipeline execution events
- Data-quality failures per expectation

## What the assignment is testing

### Auto Loader is incremental

Auto Loader remembers which files it has consumed using a checkpoint. Re-running the stream should not duplicate data.

### Streaming can be batch-like or continuous

`availableNow=True` processes all currently available files and then stops. `processingTime="10 seconds"` keeps checking for new files.

### Schema evolution must be controlled

When batch 3 introduces new fields, the stream records the schema change and fails once instead of silently changing the schema mid-processing. Retrying allows the new fields to be processed safely.

### Bronze, silver, and gold have separate responsibilities

- **Bronze:** Preserve raw input
- **Silver:** Flatten, type, clean, and validate
- **Gold:** Publish business-level aggregates

### Declarative pipelines reduce orchestration code

The pipeline describes what each table contains. It does not manually define execution order or manage every checkpoint. The runtime derives dependencies from the table references.

### Data quality should be observable

Invalid data is not simply discarded without explanation. Expectations count violations and expose them through the pipeline event log.

## Important note

The runbook describes a `data/` and `notebooks/` directory structure, but the repository currently stores the files at the repository root. Use the root-level filenames directly:

```text
00_setup.py
01_autoloader_structured_streaming.py
02_declarative_pipeline_python.py
03_declarative_pipeline_sql.sql
04_verify_and_event_log.sql
```

For the complete step-by-step demonstration, see [`RUNBOOK.md`](RUNBOOK.md).
