# Databricks Demo Runbook
### Auto Loader · Structured Streaming · Spark Declarative Pipelines

**Scenario:** a QSR chain lands order events as JSON files in cloud storage. We ingest them
incrementally, enforce quality, and serve gold aggregates — first by hand, then declaratively.

**Total run time:** ~45 min with commentary, ~20 min if you skip notebook 01.

---

## 0 · What's in the kit

```
data/
  landing/orders/
    orders_2026_08_01.json    100 rows   baseline
    orders_2026_08_02.json    120 rows   incremental pickup
    orders_2026_08_03.json    150 rows   + loyalty_id, promo_code  → schema evolution
    orders_2026_08_04.json    140 rows   12 bad records            → expectations fire
    orders_2026_08_05.json    130 rows   spare, for continuous mode
  reference/
    stores.csv                8 stores across 5 Indian cities
    menu_items.csv            12 menu items in 5 categories
notebooks/
  00_setup.py                        run once
  01_autoloader_structured_streaming.py   interactive, hand-written streaming
  02_declarative_pipeline_python.py  pipeline source (Python)
  03_declarative_pipeline_sql.sql    pipeline source (SQL) — alternative to 02
  04_verify_and_event_log.sql        run after the pipeline
```

**Order event shape** — nested `items` array, which is what makes the bronze→silver
explode step feel real rather than toy:

```json
{"order_id":"ORD1057","store_id":"ST006","order_ts":"2026-08-01T15:44:16",
 "channel":"TAKEAWAY","payment_method":"NET_BANKING","customer_id":"CUST0311",
 "items":[{"item_id":"IT109","qty":1,"unit_price":129.0}],
 "order_total":129.0,"currency":"INR"}
```

**Injected defects in batch 04** (12 rows): null `store_id`, negative `order_total`,
`qty = 0`, `channel = 'UNKNOWN'`, `order_ts` in the year 2099.

---

## 1 · Prerequisites

- Unity Catalog enabled workspace (Free Edition works)
- DBR **15.4 LTS or later** — 16.x preferred. Serverless is fine and starts faster
- Permission to `CREATE SCHEMA` and `CREATE VOLUME` in some catalog
- If you can't use `main`, change `CATALOG` at the top of notebook 00 and in the
  pipeline configuration below

---

## 2 · Setup (do this the night before)

1. Import all five notebook files: **Workspace → Import → File**.
2. Open `00_setup.py`, attach compute, **Run all**. It creates
   `main.qsr_demo` and the `landing` volume.
3. **Catalog → main → qsr_demo → Volumes → landing → Upload to this volume:**
   - into `reference/` → `stores.csv`, `menu_items.csv`
   - into `orders/` → **`orders_2026_08_01.json` and `orders_2026_08_02.json` only**
4. Run the last cell of notebook 00 to register the two dimension tables.

> 🚫 **Do not upload batches 03, 04, 05 yet.** They are your live moments.

---

## 3 · Act I — Auto Loader by hand (15 min)

Notebook `01_autoloader_structured_streaming.py`.

| Step | Cell | Say this |
|---|---|---|
| 1 | Read definition | "Three options and we have incremental ingestion. Note `_rescued_data` — nothing gets silently dropped." |
| 2 | `availableNow` write | "Streaming bookkeeping, batch cost profile. The cluster doesn't idle overnight." |
| 3 | Row count per file | 220 rows, two files |
| 4 | **Re-run the write** | Count unchanged. "No MERGE, no load-date column, no bookmark table. The checkpoint knows." |
| 5 | 🎬 **Upload batch 03**, re-run | Stream fails: `UnknownFieldException`. Pause here. |
| 6 | Re-run once more | Succeeds, 370 rows, two new columns populated only for batch 03. |
| 7 | Continuous aggregation | Start the `processingTime="10 seconds"` query. |
| 8 | 🎬 **Upload batches 04 and 05** | Refresh the display cell; watch the numbers move. Open the stream monitor. |
| 9 | Stop all streams | Never leave a demo stream running. |

**The line that sets up Act II:** *"That was one table. Now imagine bronze, silver, and
three gold tables — five checkpoints to manage, ordering I have to get right by hand, and
zero data-quality enforcement."*

---

## 4 · Act II — the Declarative Pipeline (20 min)

### Create the pipeline

**Jobs & Pipelines → Create → ETL Pipeline**

| Field | Value |
|---|---|
| Name | `qsr_declarative_demo` |
| Source code | `02_declarative_pipeline_python.py` *(or `03_..._sql.sql`)* |
| Default catalog | `main` |
| Default schema | `qsr_demo_pipeline` |
| Pipeline mode | **Triggered** |
| Serverless | on |

**Advanced → Configuration**, add two entries:

| Key | Value |
|---|---|
| `demo.source_path` | `/Volumes/main/qsr_demo/landing/orders` |
| `demo.ref_schema` | `main.qsr_demo` |

### Run it

1. **Start.** The DAG builds itself — bronze → silver → three gold tables. Point out that
   nothing in the code declares that order. It was inferred from the table references.
2. Click `silver_order_lines` → **Data quality** tab. Expectations, pass/fail counts, per run.
3. Compare to notebook 01: no `checkpointLocation`, no `schemaLocation`, no orchestration.

### 🎬 The money moment

Upload **`orders_2026_08_04.json`** (if you didn't already in Act I — if you did, use 05).
Hit **Start** again.

- Only the new file is read — Auto Loader again, but managed for you
- The Data quality tab lights up: ~12 failed records across five expectations
- Rows failing `DROP ROW` constraints never reach silver
- Gold recomputes and the numbers stay correct

Then in a SQL editor, run notebook `04_verify_and_event_log.sql`. The event-log query is
the closer: **data quality is a queryable table, not a dashboard someone hopes to notice.**

### Optional finale — continuous mode

Switch the pipeline to **Continuous**, start it, drop `orders_2026_08_05.json` into the
volume, and watch it flow through all five tables without you touching anything. Same code,
one setting. Stop it afterwards — continuous mode keeps compute alive.

---

## 5 · Talking points that land

- **"Auto Loader is not a file reader, it's a file *discovery* engine."** The value is
  knowing what it has already seen, at millions-of-files scale.
- **Directory listing vs file notification.** Default is listing, and it's incrementally
  optimised. Past ~a few hundred thousand files per directory, switch
  `cloudFiles.useNotifications` on and it subscribes to cloud events instead.
- **Schema evolution failing is a feature.** The alternative is a silent schema change
  mid-micro-batch, which is how columns quietly disappear from a dashboard.
- **The declarative shift:** you describe *what each table is*, not *when and in what order
  to compute it*. The dependency graph, checkpoints, retries, backfills and refresh
  strategy all become the runtime's problem.
- **Naming, for the pedants in the room:** DLT → renamed **Lakeflow Declarative Pipelines**
  (2025) → the framework was donated to Apache Spark as **Spark Declarative Pipelines**,
  GA in **Spark 4.1**. The `dlt` module still works; `from pyspark import pipelines as dp`
  is the current form and is portable to OSS Spark.

---

## 6 · Reset between runs

In notebook 00, uncomment and run the reset cell, then:

```sql
DROP SCHEMA IF EXISTS main.qsr_demo_pipeline CASCADE;
```

and delete batches 03–05 from the volume. Regenerate data any time with
`python generate_data.py` (fixed seed — identical output every run).

---

## 7 · If something goes wrong on stage

| Symptom | Fix |
|---|---|
| `UnknownFieldException` | **Expected** after batch 03. Just re-run. |
| Pipeline can't find `stores` | `demo.ref_schema` config is missing or wrong |
| `pyspark.pipelines` not found | You ran notebook 02 interactively. It only works inside a pipeline. |
| Stream reads nothing new | Checkpoint already consumed it — delete `_checkpoints/` and re-run |
| `FAIL UPDATE` on `currency_is_inr` | Something edited the data; all generated rows are INR |
| Volume upload greyed out | Missing `WRITE VOLUME` — grant it or use a catalog you own |
