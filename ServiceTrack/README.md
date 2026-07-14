# ServiceTrack: Job Tracking & Customer Visit Analytics Pipeline

A batch data engineering pipeline, built on **Databricks, Apache Spark
(PySpark), Delta Lake, Python, and SQL**, that transforms raw service-center
data into structured, analysis-ready datasets using a **Medallion
Architecture** (Bronze → Silver → Gold).

It ingests raw CSV exports (customers, devices, service jobs), cleans and
enriches them, and produces four business-ready Gold tables covering repair
turnaround time, technician performance, repeat customer behavior, and
device failure trends.

## Architecture

```
Raw CSVs (Volume)                Bronze                    Silver                         Gold
─────────────────         ──────────────────       ──────────────────────       ──────────────────────────
customers.csv        ──▶  bronze_customers    ──▶   silver_customers        ──┐
devices.csv           ──▶  bronze_devices      ──▶   silver_devices          ──┼─▶ gold_repair_turnaround
service_jobs.csv    ──▶  bronze_service_jobs ──▶   silver_service_jobs    ──┤   gold_technician_performance
                                                                              ├─▶ gold_repeat_customers
                                                                              └─▶ gold_device_failure_trends
```

- **Bronze**: raw, 1:1 copy of the source CSVs as Delta tables, with
  ingestion metadata (`_ingest_ts`, `_source_file`, `_batch_id`). No business
  logic.
- **Silver**: cleaned, de-duplicated, correctly typed, referentially checked,
  and enriched with derived fields (e.g. `turnaround_days`, `is_overdue`).
- **Gold**: four aggregated marts, ready to plug into a BI tool or query
  directly with SQL.

## Repo layout

```
ServiceTrack/
├── databricks.yml                     # Databricks Asset Bundle (deploy config)
├── resources/
│   └── servicetrack_pipeline.job.yml  # Job/Workflow definition (task graph, schedule)
├── src/
│   ├── common/
│   │   ├── utils.py                   # widgets, table-name helpers, ingestion metadata
│   │   └── transformations.py         # pure, unit-tested transformation functions
│   ├── bronze/
│   │   ├── 01_bronze_customers.py
│   │   ├── 02_bronze_devices.py
│   │   └── 03_bronze_service_jobs.py
│   ├── silver/
│   │   ├── 01_silver_customers.py
│   │   ├── 02_silver_devices.py
│   │   └── 03_silver_service_jobs.py
│   └── gold/
│       ├── 01_gold_repair_turnaround.py
│       ├── 02_gold_technician_performance.py
│       ├── 03_gold_repeat_customers.py
│       └── 04_gold_device_failure_trends.py
├── notebooks/
│   └── 00_run_full_pipeline.py        # interactive end-to-end runner (dev use)
├── tests/
│   ├── conftest.py                    # local SparkSession fixture
│   └── test_transformations.py        # unit tests, no cluster required
├── data/raw/                          # sample CSVs (for local dev / testing only)
├── requirements.txt
└── .gitignore
```

Every notebook file is written in the `# Databricks notebook source` /
`# COMMAND ----------` format, so they open as proper multi-cell notebooks
when imported into a Databricks workspace (via Git folders, `databricks
workspace import`, or Asset Bundle deploy) — no manual reformatting needed.

## Source data

| File | Rows | Grain | Key columns |
|---|---|---|---|
| `customers.csv` | 300 | 1 row per customer | `customer_id`, `customer_name`, `phone_number`, `email`, `city`, `registration_date` |
| `devices.csv` | 43 | 1 row per device model | `device_id`, `brand`, `device_type`, `model_series`, `warranty_months`, `price_range` |
| `service_jobs.csv` | 1,510 | 1 row per repair job | `job_id`, `customer_id`, `device_id`, `issue_type`, `job_status`, `received_date`, `promised_date`, `completed_date`, `technician_id`, `technician_name`, `repair_notes`, `estimated_cost`, `actual_cost` |

### Data quality issues found (and how Silver handles them)

Profiling `service_jobs.csv` up front — rather than guessing — surfaced real
issues, all fixed explicitly in `src/silver/03_silver_service_jobs.py`:

| Issue | Rows | Handling |
|---|---|---|
| Exact duplicate rows (same `job_id` repeated verbatim) | 10 | Dropped, keep one |
| `technician_name` null but `technician_id` present | 75 | Backfilled via a `technician_id → name` lookup built from the file itself |
| `completed_date` null | 370 | Expected — job is `Pending`/`In Progress`; left null rather than invented |
| `actual_cost` null | 447 | Expected — job not yet billed (`Pending`/`In Progress`/`Cancelled`); left null |
| `repair_notes` null | 483 | Left null — free-text field, absence is meaningful, not an error |

Referential integrity (`customer_id` → customers, `device_id` → devices) was
checked and is 100% clean in the sample data, but Silver still computes
`has_valid_customer` / `has_valid_device` flags defensively for future loads
that might not be.

## Gold tables

| Table | Grain | Answers |
|---|---|---|
| `gold_repair_turnaround` | device_type × issue_type × month | How fast do we fix things, and where do we miss SLA? |
| `gold_technician_performance` | technician | Who's fastest, most reliable, and generating the most completed revenue? |
| `gold_repeat_customers` | customer | Who comes back, how often, and how much have they spent? |
| `gold_device_failure_trends` | brand × device_type × issue_type | What fails most, and does warranty/price tier correlate with failure? |

## Setup

### 1. Unity Catalog

Create a catalog, schema, and a Volume to land the raw CSVs:

```sql
CREATE CATALOG IF NOT EXISTS servicetrack;
CREATE SCHEMA IF NOT EXISTS servicetrack.dev;
CREATE VOLUME IF NOT EXISTS servicetrack.dev.raw_data;
```

Upload `customers.csv`, `devices.csv`, `service_jobs.csv` to
`/Volumes/servicetrack/dev/raw_data/`.

### 2. Deploy with Databricks Asset Bundles

```bash
databricks auth login --host https://<your-workspace-instance>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run servicetrack_pipeline -t dev
```

This deploys the notebooks and creates the `servicetrack_pipeline` Job
defined in `resources/servicetrack_pipeline.job.yml`, with Bronze → Silver →
Gold tasks wired up as a dependency graph (not one flat notebook), a nightly
3 AM schedule (created **paused** — unpause once the raw Volume is wired up),
and failure email alerting.

### 3. Or run interactively

Open `notebooks/00_run_full_pipeline.py` in a Databricks workspace, attach a
cluster, set the `catalog` / `schema` / `raw_data_path` widgets, and Run All.

## Local development & testing

The tricky transformation logic (de-dup, technician backfill, turnaround/SLA
math, price-range parsing) lives in `src/common/transformations.py` as plain
functions that take/return DataFrames — no `dbutils`, no widgets, no Unity
Catalog dependency — so it's unit tested locally with plain PySpark:

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Notes / next steps for a production hardening pass

- The Gold notebooks currently do a full `overwrite` on every run, matching
  the "raw CSV is a full snapshot" nature of this sample data. If the source
  ever becomes an append-only daily feed, switch Bronze to
  `trigger(availableOnce)` Auto Loader + `MERGE` in Silver instead.
- Add a `technicians` and `issue_types` dimension table once those become
  independently maintained reference data rather than embedded in
  `service_jobs.csv`.
- Wire the `email_notifications.on_failure` address in
  `resources/servicetrack_pipeline.job.yml` to your team's real alias, and
  unpause the schedule.
