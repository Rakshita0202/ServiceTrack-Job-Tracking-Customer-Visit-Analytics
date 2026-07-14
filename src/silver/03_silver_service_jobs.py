# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Service Jobs
# MAGIC This is the most involved transformation in the pipeline. Profiling the
# MAGIC raw file surfaced several real issues that are fixed here, deliberately
# MAGIC and traceably, rather than silently:
# MAGIC
# MAGIC | Issue | Rows affected | Fix |
# MAGIC |---|---|---|
# MAGIC | Exact duplicate rows (same `job_id` repeated) | 10 | De-duplicate, keep one |
# MAGIC | `technician_name` null but `technician_id` present | 75 | Backfill from a technician lookup built from the file itself |
# MAGIC | `completed_date` null | 370 | Expected for `Pending` / `In Progress` jobs -- left null, not invented |
# MAGIC | `actual_cost` null | 447 | Expected for jobs not yet billed (`Pending`/`In Progress`/`Cancelled`) -- left null |
# MAGIC | `repair_notes` null | 483 | Left null (free-text field, absence is meaningful) |
# MAGIC
# MAGIC It also derives the fields the Gold layer needs: `turnaround_days`,
# MAGIC `is_completed`, `is_overdue` (missed the promised date), and
# MAGIC `sla_breach_days`.
# MAGIC
# MAGIC **Source:** `<catalog>.<schema>.bronze_service_jobs`
# MAGIC **Target:** `<catalog>.<schema>.silver_service_jobs`

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

# MAGIC %md
# MAGIC The de-dup, technician-backfill, and turnaround-derivation logic below
# MAGIC lives in `src/common/transformations.py` as plain, unit-testable
# MAGIC functions (see `tests/test_transformations.py`) rather than being
# MAGIC written inline here.

# COMMAND ----------

import sys, os

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import functions as F
from common.transformations import (
    dedupe_exact_rows,
    keep_latest_per_key,
    backfill_technician_names,
    add_turnaround_fields,
)

# COMMAND ----------

bronze_df = spark.table(bronze_table("service_jobs"))
silver_customers = spark.table(silver_table("customers")).select("customer_id")
silver_devices = spark.table(silver_table("devices")).select("device_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: De-duplicate
# MAGIC Drop exact duplicate rows first (same `job_id`, same everything) --
# MAGIC these are re-sends of the same record, not genuinely distinct jobs.

# COMMAND ----------

deduped_df = dedupe_exact_rows(bronze_df)

print(f"Bronze rows: {bronze_df.count()} -> after exact-duplicate removal: {deduped_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Keep the latest version of each `job_id`
# MAGIC If the same `job_id` legitimately appears with different field values
# MAGIC across loads (e.g. status changed from "In Progress" to "Completed"),
# MAGIC keep the most recently ingested version.

# COMMAND ----------

latest_df = keep_latest_per_key(deduped_df, key_cols=["job_id"], order_col="_ingest_ts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Backfill missing technician names
# MAGIC Build a `technician_id -> technician_name` lookup from every row where
# MAGIC the name IS present, then use it to fill the 75 rows where the name is
# MAGIC missing but the id isn't. This avoids losing technician-level
# MAGIC attribution in the Gold "technician performance" mart.

# COMMAND ----------

backfilled_df = backfill_technician_names(latest_df)

still_missing = backfilled_df.filter(F.col("technician_name").isNull()).count()
print(f"technician_name still null after backfill: {still_missing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Type casting & derived fields
# MAGIC - Cast the three date columns to real `date` types
# MAGIC - `turnaround_days`: calendar days from received to completed (null if
# MAGIC   not yet completed)
# MAGIC - `is_completed` / `is_cancelled`: convenience boolean flags
# MAGIC - `is_overdue`: completed later than promised (or, for open jobs, already
# MAGIC   past the promised date as of today)
# MAGIC - `sla_breach_days`: how many days late (0 if on time / not yet due)

# COMMAND ----------

typed_df = (
    backfilled_df.withColumn("received_date", F.to_date("received_date", "yyyy-MM-dd"))
    .withColumn("promised_date", F.to_date("promised_date", "yyyy-MM-dd"))
    .withColumn("completed_date", F.to_date("completed_date", "yyyy-MM-dd"))
    .withColumn("job_status", F.trim(F.initcap(F.col("job_status"))))
    .withColumn("issue_type", F.trim(F.col("issue_type")))
    .withColumn("technician_id", F.trim(F.col("technician_id")))
    .withColumn("technician_name", F.trim(F.col("technician_name")))
)

enriched_df = add_turnaround_fields(typed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Referential integrity check
# MAGIC Every `customer_id` / `device_id` on a job should exist in the
# MAGIC corresponding Silver dimension table. Flag (don't silently drop) any
# MAGIC orphaned rows so they're visible to whoever owns the pipeline.

# COMMAND ----------

silver_df = (
    enriched_df.join(
        silver_customers.withColumnRenamed("customer_id", "_valid_customer_id"),
        enriched_df.customer_id == F.col("_valid_customer_id"),
        "left",
    )
    .join(
        silver_devices.withColumnRenamed("device_id", "_valid_device_id"),
        enriched_df.device_id == F.col("_valid_device_id"),
        "left",
    )
    .withColumn("has_valid_customer", F.col("_valid_customer_id").isNotNull())
    .withColumn("has_valid_device", F.col("_valid_device_id").isNotNull())
    .drop("_valid_customer_id", "_valid_device_id")
    .withColumn("_silver_updated_ts", F.current_timestamp())
)

orphaned_customer = silver_df.filter(~F.col("has_valid_customer")).count()
orphaned_device = silver_df.filter(~F.col("has_valid_device")).count()
print(f"Jobs referencing an unknown customer_id: {orphaned_customer}")
print(f"Jobs referencing an unknown device_id: {orphaned_device}")

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks

# COMMAND ----------

total_rows = silver_df.count()
distinct_jobs = silver_df.select("job_id").distinct().count()
assert total_rows == distinct_jobs, "Duplicate job_id values remain after dedup"

negative_turnaround = silver_df.filter(F.col("turnaround_days") < 0).count()
assert negative_turnaround == 0, "Found jobs completed before they were received"

print(f"Silver service_jobs row count: {total_rows}")

# COMMAND ----------

target_table = silver_table("service_jobs")

silver_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {total_rows} rows to {target_table}")
