# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Device Failure Trends
# MAGIC Answers: *which brands/device types/issues fail most, and does that
# MAGIC correlate with warranty length or price tier?* Grain: one row per
# MAGIC `brand` x `device_type` x `issue_type`.
# MAGIC
# MAGIC **Sources:** `silver_service_jobs`, `silver_devices`
# MAGIC **Target:** `<catalog>.<schema>.gold_device_failure_trends`

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

jobs = spark.table(silver_table("service_jobs")).filter(F.col("has_valid_device"))
devices = spark.table(silver_table("devices"))

# COMMAND ----------

enriched = jobs.join(devices, on="device_id", how="left")

# COMMAND ----------

failure_summary = enriched.groupBy(
    "brand", "device_type", "issue_type", "warranty_months", "price_tier"
).agg(
    F.count("job_id").alias("failure_count"),
    F.countDistinct("device_id").alias("distinct_units_affected"),
    F.round(F.avg(F.when(F.col("is_completed"), F.col("turnaround_days"))), 2).alias(
        "avg_turnaround_days"
    ),
    F.round(F.avg(F.when(F.col("is_completed"), F.col("actual_cost"))), 2).alias(
        "avg_repair_cost"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rank issue types within each brand/device_type
# MAGIC Makes it trivial to answer "what's the #1 failure mode for Samsung
# MAGIC laptops?" directly from the table without re-aggregating.

# COMMAND ----------

from pyspark.sql.window import Window

rank_window = Window.partitionBy("brand", "device_type").orderBy(F.col("failure_count").desc())

gold_df = failure_summary.withColumn(
    "failure_rank_within_device", F.rank().over(rank_window)
).orderBy(F.col("failure_count").desc())

display(gold_df)

# COMMAND ----------

target_table = gold_table("device_failure_trends")

gold_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {gold_df.count()} rows to {target_table}")
