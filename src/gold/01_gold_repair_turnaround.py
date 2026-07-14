# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Repair Turnaround Analytics
# MAGIC Answers: *how fast do we actually fix things, and where are we breaking
# MAGIC our promised SLA?* Grain: one row per `device_type` x `issue_type` x
# MAGIC completion month.
# MAGIC
# MAGIC **Sources:** `silver_service_jobs`, `silver_devices`
# MAGIC **Target:** `<catalog>.<schema>.gold_repair_turnaround`

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

jobs = spark.table(silver_table("service_jobs")).filter(F.col("is_completed"))
devices = spark.table(silver_table("devices"))

# COMMAND ----------

enriched = jobs.join(devices, on="device_id", how="left").withColumn(
    "completion_month", F.date_trunc("month", F.col("completed_date"))
)

# COMMAND ----------

gold_df = (
    enriched.groupBy("completion_month", "device_type", "issue_type")
    .agg(
        F.count("job_id").alias("completed_jobs"),
        F.round(F.avg("turnaround_days"), 2).alias("avg_turnaround_days"),
        F.expr("percentile_approx(turnaround_days, 0.5)").alias("median_turnaround_days"),
        F.max("turnaround_days").alias("max_turnaround_days"),
        F.sum(F.col("is_overdue").cast("int")).alias("sla_breaches"),
        F.round(F.avg(F.col("is_overdue").cast("int")) * 100, 2).alias("sla_breach_rate_pct"),
        F.round(F.avg("actual_cost"), 2).alias("avg_actual_cost"),
    )
    .orderBy("completion_month", "device_type", "issue_type")
)

display(gold_df)

# COMMAND ----------

target_table = gold_table("repair_turnaround")

gold_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {gold_df.count()} rows to {target_table}")
