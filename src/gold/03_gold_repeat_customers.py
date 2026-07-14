# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Repeat Customer Behavior
# MAGIC Answers: *who are our repeat customers, how often do they come back, and
# MAGIC how much have they spent?* Grain: one row per customer.
# MAGIC
# MAGIC **Sources:** `silver_service_jobs`, `silver_customers`
# MAGIC **Target:** `<catalog>.<schema>.gold_repeat_customers`

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

jobs = spark.table(silver_table("service_jobs")).filter(F.col("has_valid_customer"))
customers = spark.table(silver_table("customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average days between visits
# MAGIC For customers with more than one visit, compute the gap between each
# MAGIC job's `received_date` and the customer's previous one, then average it.

# COMMAND ----------

visit_window = Window.partitionBy("customer_id").orderBy("received_date")

gaps_df = jobs.withColumn(
    "prev_received_date", F.lag("received_date").over(visit_window)
).withColumn(
    "days_since_prev_visit", F.datediff(F.col("received_date"), F.col("prev_received_date"))
)

avg_gap_df = gaps_df.groupBy("customer_id").agg(
    F.round(F.avg("days_since_prev_visit"), 1).alias("avg_days_between_visits")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-customer summary

# COMMAND ----------

customer_summary = jobs.groupBy("customer_id").agg(
    F.count("job_id").alias("total_visits"),
    F.min("received_date").alias("first_visit_date"),
    F.max("received_date").alias("last_visit_date"),
    F.countDistinct("device_id").alias("distinct_devices_serviced"),
    F.countDistinct("issue_type").alias("distinct_issue_types"),
    F.round(F.sum(F.when(F.col("is_completed"), F.col("actual_cost"))), 2).alias("total_spend"),
)

gold_df = (
    customer_summary.join(avg_gap_df, on="customer_id", how="left")
    .join(customers.select("customer_id", "customer_name", "city", "registration_date"), on="customer_id", how="left")
    .withColumn("is_repeat_customer", F.col("total_visits") > 1)
    .select(
        "customer_id",
        "customer_name",
        "city",
        "registration_date",
        "total_visits",
        "is_repeat_customer",
        "first_visit_date",
        "last_visit_date",
        "avg_days_between_visits",
        "distinct_devices_serviced",
        "distinct_issue_types",
        "total_spend",
    )
    .orderBy(F.col("total_visits").desc())
)

display(gold_df)

# COMMAND ----------

repeat_pct = gold_df.select(F.round(F.avg(F.col("is_repeat_customer").cast("int")) * 100, 2)).first()[0]
print(f"Share of customers who are repeat visitors: {repeat_pct}%")

# COMMAND ----------

target_table = gold_table("repeat_customers")

gold_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {gold_df.count()} rows to {target_table}")
