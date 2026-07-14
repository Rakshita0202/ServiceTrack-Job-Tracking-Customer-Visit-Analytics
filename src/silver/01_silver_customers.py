# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Customers
# MAGIC Cleans and standardizes Bronze customer data:
# MAGIC - Trims/normalizes text fields, lowercases email
# MAGIC - Casts `registration_date` to a real `date` type
# MAGIC - De-duplicates on `customer_id` (keeps the most recently ingested row)
# MAGIC - Drops rows with a null business key (`customer_id`)
# MAGIC
# MAGIC **Source:** `<catalog>.<schema>.bronze_customers`
# MAGIC **Target:** `<catalog>.<schema>.silver_customers`

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

bronze_df = spark.table(bronze_table("customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean & standardize

# COMMAND ----------

cleaned_df = (
    bronze_df.withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("phone_number", F.trim(F.col("phone_number").cast("string")))
    .withColumn("email", F.trim(F.lower(F.col("email"))))
    .withColumn("city", F.trim(F.initcap(F.col("city"))))
    .withColumn("registration_date", F.to_date(F.col("registration_date"), "yyyy-MM-dd"))
    .filter(F.col("customer_id").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## De-duplicate on business key
# MAGIC Keeps one row per `customer_id`, preferring the latest `_ingest_ts` if
# MAGIC the same customer ever appears more than once in a load.

# COMMAND ----------

dedup_window = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())

silver_df = (
    cleaned_df.withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    .withColumn("_silver_updated_ts", F.current_timestamp())
)

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks
# MAGIC Hard-fail the notebook (and therefore the job) if these invariants are
# MAGIC violated -- a bad Silver customers table would silently corrupt every
# MAGIC downstream Gold metric that joins on `customer_id`.

# COMMAND ----------

total_rows = silver_df.count()
distinct_ids = silver_df.select("customer_id").distinct().count()
assert total_rows == distinct_ids, "Duplicate customer_id values remain after dedup"

null_email = silver_df.filter(F.col("email").isNull()).count()
print(f"Rows with null email (allowed, informational only): {null_email}")

print(f"Silver customers row count: {total_rows}")

# COMMAND ----------

target_table = silver_table("customers")

silver_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {total_rows} rows to {target_table}")
