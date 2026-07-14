# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Service Jobs
# MAGIC Raw ingestion of `service_jobs.csv` -- the core fact table of the
# MAGIC pipeline (one row per repair job). Loaded as-is, including duplicates
# MAGIC and nulls; those are handled deliberately in Silver, not hidden here.
# MAGIC
# MAGIC **Source:** `service_jobs.csv`
# MAGIC **Target:** `<catalog>.<schema>.bronze_service_jobs`

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------

service_jobs_schema = StructType(
    [
        StructField("job_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("issue_type", StringType(), True),
        StructField("job_status", StringType(), True),
        StructField("received_date", StringType(), True),
        StructField("promised_date", StringType(), True),
        StructField("completed_date", StringType(), True),
        StructField("technician_id", StringType(), True),
        StructField("technician_name", StringType(), True),
        StructField("repair_notes", StringType(), True),
        StructField("estimated_cost", DoubleType(), True),
        StructField("actual_cost", DoubleType(), True),
    ]
)

# COMMAND ----------

ensure_schema_exists()

source_path = raw_file_path("service_jobs.csv")

raw_df = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .schema(service_jobs_schema)
    .csv(source_path)
)

bronze_df = with_ingestion_metadata(raw_df, source_path)

display(bronze_df)

# COMMAND ----------

target_table = bronze_table("service_jobs")

(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Wrote {bronze_df.count()} rows to {target_table}")

# COMMAND ----------

row_count = spark.table(target_table).count()
assert row_count > 0, f"Bronze service_jobs table is empty: {target_table}"
print(f"Row count check passed: {row_count} rows")
