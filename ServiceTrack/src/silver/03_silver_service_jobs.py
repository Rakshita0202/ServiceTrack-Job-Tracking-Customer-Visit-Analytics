import sys, os

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import functions as F
from common.transformations import (
    dedupe_exact_rows,
    keep_latest_per_key,
    backfill_technician_names,
    add_turnaround_fields,
)

bronze_df = spark.table(bronze_table("service_jobs"))
silver_customers = spark.table(silver_table("customers")).select("customer_id")
silver_devices = spark.table(silver_table("devices")).select("device_id")


deduped_df = dedupe_exact_rows(bronze_df)

print(f"Bronze rows: {bronze_df.count()} -> after exact-duplicate removal: {deduped_df.count()}")


latest_df = keep_latest_per_key(deduped_df, key_cols=["job_id"], order_col="_ingest_ts")


backfilled_df = backfill_technician_names(latest_df)

still_missing = backfilled_df.filter(F.col("technician_name").isNull()).count()
print(f"technician_name still null after backfill: {still_missing}")

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

total_rows = silver_df.count()
distinct_jobs = silver_df.select("job_id").distinct().count()
assert total_rows == distinct_jobs, "Duplicate job_id values remain after dedup"

negative_turnaround = silver_df.filter(F.col("turnaround_days") < 0).count()
assert negative_turnaround == 0, "Found jobs completed before they were received"

print(f"Silver service_jobs row count: {total_rows}")



target_table = silver_table("service_jobs")

silver_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {total_rows} rows to {target_table}")
