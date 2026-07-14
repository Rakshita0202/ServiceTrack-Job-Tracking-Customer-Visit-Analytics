# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Devices
# MAGIC Raw ingestion of `devices.csv` (the device catalog / dimension data) into
# MAGIC a Delta table with ingestion metadata only -- no cleaning applied yet.
# MAGIC
# MAGIC **Source:** `devices.csv`
# MAGIC **Target:** `<catalog>.<schema>.bronze_devices`

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

devices_schema = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("brand", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("model_series", StringType(), True),
        StructField("warranty_months", IntegerType(), True),
        StructField("price_range", StringType(), True),
    ]
)

# COMMAND ----------

ensure_schema_exists()

source_path = raw_file_path("devices.csv")

raw_df = (
    spark.read.option("header", True)
    .schema(devices_schema)
    .csv(source_path)
)

bronze_df = with_ingestion_metadata(raw_df, source_path)

display(bronze_df)

# COMMAND ----------

target_table = bronze_table("devices")

(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Wrote {bronze_df.count()} rows to {target_table}")

# COMMAND ----------

row_count = spark.table(target_table).count()
assert row_count > 0, f"Bronze devices table is empty: {target_table}"
print(f"Row count check passed: {row_count} rows")
