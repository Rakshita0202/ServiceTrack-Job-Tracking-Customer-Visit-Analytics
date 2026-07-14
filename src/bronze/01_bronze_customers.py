# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Customers
# MAGIC Raw ingestion of `customers.csv` into a Delta table with no business
# MAGIC transformation applied -- only ingestion metadata is added. This gives us
# MAGIC an auditable, replayable copy of exactly what landed in the source file.
# MAGIC
# MAGIC **Source:** `customers.csv`
# MAGIC **Target:** `<catalog>.<schema>.bronze_customers`

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicit schema
# MAGIC We define the schema explicitly rather than using `inferSchema`. This
# MAGIC makes Bronze ingestion deterministic and fast, and means a malformed
# MAGIC upstream file fails loudly instead of silently guessing types.

# COMMAND ----------

customers_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("registration_date", StringType(), True),
    ]
)

# COMMAND ----------

ensure_schema_exists()

source_path = raw_file_path("customers.csv")

raw_df = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .schema(customers_schema)
    .csv(source_path)
)

bronze_df = with_ingestion_metadata(raw_df, source_path)

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta
# MAGIC `customers.csv` is a full snapshot on every drop (not an append feed), so
# MAGIC Bronze does a full overwrite. Schema is allowed to evolve (new columns
# MAGIC appended upstream won't break the load).

# COMMAND ----------

target_table = bronze_table("customers")

(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Wrote {bronze_df.count()} rows to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic row-count sanity check
# MAGIC Fails the job (raises) if the file was empty or truncated -- catches
# MAGIC obvious upstream extraction failures before bad data flows to Silver.

# COMMAND ----------

row_count = spark.table(target_table).count()
assert row_count > 0, f"Bronze customers table is empty: {target_table}"
print(f"Row count check passed: {row_count} rows")
