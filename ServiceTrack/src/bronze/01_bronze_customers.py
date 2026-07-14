from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

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

target_table = bronze_table("customers")

(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Wrote {bronze_df.count()} rows to {target_table}")


row_count = spark.table(target_table).count()
assert row_count > 0, f"Bronze customers table is empty: {target_table}"
print(f"Row count check passed: {row_count} rows")
