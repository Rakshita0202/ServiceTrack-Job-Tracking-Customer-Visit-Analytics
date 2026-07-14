from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_df = spark.table(bronze_table("customers"))

cleaned_df = (
    bronze_df.withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("phone_number", F.trim(F.col("phone_number").cast("string")))
    .withColumn("email", F.trim(F.lower(F.col("email"))))
    .withColumn("city", F.trim(F.initcap(F.col("city"))))
    .withColumn("registration_date", F.to_date(F.col("registration_date"), "yyyy-MM-dd"))
    .filter(F.col("customer_id").isNotNull())
)


dedup_window = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())

silver_df = (
    cleaned_df.withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    .withColumn("_silver_updated_ts", F.current_timestamp())
)

display(silver_df)



total_rows = silver_df.count()
distinct_ids = silver_df.select("customer_id").distinct().count()
assert total_rows == distinct_ids, "Duplicate customer_id values remain after dedup"

null_email = silver_df.filter(F.col("email").isNull()).count()
print(f"Rows with null email (allowed, informational only): {null_email}")

print(f"Silver customers row count: {total_rows}")


target_table = silver_table("customers")

silver_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {total_rows} rows to {target_table}")
