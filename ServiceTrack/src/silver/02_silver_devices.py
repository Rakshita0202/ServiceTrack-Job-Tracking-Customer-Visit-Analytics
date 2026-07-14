import sys, os

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.transformations import price_range_bounds

bronze_df = spark.table(bronze_table("devices"))


price_min_col, price_max_col = price_range_bounds(bronze_df, "price_range")

cleaned_df = (
    bronze_df.withColumn("device_id", F.trim(F.col("device_id")))
    .withColumn("brand", F.trim(F.col("brand")))
    .withColumn("device_type", F.trim(F.col("device_type")))
    .withColumn("model_series", F.trim(F.col("model_series")))
    .withColumn("price_tier", F.trim(F.regexp_extract(F.col("price_range"), r"^(\w[\w\-]*)", 1)))
    .withColumn("price_range_min", price_min_col)
    .withColumn("price_range_max", price_max_col)
    .filter(F.col("device_id").isNotNull())
)


dedup_window = Window.partitionBy("device_id").orderBy(F.col("_ingest_ts").desc())

silver_df = (
    cleaned_df.withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    .withColumn("_silver_updated_ts", F.current_timestamp())
)

display(silver_df)

total_rows = silver_df.count()
distinct_ids = silver_df.select("device_id").distinct().count()
assert total_rows == distinct_ids, "Duplicate device_id values remain after dedup"
print(f"Silver devices row count: {total_rows}")


target_table = silver_table("devices")

silver_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {total_rows} rows to {target_table}")
