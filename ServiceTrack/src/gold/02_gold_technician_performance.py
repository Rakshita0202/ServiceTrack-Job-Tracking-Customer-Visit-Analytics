from pyspark.sql import functions as F

jobs = spark.table(silver_table("service_jobs")).filter(F.col("technician_id").isNotNull())


gold_df = (
    jobs.groupBy("technician_id", "technician_name")
    .agg(
        F.count("job_id").alias("total_jobs_assigned"),
        F.sum(F.col("is_completed").cast("int")).alias("jobs_completed"),
        F.sum(F.col("is_cancelled").cast("int")).alias("jobs_cancelled"),
        F.round(F.avg(F.when(F.col("is_completed"), F.col("turnaround_days"))), 2).alias(
            "avg_turnaround_days"
        ),
        F.sum(F.col("is_overdue").cast("int")).alias("jobs_overdue"),
        F.round(F.sum(F.when(F.col("is_completed"), F.col("actual_cost"))), 2).alias(
            "total_revenue_generated"
        ),
    )
    .withColumn(
        "completion_rate_pct",
        F.round(F.col("jobs_completed") / F.col("total_jobs_assigned") * 100, 2),
    )
    .withColumn(
        "on_time_rate_pct",
        F.round(
            (F.col("jobs_completed") - F.col("jobs_overdue")) / F.greatest(F.col("jobs_completed"), F.lit(1)) * 100,
            2,
        ),
    )
    .orderBy(F.col("jobs_completed").desc())
)

display(gold_df)

target_table = gold_table("technician_performance")

gold_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {gold_df.count()} rows to {target_table}")
