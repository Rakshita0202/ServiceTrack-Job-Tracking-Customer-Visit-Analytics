"""
Pure PySpark transformation functions used by the Silver and Gold notebooks.

Pulled out of the notebooks on purpose: functions here take/return DataFrames
and have no dependency on `dbutils`, widgets, or table names, so they can be
unit tested locally (see tests/) without a Databricks cluster or Unity
Catalog. Notebooks import this module and wire it up to real tables/widgets.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def dedupe_exact_rows(df: DataFrame, ignore_prefix: str = "_") -> DataFrame:
    """Drop exact duplicate rows, ignoring metadata columns (e.g. `_ingest_ts`)."""
    business_cols = [c for c in df.columns if not c.startswith(ignore_prefix)]
    return df.dropDuplicates(business_cols)


def keep_latest_per_key(df: DataFrame, key_cols, order_col: str) -> DataFrame:
    """Keep one row per `key_cols`, preferring the highest `order_col`."""
    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def backfill_technician_names(df: DataFrame) -> DataFrame:
    """Fill null `technician_name` using a lookup built from non-null rows
    for the same `technician_id` within the same DataFrame."""
    lookup = (
        df.filter(F.col("technician_name").isNotNull())
        .groupBy("technician_id")
        .agg(F.first("technician_name", ignorenulls=True).alias("technician_name_lookup"))
    )
    return (
        df.join(lookup, on="technician_id", how="left")
        .withColumn(
            "technician_name",
            F.coalesce(F.col("technician_name"), F.col("technician_name_lookup")),
        )
        .drop("technician_name_lookup")
    )


def add_turnaround_fields(df: DataFrame) -> DataFrame:
    """Add is_completed, is_cancelled, turnaround_days, is_overdue,
    sla_breach_days derived from job_status/received_date/promised_date/
    completed_date. Assumes those columns are already `date` typed."""
    return (
        df.withColumn("is_completed", F.col("job_status") == F.lit("Completed"))
        .withColumn("is_cancelled", F.col("job_status") == F.lit("Cancelled"))
        .withColumn(
            "turnaround_days",
            F.when(
                F.col("completed_date").isNotNull(),
                F.datediff(F.col("completed_date"), F.col("received_date")),
            ),
        )
        .withColumn(
            "is_overdue",
            F.when(
                F.col("completed_date").isNotNull(),
                F.col("completed_date") > F.col("promised_date"),
            ).when(
                F.col("completed_date").isNull() & (F.col("job_status") != "Cancelled"),
                F.current_date() > F.col("promised_date"),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "sla_breach_days",
            F.when(
                F.col("completed_date").isNotNull(),
                F.greatest(F.datediff(F.col("completed_date"), F.col("promised_date")), F.lit(0)),
            ).otherwise(F.lit(0)),
        )
    )


def price_range_bounds(df: DataFrame, col_name: str = "price_range"):
    """Return (price_min_col, price_max_col) Column expressions parsed from
    free-text values like 'Budget (< 15K)', 'Mid-range (15K-40K)',
    'Premium (> 40K)'."""
    col = F.col(col_name)
    is_budget = col.contains("Budget")
    is_premium = col.contains("Premium")
    price_min = F.when(is_budget, F.lit(0)).when(is_premium, F.lit(40000)).otherwise(F.lit(15000))
    price_max = F.when(is_budget, F.lit(15000)).when(is_premium, F.lit(None)).otherwise(F.lit(40000))
    return price_min, price_max
