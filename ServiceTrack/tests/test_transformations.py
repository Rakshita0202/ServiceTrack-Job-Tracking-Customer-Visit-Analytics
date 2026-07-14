import datetime as dt
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from common.transformations import (
    dedupe_exact_rows,
    keep_latest_per_key,
    backfill_technician_names,
    add_turnaround_fields,
    price_range_bounds,
)


def test_dedupe_exact_rows_removes_only_exact_duplicates(spark):
    df = spark.createDataFrame(
        [
            ("JOB1", "Completed", "2024-01-01"),
            ("JOB1", "Completed", "2024-01-01"),  # exact duplicate
            ("JOB2", "Pending", "2024-01-02"),
        ],
        ["job_id", "job_status", "received_date"],
    )
    result = dedupe_exact_rows(df)
    assert result.count() == 2


def test_keep_latest_per_key_prefers_highest_order_col(spark):
    df = spark.createDataFrame(
        [
            ("JOB1", "Pending", 1),
            ("JOB1", "Completed", 2),
        ],
        ["job_id", "job_status", "_ingest_ts"],
    )
    result = keep_latest_per_key(df, key_cols=["job_id"], order_col="_ingest_ts")
    row = result.collect()[0]
    assert result.count() == 1
    assert row["job_status"] == "Completed"


def test_backfill_technician_names_fills_null_from_same_id(spark):
    df = spark.createDataFrame(
        [
            ("T001", "Rajesh Kumar"),
            ("T001", None),
            ("T002", None),  # no name anywhere for T002 -> stays null
        ],
        ["technician_id", "technician_name"],
    )
    result = backfill_technician_names(df)
    names_by_tech = {row["technician_id"]: row["technician_name"] for row in result.collect()}
    # both T001 rows should end up with "Rajesh Kumar" somewhere; check no
    # null remains for T001
    t001_rows = [r["technician_name"] for r in result.collect() if r["technician_id"] == "T001"]
    assert all(name == "Rajesh Kumar" for name in t001_rows)
    assert names_by_tech["T002"] is None


def test_add_turnaround_fields_completed_on_time(spark):
    df = spark.createDataFrame(
        [
            (
                "Completed",
                dt.date(2024, 1, 1),
                dt.date(2024, 1, 5),
                dt.date(2024, 1, 4),
            )
        ],
        ["job_status", "received_date", "promised_date", "completed_date"],
    )
    result = add_turnaround_fields(df).collect()[0]
    assert result["is_completed"] is True
    assert result["is_cancelled"] is False
    assert result["turnaround_days"] == 3
    assert result["is_overdue"] is False
    assert result["sla_breach_days"] == 0


def test_add_turnaround_fields_completed_late(spark):
    df = spark.createDataFrame(
        [
            (
                "Completed",
                dt.date(2024, 1, 1),
                dt.date(2024, 1, 5),
                dt.date(2024, 1, 8),
            )
        ],
        ["job_status", "received_date", "promised_date", "completed_date"],
    )
    result = add_turnaround_fields(df).collect()[0]
    assert result["is_overdue"] is True
    assert result["sla_breach_days"] == 3


def test_add_turnaround_fields_open_job_has_null_turnaround(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DateType

    schema = StructType(
        [
            StructField("job_status", StringType()),
            StructField("received_date", DateType()),
            StructField("promised_date", DateType()),
            StructField("completed_date", DateType()),
        ]
    )
    df = spark.createDataFrame(
        [
            (
                "In Progress",
                dt.date(2024, 1, 1),
                dt.date(2024, 1, 5),
                None,
            )
        ],
        schema=schema,
    )
    result = add_turnaround_fields(df).collect()[0]
    assert result["turnaround_days"] is None
    assert result["sla_breach_days"] == 0


def test_price_range_bounds_parses_all_tiers(spark):
    df = spark.createDataFrame(
        [
            ("Budget (< 15K)",),
            ("Mid-range (15K-40K)",),
            ("Premium (> 40K)",),
        ],
        ["price_range"],
    )
    price_min, price_max = price_range_bounds(df, "price_range")
    result = df.select(price_min.alias("min"), price_max.alias("max")).collect()
    assert result[0]["min"] == 0 and result[0]["max"] == 15000
    assert result[1]["min"] == 15000 and result[1]["max"] == 40000
    assert result[2]["min"] == 40000 and result[2]["max"] is None
