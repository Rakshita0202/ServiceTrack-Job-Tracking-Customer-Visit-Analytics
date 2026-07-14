from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "servicetrack", "Unity Catalog catalog name")
dbutils.widgets.text("schema", "dev", "Schema / environment (dev, staging, prod)")
dbutils.widgets.text(
    "raw_data_path",
    "/Volumes/servicetrack/dev/raw_data",
    "Volume path where raw CSV files land",
)

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW_DATA_PATH = dbutils.widgets.get("raw_data_path")


def bronze_table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.bronze_{name}"


def silver_table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.silver_{name}"


def gold_table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.gold_{name}"


def raw_file_path(filename: str) -> str:
    return f"{RAW_DATA_PATH.rstrip('/')}/{filename}"


def with_ingestion_metadata(df: DataFrame, source_file: str) -> DataFrame:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ensure_schema_exists() -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
