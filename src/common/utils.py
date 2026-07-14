# Databricks notebook source
# MAGIC %md
# MAGIC # ServiceTrack Common Utilities
# MAGIC Shared helpers used across the Bronze, Silver, and Gold layers:
# MAGIC - Widget/parameter handling (catalog, schema, raw data path)
# MAGIC - Table naming helpers
# MAGIC - Ingestion metadata helper
# MAGIC
# MAGIC Import this from other notebooks with:
# MAGIC ```python
# MAGIC %run ../common/utils
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime, timezone

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets
# MAGIC Every notebook in the pipeline accepts the same three parameters so the
# MAGIC whole pipeline can be pointed at dev / staging / prod with no code changes.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table naming helpers
# MAGIC Enforces the `bronze_<table>`, `silver_<table>`, `gold_<table>` naming
# MAGIC convention inside a single schema (simplest to manage for a project this
# MAGIC size). Swap for three separate schemas (`bronze`, `silver`, `gold`) if you
# MAGIC prefer stronger layer isolation -- just change these helpers.

# COMMAND ----------


def bronze_table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.bronze_{name}"


def silver_table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.silver_{name}"


def gold_table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.gold_{name}"


def raw_file_path(filename: str) -> str:
    return f"{RAW_DATA_PATH.rstrip('/')}/{filename}"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion metadata
# MAGIC Every Bronze table gets `_ingest_ts`, `_source_file`, and `_batch_id` so
# MAGIC downstream layers (and humans debugging at 2am) can always trace a row
# MAGIC back to the file and run that produced it.

# COMMAND ----------


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
