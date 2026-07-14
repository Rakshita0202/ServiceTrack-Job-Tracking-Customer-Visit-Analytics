

dbutils.widgets.text("catalog", "servicetrack", "Unity Catalog catalog name")
dbutils.widgets.text("schema", "dev", "Schema / environment (dev, staging, prod)")
dbutils.widgets.text(
    "raw_data_path",
    "/Volumes/servicetrack/dev/raw_data",
    "Volume path where raw CSV files land",
)

params = {
    "catalog": dbutils.widgets.get("catalog"),
    "schema": dbutils.widgets.get("schema"),
    "raw_data_path": dbutils.widgets.get("raw_data_path"),
}
params


dbutils.notebook.run("../src/bronze/01_bronze_customers", 0, params)
dbutils.notebook.run("../src/bronze/02_bronze_devices", 0, params)
dbutils.notebook.run("../src/bronze/03_bronze_service_jobs", 0, params)


dbutils.notebook.run("../src/silver/01_silver_customers", 0, params)
dbutils.notebook.run("../src/silver/02_silver_devices", 0, params)
dbutils.notebook.run("../src/silver/03_silver_service_jobs", 0, params)


dbutils.notebook.run("../src/gold/01_gold_repair_turnaround", 0, params)
dbutils.notebook.run("../src/gold/02_gold_technician_performance", 0, params)
dbutils.notebook.run("../src/gold/03_gold_repeat_customers", 0, params)
dbutils.notebook.run("../src/gold/04_gold_device_failure_trends", 0, params)


print("ServiceTrack pipeline completed: Bronze -> Silver -> Gold")
