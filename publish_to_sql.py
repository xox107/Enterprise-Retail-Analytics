# Databricks notebook source
import pyspark.sql.functions as F

STORAGE_ACCOUNT_NAME = "retailcapestone"
STORAGE_ACCOUNT_KEY  = "cK765/4hPsBBi0sD+xR8Xzmk/ccIC7FfsaBzgPDjbrPxci4+3Ejn8TSRW9nprM+hy2EglNDgOf6f+AStFxZgdw=="
CONTAINER_NAME       = "medallion"
AUTH_OPTION          = f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
BASE_PATH            = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

SQL_SERVER   = "retailserver001.database.windows.net"
SQL_DATABASE = "retail-cp"
SQL_USERNAME = "adminthiru"
SQL_PASSWORD = "Thiru@1234"

def get_path(layer, table):
    return f"{BASE_PATH}/{layer}/{table}"

def read_delta(path):
    return (
        spark.read
        .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)
        .format("delta")
        .load(path)
    )

# Read publish_config
publish_config_df = read_delta(
    get_path("metadata", "publish_config")
).filter(F.col("enabled") == True)

publish_tables = publish_config_df.collect()

# Publish each table
for row in publish_tables:
    print(f"Publishing {row.table_name} → {row.target_table}")
    df = read_delta(get_path("gold", row.table_name))
    (
        df.write
        .format("sqlserver")
        .option("host", SQL_SERVER)
        .option("port", "1433")
        .option("database", SQL_DATABASE)
        .option("user", SQL_USERNAME)
        .option("password", SQL_PASSWORD)
        .option("dbtable", row.target_table)
        .option("encrypt", "true")
        .option("trustServerCertificate", "true")
        .mode("overwrite")
        .save()
    )
    print(f"  {row.target_table} → {df.count()} rows ✅")

print("All tables published to SQL ✅")