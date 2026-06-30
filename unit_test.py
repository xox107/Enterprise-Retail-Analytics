# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable

BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"

test_results = []

# COMMAND ----------

def run_test(test_name, condition):

    status = "PASS" if condition else "FAIL"

    test_results.append((test_name, status))

    print(f"{test_name} : {status}")

# COMMAND ----------

Bronze Data Testing

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Silver Data Test

# COMMAND ----------

try:

    silver_tables = [
        "orders",
        "products",
        "customers",
        "exchange_rates"
    ]

    for table in silver_tables:

        df = spark.read.format("delta").load(f"{SILVER_PATH}/{table}")

        run_test(
            f"{table} Silver Exists",
            df.count() > 0
        )

except:

    run_test("Bronze Layer", False)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Data Test

# COMMAND ----------

try:

    gold_tables = [
        "customer_dim",
        "product_dim",
        "exchange_rate_dim",
        "sales_fact"
    ]

    for table in gold_tables:

        df = spark.read.format("delta").load(f"{GOLD_PATH}/{table}")

        run_test(
            f"{table} Gold Exists",
            df.count() > 0
        )

except:

    run_test("Gold Layer", False)

# COMMAND ----------

# MAGIC %md
# MAGIC Rejects Data Test

# COMMAND ----------

try:

    reject_df = spark.read.format("delta").load(f"{SILVER_PATH}/reject_records")

    run_test(
        "Reject Table Exists",
        reject_df is not None
    )

except:

    run_test("Reject Table Exists", False)

# COMMAND ----------

# MAGIC %md
# MAGIC SCD Test

# COMMAND ----------

try:

    customer_dim = spark.read.format("delta").load(f"{GOLD_PATH}/customer_dim")

    current_count = customer_dim.filter(
        col("IsCurrent") == True
    ).count()

    run_test(
        "SCD Current Records",
        current_count > 0
    )

except:

    run_test("SCD", False)

# COMMAND ----------

# MAGIC %md
# MAGIC Audit Test

# COMMAND ----------

try:

    audit_df = spark.read.format("delta").load(f"{base_path}/audit")

    run_test(
        "Audit Table",
        audit_df.count() > 0
    )

except:

    run_test("Audit", False)

# COMMAND ----------

result_df = spark.createDataFrame(
    test_results,
    ["Test Name", "Status"]
)

display(result_df)