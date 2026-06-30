# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import uuid

# COMMAND ----------

# ==========================================================
# Read Table Configuration
# ==========================================================

TABLE_CONFIG_PATH = f"{METADATA_PATH}/table_config"
table_config = read_delta(TABLE_CONFIG_PATH)

table_config = (

    table_config

    .filter(

        col("active_flag") == "Y"

    )

)

display(table_config)

# COMMAND ----------

# MAGIC %md
# MAGIC Landing Processing

# COMMAND ----------

# ==========================================================
# Landing Processing
# ==========================================================

batch_id = str(uuid.uuid4())

print(f"Batch ID : {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC Process All Tables

# COMMAND ----------

# ==========================================================
# Process Landing
# ==========================================================

for row in table_config.collect():

    table_name = row["table_name"]

    file_format = row["file_format"]

    source_type = row["source_type"]

    print(f"\nProcessing : {table_name}")

    # ------------------------------------------------------
    # Read Source
    # ------------------------------------------------------

    if source_type == "ADLS":

        df = (

            spark.read

            .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)

            .option("header", True)

            .format(file_format)

            .load(f"{SOURCE_PATH}/{table_name}")

        )

    elif source_type == "AZURE_SQL":

        source_name = row["source_name"]

        df = (

            spark.read

            .format("jdbc")

            .option("url", JDBC_URL)

            .option("dbtable", source_name)

            .option("user", SQL_CONNECTION_PROPERTIES["user"])

            .option("password", SQL_CONNECTION_PROPERTIES["password"])

            .option("driver", SQL_CONNECTION_PROPERTIES["driver"])

            .load()

        )

    elif source_type == "REST_API":

        print("Already available in Landing")

        continue

    # ------------------------------------------------------
    # Add Technical Columns
    # ------------------------------------------------------

    df = (

        df

        .withColumn(

            "BatchID",

            lit(batch_id)

        )

        .withColumn(

            "LandingTimestamp",

            current_timestamp()

        )

    )

    # ------------------------------------------------------
    # Write Landing
    # ------------------------------------------------------

    if file_format.lower() == "csv":

        (

            df.write

            .mode("overwrite")

            .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)

            .option("header", True)

            .csv(f"{LANDING_PATH}/{table_name}")

        )

    elif file_format.lower() == "json":

        (

            df.write

            .mode("overwrite")

            .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)

            .json(f"{LANDING_PATH}/{table_name}")

        )

    else:

        raise Exception(

            f"Unsupported File Format : {file_format}"

        )

    print(f"{table_name} Loaded Successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Validate Landing

# COMMAND ----------

# ==========================================================
# Validation
# ==========================================================

for row in table_config.collect():

    table_name = row["table_name"]

    if row["source_type"] == "API":

        continue

    print(f"\n{table_name}")

    display(

        spark.read

        .option(

            AUTH_OPTION,

            STORAGE_ACCOUNT_KEY

        )

        .option(

            "header",

            True

        )

        .csv(

            f"{LANDING_PATH}/{table_name}"

        )

        .limit(5)

    )