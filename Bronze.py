# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

# MAGIC %md
# MAGIC Required Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable


import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC BAtch CEll ID

# COMMAND ----------

# ==========================================================
# Generate Batch ID
# ==========================================================

# ----------------------------------------------------------
# Generate one unique Batch ID for the current execution.
#
# All records processed during this notebook execution will
# receive the same Batch ID.
#
# This is useful for:
#   • Audit
#   • Troubleshooting
#   • Tracking ETL batches
# ----------------------------------------------------------

import uuid

batch_id = str(uuid.uuid4())

print(f"Batch ID : {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC Read Metadata

# COMMAND ----------

# ==========================================================
# Read Metadata
# ==========================================================

# ----------------------------------------------------------
# Read table configuration.
#
# The table_config metadata controls the framework.
# Instead of hardcoding table names, folders and formats,
# the notebook dynamically reads them from metadata.
# ----------------------------------------------------------

table_config = (

    read_delta(f"{METADATA_PATH}/table_config")

    .filter(
        col("active_flag") == "Y"
    )

    .orderBy(
        "execution_order"
    )

)

# ----------------------------------------------------------
# Read Technical Configuration.
#
# Technical column names are stored in metadata so they can
# be changed without modifying notebook code.
# ----------------------------------------------------------

technical_config = read_delta(
    f"{METADATA_PATH}/technical_config"
)

technical_dict = {

    row["config_name"]: row["config_value"]

    for row in technical_config.collect()

}

print("Metadata Loaded Successfully")

display(table_config)

display(technical_config)

# COMMAND ----------

# MAGIC %md
# MAGIC Pipeline Run ID & Batch ID

# COMMAND ----------

# ==========================================================
# Runtime Information
# ==========================================================

# ----------------------------------------------------------
# If this notebook is executed from Azure Data Factory,
# ADF will pass the Pipeline Run ID.
#
# While developing in Databricks manually,
# "MANUAL_RUN" will be used.
# ----------------------------------------------------------

dbutils.widgets.text(
    "pipelineRunId",
    "MANUAL_RUN"
)

pipeline_run_id = dbutils.widgets.get(
    "pipelineRunId"
)

# ----------------------------------------------------------
# Generate Batch ID.
#
# One Batch ID is generated for every notebook execution.
# Every record processed in this run will receive
# the same Batch ID.
# ----------------------------------------------------------

batch_id = str(uuid.uuid4())

print(f"Pipeline Run ID : {pipeline_run_id}")

print(f"Batch ID : {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC Landing → Bronze Processing

# COMMAND ----------

# ==========================================================
# Landing to Bronze Processing
# ==========================================================

import re

# ----------------------------------------------------------
# Loop through every active table from table_config.
# ----------------------------------------------------------

for row in table_config.collect():

    # ------------------------------------------------------
    # Read metadata values
    # ------------------------------------------------------

    table_name = row["table_name"]

    source_type = row["source_type"]

    file_format = row["file_format"]

    landing_folder = row["landing_folder"]

    bronze_folder = row["bronze_folder"]

    print("=" * 60)
    print(f"Processing Table : {table_name}")
    print("=" * 60)

    # ------------------------------------------------------
    # Read Landing Data
    # ------------------------------------------------------

    if file_format.lower() == "csv":

        df = (

            spark.read

            .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)

            .option("header", True)

            .option("inferSchema", False)

            .csv(

                f"{LANDING_PATH}/{landing_folder}"

            )

        )

    elif file_format.lower() == "json":

        df = (

            spark.read

            .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)

            .json(

                f"{LANDING_PATH}/{landing_folder}"

            )

        )

        # ----------------------------------------------
        # Convert all business columns to StringType
        # Bronze layer stores raw data as strings.
        # ----------------------------------------------

        for c in df.columns:

            df = df.withColumn(

                c,

                col(c).cast("string")

            )

    else:

        raise Exception(

            f"Unsupported File Format : {file_format}"

        )

    # ------------------------------------------------------
    # Sanitize Column Names
    # ------------------------------------------------------

    for column in df.columns:

        clean_name = re.sub(

            r"[^a-zA-Z0-9_]",

            "_",

            column

        )

        df = df.withColumnRenamed(

            column,

            clean_name

        )

    # ------------------------------------------------------
    # Add Technical Columns
    # ------------------------------------------------------

    df = (

        df

        .withColumn(

            technical_dict["BATCH_ID_COLUMN"],

            lit(batch_id)

        )

        .withColumn(

            technical_dict["ADF_RUN_ID_COLUMN"],

            lit(pipeline_run_id)

        )

        .withColumn(

            technical_dict["INGESTION_TIMESTAMP_COLUMN"],

            current_timestamp()

        )

    )

    # ------------------------------------------------------
    # Generate Record Hash
    #
    # Technical columns are excluded from hashing.
    # ------------------------------------------------------

    technical_columns = [

        technical_dict["BATCH_ID_COLUMN"],

        technical_dict["ADF_RUN_ID_COLUMN"],

        technical_dict["INGESTION_TIMESTAMP_COLUMN"]

    ]

    compare_columns = [

        c

        for c in df.columns

        if c not in technical_columns

    ]

    df = (

        df.withColumn(

            technical_dict["RECORD_HASH_COLUMN"],

            sha2(

                concat_ws(

                    "||",

                    *[

                        coalesce(

                            col(c).cast("string"),

                            lit("")

                        )

                        for c in compare_columns

                    ]

                ),

                256

            )

        )

    )

    # ------------------------------------------------------
    # Write Bronze Layer
    # ------------------------------------------------------

    overwrite_delta(

        df,

        f"{BRONZE_PATH}/{bronze_folder}"

    )

    print(f"{table_name} Bronze Load Completed")

print("=" * 60)
print("Bronze Layer Processing Completed")
print("=" * 60)

# COMMAND ----------

products_df = (
    spark.read
    .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)
    .format("delta")
    .load(f"{BRONZE_PATH}/products")
)

display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Audit Logging

# COMMAND ----------

# ==========================================================o
# Update Audit Log
# ==========================================================

# ----------------------------------------------------------
# Create an empty list to store audit information for every
# processed table.
#
# Instead of writing to the audit log after every table,
# we collect all audit records and write them once at the
# end of the notebook.
# ----------------------------------------------------------

audit_records = []

for row in table_config.collect():

    table_name = row["table_name"]

    bronze_folder = row["bronze_folder"]

    start_time = current_timestamp()

    bronze_df = read_delta(f"{BRONZE_PATH}/{bronze_folder}")

    record_count = bronze_df.count()

    audit_records.append(

        (

            pipeline_run_id,

            batch_id,

            "Bronze",

            table_name,

            record_count,

            "SUCCESS"

        )

    )

audit_schema = StructType([

    StructField("PipelineRunId", StringType()),

    StructField("BatchId", StringType()),

    StructField("Layer", StringType()),

    StructField("TableName", StringType()),

    StructField("RecordCount", LongType()),

    StructField("Status", StringType())

])

audit_df = spark.createDataFrame(

    audit_records,

    audit_schema

)

append_delta(

    audit_df,

    LOG_PATH

)

print("Audit Log Updated Successfully")

# COMMAND ----------

display(audit_df)

# COMMAND ----------

# ==========================================================
# Validate Bronze Layer
# ==========================================================

for row in table_config.collect():

    table_name = row["table_name"]

    bronze_folder = row["bronze_folder"]

    print("=" * 60)

    print(f"{table_name}")

    display(

        read_delta(

            f"{BRONZE_PATH}/{bronze_folder}"

        ).limit(5)

    )