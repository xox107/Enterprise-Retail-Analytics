# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

# MAGIC %md
# MAGIC import libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

from delta.tables import DeltaTable

import uuid


# COMMAND ----------

# MAGIC %md
# MAGIC Read Metadata

# COMMAND ----------

# ==========================================================
# Read Metadata
# ==========================================================

# ----------------------------------------------------------
# Read Table Configuration
#
# Used to determine:
# • Bronze Folder
# • Silver Folder
# • Load Type
# • Primary Key
# • SCD Type
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
# Read Datatype Configuration
#
# Used for datatype casting.
# Example:
# CustomerID -> Integer
# Price -> Double
# OrderDate -> Date
# ----------------------------------------------------------

datatype_config = read_delta(
    f"{METADATA_PATH}/datatype_config"
)

# ----------------------------------------------------------
# Read Data Quality Configuration
#
# Business validation rules are stored here.
# Examples:
# • Quantity > 0
# • Price > 0
# • Email should not be null
# ----------------------------------------------------------

dq_config = read_delta(
    f"{METADATA_PATH}/dq_config"
)

# ----------------------------------------------------------
# Read Technical Configuration
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

# COMMAND ----------

# MAGIC %md
# MAGIC Runtime Information (Pipeline ID and BAtch ID)

# COMMAND ----------

# ==========================================================
# Runtime Information
# ==========================================================

# ----------------------------------------------------------
# Pipeline Run ID
# ----------------------------------------------------------

dbutils.widgets.text(
    "pipelineRunId",
    "MANUAL_RUN"
)

pipeline_run_id = dbutils.widgets.get(
    "pipelineRunId"
)

# ----------------------------------------------------------
# Generate Batch ID
# ----------------------------------------------------------

batch_id = str(uuid.uuid4())

print(f"Pipeline Run ID : {pipeline_run_id}")

print(f"Batch ID : {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze → Silver Processing

# COMMAND ----------

# ==========================================================
# Bronze to Silver Processing
# ==========================================================

# ----------------------------------------------------------
# Column Name Sanitization Function
#
# This function replicates the column name sanitization
# that was applied in the Bronze layer.
# ----------------------------------------------------------

import re

def sanitize_column_name(column_name):
    return re.sub(r"[^a-zA-Z0-9_]", "_", column_name)

# ----------------------------------------------------------
# Process each active table from the metadata.
# ----------------------------------------------------------

for row in table_config.collect():

    table_name = row["table_name"]

    bronze_folder = row["bronze_folder"]

    silver_folder = row["silver_folder"]

    primary_key = row["primary_key"]

    print("=" * 60)
    print(f"Processing : {table_name}")
    print("=" * 60)

    # ------------------------------------------------------
    # Read Bronze Table
    # ------------------------------------------------------

    df = read_delta(f"{BRONZE_PATH}/{bronze_folder}")

    # ------------------------------------------------------
    # Read datatype configuration for current table
    # ------------------------------------------------------

    datatype_rules = (

        datatype_config

        .filter(
            col("table_name") == table_name
        )

    )

    # ------------------------------------------------------
    # Apply datatype conversions
    # ------------------------------------------------------

    for rule in datatype_rules.collect():

        column_name = sanitize_column_name(rule["column_name"])

        target_datatype = rule["target_datatype"]

        # Skip datatype conversion if column doesn't exist
        if column_name not in df.columns:
            print(f"Warning: Column '{column_name}' not found in {table_name}, skipping datatype conversion")
            continue

        df = df.withColumn(

            column_name,

            col(column_name).cast(target_datatype)

        )

    # ------------------------------------------------------
    # Read DQ rules for current table
    # ------------------------------------------------------

    dq_rules = (

        dq_config

        .filter(
            col("table_name") == table_name
        )

    )

    # ------------------------------------------------------
    # Initialize Validation Status
    # ------------------------------------------------------

    df = df.withColumn(

        "_ValidationStatus",

        lit(True)

    )

    # ------------------------------------------------------
    # Apply Business Data Quality Rules
    # ------------------------------------------------------

    for rule in dq_rules.collect():

        column_name = sanitize_column_name(rule["column_name"])

        validation_rule = rule["validation_rule"]

        # Skip validation if column doesn't exist in DataFrame
        if column_name not in df.columns:
            print(f"Warning: Column '{column_name}' not found in {table_name}, skipping validation")
            continue

        # ------------------------------------------
        # NOT NULL
        # ------------------------------------------

        if validation_rule == "NOT_NULL":

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    col(column_name).isNull(),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # NOT EMPTY
        # ------------------------------------------

        elif validation_rule == "NOT_EMPTY":

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    trim(col(column_name)) == "",
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Positive Number
        # ------------------------------------------

        elif validation_rule == "POSITIVE_NUMBER":

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    col(column_name) <= 0,
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Email Validation
        # ------------------------------------------

        elif validation_rule == "VALID_EMAIL":

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    ~col(column_name).rlike(
                        r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                    ),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Phone Number Validation
        # ------------------------------------------

        elif validation_rule == "VALID_PHONE":

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    ~col(column_name).rlike(r'^\d{10}$'),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Country Validation
        # ------------------------------------------

        elif validation_rule == "VALID_COUNTRY":

            valid_countries = [

                "India",
                "USA",
                "Canada",
                "UK",
                "Australia"

            ]

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    ~col(column_name).isin(valid_countries),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Discount Validation
        # Discount <= UnitPrice
        # ------------------------------------------

        elif validation_rule == "VALID_DISCOUNT":

            # Skip if required columns don't exist
            if "Discount" not in df.columns or "UnitPrice" not in df.columns:
                print(f"Warning: Discount or UnitPrice column not found in {table_name}, skipping VALID_DISCOUNT")
                continue

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    col("Discount") > col("UnitPrice"),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Order Total Validation
        #
        # TotalAmount =
        # Quantity * UnitPrice - Discount
        # ------------------------------------------

        elif validation_rule == "VALID_ORDER_TOTAL":

            # Skip if required columns don't exist
            if "TotalAmount" not in df.columns or "Quantity" not in df.columns or "UnitPrice" not in df.columns or "Discount" not in df.columns:
                print(f"Warning: Required columns for VALID_ORDER_TOTAL not found in {table_name}, skipping")
                continue

            df = df.withColumn(

                "_ValidationStatus",

                when(

                    abs(

                        col("TotalAmount") -

                        (
                            (col("Quantity") * col("UnitPrice"))
                            - col("Discount")
                        )

                    ) > 1,

                    False

                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Order Date Validation
        # ------------------------------------------

        elif validation_rule == "NOT_FUTURE_DATE":

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    col(column_name) > current_date(),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Ship Date Validation
        # ------------------------------------------

        elif validation_rule == "SHIP_AFTER_ORDER":

            # Skip if required columns don't exist
            if "ShipDate" not in df.columns or "OrderDate" not in df.columns:
                print(f"Warning: ShipDate or OrderDate column not found in {table_name}, skipping SHIP_AFTER_ORDER")
                continue

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    col("ShipDate") < col("OrderDate"),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Selling Price >= Unit Cost
        # ------------------------------------------

        elif validation_rule == "SELL_PRICE_GE_COST":

            # Skip if required columns don't exist
            if "SellingPrice" not in df.columns or "UnitCost" not in df.columns:
                print(f"Warning: SellingPrice or UnitCost column not found in {table_name}, skipping SELL_PRICE_GE_COST")
                continue

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    col("SellingPrice") < col("UnitCost"),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Product Category Validation
        # ------------------------------------------

        elif validation_rule == "VALID_CATEGORY":

            valid_categories = [

                "Electronics",
                "Clothing",
                "Furniture",
                "Grocery",
                "Sports",
                "Books"

            ]

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    ~col(column_name).isin(valid_categories),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Product Status Validation
        # ------------------------------------------

        elif validation_rule == "VALID_PRODUCT_STATUS":

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    ~col(column_name).isin("Active", "Inactive"),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Date Of Birth Validation
        # ------------------------------------------

        elif validation_rule == "VALID_DOB":

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    col(column_name) >= current_date(),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

        # ------------------------------------------
        # Currency Validation
        # ------------------------------------------

        elif validation_rule == "VALID_CURRENCY":

            valid_currency = [

                "USD",
                "INR",
                "EUR",
                "GBP",
                "AUD",
                "CAD",
                "JPY"

            ]

            df = df.withColumn(

                "_ValidationStatus",

                when(
                    ~col(column_name).isin(valid_currency),
                    False
                ).otherwise(col("_ValidationStatus"))

            )

    # ------------------------------------------------------
    # Split Valid and Reject Records
    # ------------------------------------------------------

    valid_df = (

        df.filter(

            col("_ValidationStatus") == True

        )

    )

    reject_df = (

        df.filter(

            col("_ValidationStatus") == False

        )

    )

    # ------------------------------------------------------
    # Add Technical Columns
    # ------------------------------------------------------

    valid_df = (

        valid_df

        .withColumn(

            technical_dict["PROCESSED_TIMESTAMP_COLUMN"],

            current_timestamp()

        )

        .withColumn(

            technical_dict["IS_REJECTED_COLUMN"],

            lit(False)

        )

    )

    reject_df = (

        reject_df

        .withColumn(

            technical_dict["PROCESSED_TIMESTAMP_COLUMN"],

            current_timestamp()

        )

        .withColumn(

            technical_dict["IS_REJECTED_COLUMN"],

            lit(True)

        )

    )

    # ------------------------------------------------------
    # Remove Temporary Column
    # ------------------------------------------------------

    valid_df = valid_df.drop("_ValidationStatus")

    reject_df = reject_df.drop("_ValidationStatus")

    # ------------------------------------------------------
    # Write Silver Layer
    # ------------------------------------------------------

    overwrite_delta(

        valid_df,

        f"{SILVER_PATH}/{silver_folder}"

    )

    # ------------------------------------------------------
    # Write Reject Records
    # ------------------------------------------------------

    overwrite_delta(

        reject_df,

        f"{REJECT_PATH}/{table_name}"

    )

    print(f"{table_name} Silver Load Completed")

print("=" * 60)
print("Silver Layer Completed")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC Update Audit Log

# COMMAND ----------

# ==========================================================
# Update Audit Log
# ==========================================================

# ----------------------------------------------------------
# Store one audit record for every processed table.
#
# This helps monitor:
# • Total records read
# • Valid records
# • Rejected records
# • Processing status
# ----------------------------------------------------------

from datetime import datetime

audit_data = []

for row in table_config.collect():

    table_name = row["table_name"]

    bronze_folder = row["bronze_folder"]

    silver_folder = row["silver_folder"]

    bronze_count = read_delta(
        f"{BRONZE_PATH}/{bronze_folder}"
    ).count()

    silver_count = read_delta(
        f"{SILVER_PATH}/{silver_folder}"
    ).count()

    reject_count = read_delta(
        f"{REJECT_PATH}/{table_name}"
    ).count()

    audit_data.append(

        (

            pipeline_run_id,

            batch_id,

            "Silver",

            table_name,

            bronze_count,

            silver_count,

            reject_count,

            "SUCCESS",

            datetime.now()

        )

    )

audit_schema = StructType([

    StructField("PipelineRunId", StringType()),

    StructField("BatchId", StringType()),

    StructField("Layer", StringType()),

    StructField("TableName", StringType()),

    StructField("RecordsRead", LongType()),

    StructField("RecordsWritten", LongType()),

    StructField("RejectedRecords", LongType()),

    StructField("Status", StringType()),

    StructField("ProcessedTimestamp", TimestampType())

])

audit_df = spark.createDataFrame(
    audit_data,
    audit_schema
)

overwrite_delta(
    audit_df,
    LOG_PATH
)

print("Audit Log Updated Successfully")

# COMMAND ----------

# ==========================================================
# Silver Layer Validation
# ==========================================================

for row in table_config.collect():

    table_name = row["table_name"]

    bronze_folder = row["bronze_folder"]

    silver_folder = row["silver_folder"]

    bronze_count = read_delta(
        f"{BRONZE_PATH}/{bronze_folder}"
    ).count()

    silver_count = read_delta(
        f"{SILVER_PATH}/{silver_folder}"
    ).count()

    reject_count = read_delta(
        f"{REJECT_PATH}/{table_name}"
    ).count()

    print("=" * 60)

    print(f"Table : {table_name}")

    print(f"Bronze Records : {bronze_count}")

    print(f"Silver Records : {silver_count}")

    print(f"Rejected Records : {reject_count}")

    print("=" * 60)

    display(

        read_delta(
            f"{SILVER_PATH}/{silver_folder}"
        ).limit(5)

    )

print("Silver Layer Completed Successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Adding updated values to test scd 2

# COMMAND ----------

from pyspark.sql.functions import *

customer_df = read_delta(f"{SILVER_PATH}/customers")

customer_df = (

    customer_df

    .withColumn(

        "City",

        when(

            col("CustomerID") == 1001,

            "Chennai"

        )

        .otherwise(col("City"))

    )

    .withColumn(

        "Email",

        when(

            col("CustomerID") == 1001,

            "customer1001_new@gmail.com"

        )

        .otherwise(col("Email"))

    )

)

# COMMAND ----------

customer_df = customer_df.withColumn(

    technical_dict["RECORD_HASH_COLUMN"],

    sha2(

        concat_ws(

            "||",

            *[col(c).cast("string") for c in customer_df.columns if c != technical_dict["RECORD_HASH_COLUMN"]]

        ),

        256

    )

)

# COMMAND ----------

overwrite_delta(

    customer_df,

    f"{SILVER_PATH}/customers"

)