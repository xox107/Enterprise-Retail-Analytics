# Databricks notebook source
# ==========================================================
# SCD Type 2
# ==========================================================

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %run "./config"

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window

import uuid


# COMMAND ----------

# ==========================================================
# Read Metadata
# ==========================================================

table_config = (
    read_delta(f"{METADATA_PATH}/table_config")
    .filter(col("active_flag") == "Y")
)

technical_config = read_delta(
    f"{METADATA_PATH}/technical_config"
)

technical_dict = {

    row["config_name"]: row["config_value"]

    for row in technical_config.collect()

}

scd_config = read_delta(
    SCD_CONFIG_PATH
)

display(scd_config)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating pipeline id and batch id

# COMMAND ----------

# ==========================================================
# Runtime Information
# ==========================================================

dbutils.widgets.text(
    "pipelineRunId",
    "MANUAL_RUN"
)

pipeline_run_id = dbutils.widgets.get(
    "pipelineRunId"
)

batch_id = str(uuid.uuid4())

print(f"Pipeline Run ID : {pipeline_run_id}")

print(f"Batch ID : {batch_id}")

# COMMAND ----------

# ==========================================================
# SCD Type 2 Processing
# ==========================================================

for scd in scd_config.collect():

    # ------------------------------------------------------
    # Read metadata
    # ------------------------------------------------------

    table_name = scd["table_name"]

    business_key = scd["business_key"]

    config = (

        table_config

        .filter(
            col("table_name") == table_name
        )

        .first()

    )

    silver_folder = config["silver_folder"]

    gold_object = config["gold_object"]

    silver_path = f"{SILVER_PATH}/{silver_folder}"

    gold_path = f"{GOLD_PATH}/{gold_object}"

    print("=" * 60)

    print(f"Processing : {table_name}")

    print("=" * 60)

    # ------------------------------------------------------
    # Read latest Silver data
    # ------------------------------------------------------

    source_df = read_delta(
        silver_path
    )

    # ------------------------------------------------------
    # Check whether Gold Dimension exists
    # ------------------------------------------------------

    try:

        target_df = read_delta(gold_path)

    # Force Spark to access the Delta table
        target_df.limit(1).count()

        table_exists = True

        print("Gold Dimension Found")

    except Exception:

        table_exists = False

        print("Gold Dimension Not Found")
    # ------------------------------------------------------
    # Initial Load
    # ------------------------------------------------------

    if not table_exists:

        print("Performing Initial Load")

        source_df = (

            source_df

            .withColumn(

                technical_dict["SCD_START_COLUMN"],

                current_timestamp()

            )

            .withColumn(

                technical_dict["SCD_END_COLUMN"],

                lit(None).cast("timestamp")

            )

            .withColumn(

                technical_dict["SCD_CURRENT_COLUMN"],

                lit(True)

            )

        )

        window_spec = Window.orderBy(

            monotonically_increasing_id()

        )

        source_df = (

            source_df

            .withColumn(

                technical_dict["SURROGATE_KEY_COLUMN"],

                row_number().over(window_spec)

            )

        )

        overwrite_delta(

            source_df,

            gold_path

        )

        print("Initial Load Completed")

        continue

    # ------------------------------------------------------
    # Read only Current Records
    # ------------------------------------------------------

    current_df = (

        target_df

        .filter(

            col(

                technical_dict["SCD_CURRENT_COLUMN"]

            ) == True

        )

    )

    print("Current Records Loaded")
        # ------------------------------------------------------
    # Find New Records
    #
    # Business key not available in Gold Dimension
    # ------------------------------------------------------

    new_df = (

        source_df.alias("src")

        .join(

            current_df.alias("tgt"),

            col(f"src.{business_key}") == col(f"tgt.{business_key}"),

            "left_anti"

        )

    )



    # ------------------------------------------------------
    # Find Changed Records
    #
    # Business Key same
    # Record Hash different
    # ------------------------------------------------------

    changed_df = (

        source_df.alias("src")

        .join(

            current_df.alias("tgt"),

            col(f"src.{business_key}") == col(f"tgt.{business_key}"),

            "inner"

        )

        .filter(

            col(f"src.{technical_dict['RECORD_HASH_COLUMN']}")

            !=

            col(f"tgt.{technical_dict['RECORD_HASH_COLUMN']}")

        )

        .select("src.*")

    )



    # ------------------------------------------------------
    # Collect Changed Business Keys
    # ------------------------------------------------------

    changed_keys = [

        row[business_key]

        for row in

        changed_df

        .select(business_key)

        .distinct()

        .collect()

    ]

    # ------------------------------------------------------
    # Expire Existing Records
    #
    # Current record becomes historical record.
    # ------------------------------------------------------

    if len(changed_keys) > 0:

        target_df = (

            target_df

            .withColumn(

                technical_dict["SCD_CURRENT_COLUMN"],

                when(

                    (col(business_key).isin(changed_keys))

                    &

                    (col(technical_dict["SCD_CURRENT_COLUMN"]) == True),

                    lit(False)

                )

                .otherwise(

                    col(

                        technical_dict["SCD_CURRENT_COLUMN"]

                    )

                )

            )

            .withColumn(

                technical_dict["SCD_END_COLUMN"],

                when(

                    (col(business_key).isin(changed_keys))

                    &

                    (col(technical_dict["SCD_CURRENT_COLUMN"]) == False),

                    current_timestamp()

                )

                .otherwise(

                    col(

                        technical_dict["SCD_END_COLUMN"]

                    )

                )

            )

        )

        print("Existing Records Expired")

    else:

        print("No Records To Expire")

    # ------------------------------------------------------
    # Prepare Latest Versions
    #
    # New + Changed Records
    # ------------------------------------------------------

    latest_df = (

        new_df

        .unionByName(

            changed_df,

            allowMissingColumns=True

        )

    )

    latest_df = (

        latest_df

        .withColumn(

            technical_dict["SCD_START_COLUMN"],

            current_timestamp()

        )

        .withColumn(

            technical_dict["SCD_END_COLUMN"],

            lit(None).cast("timestamp")

        )

        .withColumn(

            technical_dict["SCD_CURRENT_COLUMN"],

            lit(True)

        )

    )

    print("Latest Version Prepared")

        # ------------------------------------------------------
    # Generate Surrogate Key for New Records
    # ------------------------------------------------------

    if latest_df.count() > 0:

        max_sk = (

            target_df

            .agg(

                max(

                    technical_dict["SURROGATE_KEY_COLUMN"]

                ).alias("max_sk")

            )

            .first()["max_sk"]

        )

        if max_sk is None:

            max_sk = 0

        window_spec = Window.orderBy(

            monotonically_increasing_id()

        )

        latest_df = (

            latest_df

            .withColumn(

                technical_dict["SURROGATE_KEY_COLUMN"],

                row_number().over(window_spec) + max_sk

            )

        )

    else:

        print("No New Records To Insert")

    # ------------------------------------------------------
    # Merge Historical + Latest Records
    # ------------------------------------------------------

    final_df = (

        target_df

        .unionByName(

            latest_df,

            allowMissingColumns=True

        )

    )

    # ------------------------------------------------------
    # Write Gold Dimension
    # ------------------------------------------------------

    overwrite_delta(

        final_df,

        gold_path

    )

    print(f"{table_name} SCD Type 2 Completed")

print("=" * 60)

print("All SCD Tables Processed Successfully")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC Update Audit Log

# COMMAND ----------

# DBTITLE 1,Cell 10
# ==========================================================
# Update Audit Log
# ==========================================================

from datetime import datetime

audit_data = []

for scd in scd_config.collect():

    table_name = scd["table_name"]

    config = (

        table_config

        .filter(
            col("table_name") == table_name
        )

        .first()

    )

    gold_object = config["gold_object"]

    gold_path = f"{GOLD_PATH}/{gold_object}"

    record_count = read_delta(
        gold_path
    ).count()

    audit_data.append(

        (

            pipeline_run_id,

            batch_id,

            "SCD",

            table_name,

            record_count,

            datetime.now(),

            "SUCCESS"

        )

    )

audit_schema = StructType([

    StructField("PipelineRunId", StringType()),

    StructField("BatchId", StringType()),

    StructField("Layer", StringType()),

    StructField("TableName", StringType()),

    StructField("RecordsWritten", LongType()),

    StructField("ProcessedTime", TimestampType()),

    StructField("Status", StringType())

])

audit_df = spark.createDataFrame(
    audit_data,
    audit_schema
)

overwrite_delta(
    audit_df,
    LOG_PATH
)

display(audit_df)

# COMMAND ----------

# ==========================================================
# Validate SCD Output
# ==========================================================

for scd in scd_config.collect():

    table_name = scd["table_name"]

    config = (

        table_config

        .filter(
            col("table_name") == table_name
        )

        .first()

    )

    gold_object = config["gold_object"]

    gold_path = f"{GOLD_PATH}/{gold_object}"

    df = read_delta(

        gold_path

    )

    print("=" * 60)

    print(f"Table : {table_name}")

    print(f"Total Records : {df.count()}")

    print("=" * 60)

    display(

        df.orderBy(

            technical_dict["SURROGATE_KEY_COLUMN"]

        )

    )