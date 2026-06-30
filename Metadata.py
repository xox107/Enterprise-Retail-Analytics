# Databricks notebook source
# MAGIC %run ./config
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Defining meta data paths

# COMMAND ----------

# ==========================================================
# Metadata Paths
# ==========================================================

TABLE_CONFIG_PATH = get_path(
    "metadata",
    "table_config"
)

DATATYPE_CONFIG_PATH = get_path(
    "metadata",
    "datatype_config"
)

DQ_CONFIG_PATH = get_path(
    "metadata",
    "dq_config"
)

WATERMARK_PATH = get_path(
    "metadata",
    "watermark"
)

PIPELINE_CONFIG_PATH = get_path(
    "metadata",
    "pipeline_config"
)

AUDIT_LOG_PATH = get_path(
    "metadata",
    "audit_log"
)

print("Metadata Paths Created")

# COMMAND ----------

# MAGIC %md
# MAGIC Create table_config

# COMMAND ----------

# ==========================================================
# Table Configuration
# ==========================================================

TABLE_CONFIG_PATH = get_path(
    "metadata",
    "table_config"
)

table_config_schema = StructType([

    StructField("table_name", StringType(), True),

    StructField("source_type", StringType(), True),

    StructField("source_name", StringType(), True),

    StructField("file_format", StringType(), True),

    StructField("target_file_format", StringType(), True),

    StructField("load_type", StringType(), True),

    StructField("primary_key", StringType(), True),

    StructField("business_key", StringType(), True),

    StructField("scd_type", StringType(), True),

    StructField("watermark_column", StringType(), True),

    StructField("landing_folder", StringType(), True),

    StructField("bronze_folder", StringType(), True),

    StructField("silver_folder", StringType(), True),
    

    StructField("gold_object", StringType(), True),

    StructField("active_flag", StringType(), True),

    StructField("execution_order", IntegerType(), True),


])

table_config_data = [

(
    "products",
    "ADLS",
    "products",
    "csv",
    "delta",
    "FULL",
    "ProductID",
    "ProductID",
    "TYPE1",
    None,
    "products",
    "products",
    "products",
    "product_dim",
    "Y",
    1
),

(
    "customers",
    "ADLS",
    "customers",
    "csv",
    "delta",
    "INCREMENTAL",
    "CustomerID",
    "CustomerID",
    "TYPE2",
    "LastUpdated",
    "customers",
    "customers",
    "customers",
    "customer_dim",
    "Y",
    2
),

(
    "orders",
    "ADLS",
    "orders",
    "csv",
    "delta",
    "INCREMENTAL",
    "OrderID",
    "OrderID",
    "NONE",
    "OrderDate",
    "orders",
    "orders",
    "orders",
    "orders_fact",
    "Y",
    3
),

(
    "exchange_rates",
    "API",
    "exchange_rates",
    "json",
    "delta",
    "FULL",
    "TargetCurrency",
    "TargetCurrency",
    "NONE",
    None,
    "exchange_rates",
    "exchange_rates",
    "exchange_rates",
    None,
    "Y",
    4
)

]

table_config_df = spark.createDataFrame(

    table_config_data,

    table_config_schema

)

overwrite_delta(
    table_config_df,
    TABLE_CONFIG_PATH
)

display(table_config_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Data type congig for type casting

# COMMAND ----------

# ==========================================================
# Datatype Configuration
# ==========================================================

datatype_schema = StructType([

    StructField("table_name", StringType()),
    StructField("column_name", StringType()),
    StructField("target_datatype", StringType())

])

datatype_data = [

# ==========================================================
# Orders
# ==========================================================

("orders","OrderID","INT"),
("orders","CustomerID","INT"),
("orders","ProductID","INT"),
("orders","StoreID","INT"),
("orders","OrderDate","TIMESTAMP"),
("orders","ShipDate","TIMESTAMP"),
("orders","Quantity","INT"),
("orders","UnitPrice","DECIMAL(18,2)"),
("orders","Discount","DECIMAL(5,2)"),
("orders","TotalAmount","DECIMAL(18,2)"),

# ==========================================================
# Products
# ==========================================================

("products","ProductID","INT"),
("products","ProductName","STRING"),
("products","Category","STRING"),
("products","SubCategory","STRING"),
("products","Brand","STRING"),
("products","UnitCost","DECIMAL(18,2)"),
("products","SellingPrice","DECIMAL(18,2)"),
("products","Status","STRING"),

# ==========================================================
# Customers
# ==========================================================

("customers","CustomerID","INT"),
("customers","CustomerName","STRING"),
("customers","Email","STRING"),
("customers","Phone","STRING"),
("customers","Gender","STRING"),
("customers","DateOfBirth","DATE"),
("customers","City","STRING"),
("customers","State","STRING"),
("customers","Country","STRING"),
("customers","LastUpdated","TIMESTAMP"),

# ==========================================================
# Exchange Rates
# ==========================================================

("exchange_rates","BaseCurrency","STRING"),
("exchange_rates","TargetCurrency","STRING"),
("exchange_rates","ExchangeRate","DECIMAL(18,6)"),
("exchange_rates","RateDate","DATE")

]

datatype_df = spark.createDataFrame(
    datatype_data,
    datatype_schema
)

write_delta(
    datatype_df,
    DATATYPE_CONFIG_PATH
)

display(datatype_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Data quality check for validation

# COMMAND ----------

# ==========================================================
# Data Quality Configuration
# ==========================================================

dq_schema = StructType([

    StructField("table_name", StringType()),
    StructField("column_name", StringType()),
    StructField("validation_rule", StringType()),
    StructField("severity", StringType())

])

dq_data = [

# ==========================================================
# Orders
# ==========================================================

("orders","OrderID","NOT_NULL","ERROR"),
("orders","CustomerID","NOT_NULL","ERROR"),
("orders","ProductID","NOT_NULL","ERROR"),
("orders","StoreID","NOT_NULL","ERROR"),
("orders","Quantity","POSITIVE_NUMBER","ERROR"),
("orders","UnitPrice","POSITIVE_NUMBER","ERROR"),
("orders","Discount","VALID_DISCOUNT","ERROR"),
("orders","OrderDate","NOT_FUTURE_DATE","ERROR"),
("orders","ShipDate","SHIP_AFTER_ORDER","ERROR"),
("orders","TotalAmount","VALID_ORDER_TOTAL","ERROR"),

# ==========================================================
# Products
# ==========================================================

("products","ProductID","NOT_NULL","ERROR"),
("products","ProductName","NOT_EMPTY","ERROR"),
("products","Category","VALID_CATEGORY","ERROR"),
("products","UnitCost","POSITIVE_NUMBER","ERROR"),
("products","SellingPrice","SELL_PRICE_GE_COST","ERROR"),
("products","Status","VALID_PRODUCT_STATUS","WARNING"),

# ==========================================================
# Customers
# ==========================================================

("customers","CustomerID","NOT_NULL","ERROR"),
("customers","CustomerName","NOT_EMPTY","ERROR"),
("customers","Email","VALID_EMAIL","ERROR"),
("customers","Phone","VALID_PHONE","WARNING"),
("customers","DateOfBirth","VALID_DOB","ERROR"),
("customers","Country","VALID_COUNTRY","WARNING"),
("customers","LastUpdated","NOT_NULL","ERROR"),

# ==========================================================
# Exchange Rates
# ==========================================================

("exchange_rates","BaseCurrency","VALID_CURRENCY","ERROR"),
("exchange_rates","TargetCurrency","VALID_CURRENCY","ERROR"),
("exchange_rates","ExchangeRate","POSITIVE_NUMBER","ERROR"),
("exchange_rates","RateDate","NOT_FUTURE_DATE","ERROR")

]

dq_df = spark.createDataFrame(
    dq_data,
    dq_schema
)

write_delta(
    dq_df,
    DQ_CONFIG_PATH
)

display(dq_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create watermark

# COMMAND ----------

watermark_schema = StructType([

    StructField("table_name", StringType()),

    StructField("watermark_column", StringType()),

    StructField("last_watermark", TimestampType())

])

watermark_data = [

(
"customers",
"LastUpdated",
None
)

]

watermark_df = spark.createDataFrame(
    watermark_data,
    watermark_schema
)

write_delta(
    watermark_df,
    WATERMARK_PATH
)

display(watermark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create pipeline_config (we should keep this as seperate note book)

# COMMAND ----------

pipeline_schema = StructType([

    StructField("pipeline_name", StringType()),

    StructField("activity_name", StringType()),

    StructField("notebook_name", StringType()),

    StructField("execution_order", IntegerType()),

    StructField("depends_on", StringType()),

    StructField("active_flag", StringType())

])

pipeline_data = [

(
"MasterPipeline",
"Bronze",
"02_Bronze",
1,
None,
"Y"
),

(
"MasterPipeline",
"Silver",
"03_Silver",
2,
"Bronze",
"Y"
),

(
"MasterPipeline",
"SCD",
"04_SCD",
3,
"Silver",
"Y"
),

(
"MasterPipeline",
"Gold",
"05_Gold",
4,
"SCD",
"Y"
),

(
"MasterPipeline",
"Publish_SQL",
"06_Publish_SQL",
5,
"Gold",
"Y"
)

]

pipeline_df = spark.createDataFrame(
    pipeline_data,
    pipeline_schema
)

write_delta(
    pipeline_df,
    PIPELINE_CONFIG_PATH
)

display(pipeline_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create audit_log

# COMMAND ----------

audit_schema = StructType([

    StructField("batch_id", StringType()),

    StructField("pipeline_name", StringType()),

    StructField("table_name", StringType()),

    StructField("start_time", TimestampType()),

    StructField("end_time", TimestampType()),

    StructField("records_processed", LongType()),

    StructField("records_rejected", LongType()),

    StructField("status", StringType()),

    StructField("error_message", StringType())

])

audit_df = spark.createDataFrame(
    [],
    audit_schema
)

write_delta(
    audit_df,
    AUDIT_LOG_PATH
)

display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create technical_config

# COMMAND ----------

# ==========================================================
# Technical Configuration
# ==========================================================

TECHNICAL_CONFIG_PATH = get_path(
    "metadata",
    "technical_config"
)

technical_schema = StructType([

    StructField("config_name", StringType()),

    StructField("config_value", StringType())

])

technical_data = [

("BATCH_ID_COLUMN","_BatchId"),

("ADF_RUN_ID_COLUMN","_AdfPipelineRunId"),

("INGESTION_TIMESTAMP_COLUMN","_IngestionTimestamp"),

("PROCESSED_TIMESTAMP_COLUMN","_ProcessedTimestamp"),

("IS_REJECTED_COLUMN","_IsRejected"),

("RECORD_HASH_COLUMN","_RecordHash"),
("SCD_START_COLUMN", "_StartDate"),
("SCD_END_COLUMN", "_EndDate"),
("SCD_CURRENT_COLUMN", "_IsCurrent"),
("SURROGATE_KEY_COLUMN","CustomerSK")


]

technical_df = spark.createDataFrame(

    technical_data,

    technical_schema

)

write_delta(

    technical_df,

    TECHNICAL_CONFIG_PATH

)

display(technical_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create scd_config

# COMMAND ----------

# ==========================================================
# SCD Configuration
# ==========================================================

SCD_CONFIG_PATH = get_path(

    "metadata",

    "scd_config"

)

scd_schema = StructType([

    StructField("table_name",StringType()),

    StructField("business_key",StringType()),

    StructField("scd_type",StringType())

])

scd_data = [

("customers","CustomerID","TYPE2"),


]

scd_df = spark.createDataFrame(

    scd_data,

    scd_schema

)

write_delta(

    scd_df,

    SCD_CONFIG_PATH

)

display(scd_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create gold_config

# COMMAND ----------

# ==========================================================
# Gold Configuration
# ==========================================================

GOLD_CONFIG_PATH = get_path(

    "metadata",

    "gold_config"

)

gold_schema = StructType([

    StructField("gold_object",StringType()),

    StructField("source_table",StringType()),

    StructField("object_type",StringType())

])

gold_data = [

("customer_dim","customers","DIM"),

("product_dim","products","DIM"),

("orders_fact","orders","FACT")

]

gold_df = spark.createDataFrame(

    gold_data,

    gold_schema

)

write_delta(

    gold_df,

    GOLD_CONFIG_PATH

)

display(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC All metadata tables

# COMMAND ----------

# ==========================================================
# Verify Metadata
# ==========================================================

print("Table Config")
display(read_delta(TABLE_CONFIG_PATH))

print("Datatype Config")
display(read_delta(DATATYPE_CONFIG_PATH))

print("DQ Config")
display(read_delta(DQ_CONFIG_PATH))

print("Watermark")
display(read_delta(WATERMARK_PATH))

print("Pipeline Config")
display(read_delta(PIPELINE_CONFIG_PATH))

print("Technical Config")
display(read_delta(TECHNICAL_CONFIG_PATH))

print("SCD Config")
display(read_delta(SCD_CONFIG_PATH))

print("Gold Config")
display(read_delta(GOLD_CONFIG_PATH))

print("Audit Log")
display(read_delta(AUDIT_LOG_PATH))