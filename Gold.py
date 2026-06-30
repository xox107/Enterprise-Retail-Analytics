# Databricks notebook source
#Gold Layer

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %run "./config"
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# ==========================================================
# Runtime Variables
# ==========================================================

from uuid import uuid4
from datetime import datetime

# Simulated ADF Pipeline Run ID
pipeline_run_id = str(uuid4())

# Batch ID
batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

print(f"Pipeline Run ID : {pipeline_run_id}")
print(f"Batch ID        : {batch_id}")

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

print("Metadata Loaded Successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Build Product Dimension

# COMMAND ----------

# ==========================================================
# Product Dimension
# ==========================================================

# ----------------------------------------------------------
# Read Product Metadata
# ----------------------------------------------------------

config = (

    table_config

    .filter(

        col("table_name") == "products"

    )

    .first()

)

silver_folder = config["silver_folder"]

gold_object = config["gold_object"]

silver_path = f"{SILVER_PATH}/{silver_folder}"

gold_path = f"{GOLD_PATH}/{gold_object}"

# ----------------------------------------------------------
# Read Silver Products
# ----------------------------------------------------------

product_df = read_delta(

    silver_path

)

# ----------------------------------------------------------
# Generate Surrogate Key
#
# Each product receives a unique surrogate key.
# ----------------------------------------------------------

from pyspark.sql.window import Window

window_spec = Window.orderBy(

    monotonically_increasing_id()

)

product_df = (

    product_df

    .withColumn(

        "ProductSK",

        row_number().over(window_spec)

    )

)

# ----------------------------------------------------------
# Write Product Dimension
# ----------------------------------------------------------

overwrite_delta(

    product_df,

    gold_path

)

print("Product Dimension Created Successfully")

display(product_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Exchange Rate Dimension

# COMMAND ----------

# ==========================================================
# Exchange Rate Dimension
# ==========================================================

# ----------------------------------------------------------
# Read Exchange Rate Metadata
# ----------------------------------------------------------

config = (

    table_config

    .filter(

        col("table_name") == "exchange_rates"

    )

    .first()

)

silver_folder = config["silver_folder"]

gold_object = config["gold_object"]

silver_path = f"{SILVER_PATH}/{silver_folder}"

gold_path = f"{GOLD_PATH}/{gold_object}"

# ----------------------------------------------------------
# Read Silver Exchange Rates
# ----------------------------------------------------------

exchange_df = read_delta(

    silver_path

)

# ----------------------------------------------------------
# Generate Surrogate Key
# ----------------------------------------------------------

from pyspark.sql.window import Window

window_spec = Window.orderBy(

    monotonically_increasing_id()

)

exchange_df = (

    exchange_df

    .withColumn(

        "ExchangeRateSK",

        row_number().over(window_spec)

    )

)

# ----------------------------------------------------------
# Write Exchange Rate Dimension
# ----------------------------------------------------------

overwrite_delta(

    exchange_df,

    gold_path

)

print("Exchange Rate Dimension Created Successfully")

display(exchange_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Build Fact Sales for Analytics for PowerBI

# COMMAND ----------

# ----------------------------------------------------------
# Create Fact Sales
#
# Join Orders with Customer and Product Dimensions
# ----------------------------------------------------------

fact_sales = (

    orders_df.alias("o")

    .join(

        customer_dim.alias("c"),

        col("o.CustomerID") == col("c.CustomerID"),

        "left"

    )

    .join(

        product_dim.alias("p"),

        col("o.ProductID") == col("p.ProductID"),

        "left"

    )

    .select(

        # Order Details
        col("o.OrderID"),

        col("o.OrderDate"),

        

        # Dimension Keys
        col("c.CustomerSK"),

        col("p.ProductSK"),

        # Business Keys (optional, useful for debugging)
        col("o.CustomerID"),

        col("o.ProductID"),

        # Measures
        col("o.Quantity"),

        col("o.UnitPrice"),

        col("o.Discount"),

        col("o.TotalAmount"),

        # Audit Columns
        col("o." + technical_dict["BATCH_ID_COLUMN"]),

        col("o." + technical_dict["ADF_RUN_ID_COLUMN"]),

        col("o." + technical_dict["PROCESSED_TIMESTAMP_COLUMN"])

    )

)

# ----------------------------------------------------------
# Write Fact Sales
# ----------------------------------------------------------

overwrite_delta(

    fact_sales,

    f"{GOLD_PATH}/fact_sales"

)

print("Fact Sales Created Successfully")

display(fact_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC Sales Summary

# COMMAND ----------

# ==========================================================
# Sales Summary
# ==========================================================

# ----------------------------------------------------------
# Read Fact Sales
# ----------------------------------------------------------

fact_sales = read_delta(

    f"{GOLD_PATH}/fact_sales"

)

# ----------------------------------------------------------
# Aggregate Daily Sales
# ----------------------------------------------------------

sales_summary = (

    fact_sales

    .groupBy(

        "OrderDate"

    )

    .agg(

        countDistinct("OrderID").alias("TotalOrders"),

        sum("Quantity").alias("TotalQuantity"),

        round(

            sum("TotalAmount"),

            2

        ).alias("TotalSales"),

        round(

            avg("TotalAmount"),

            2

        ).alias("AverageOrderValue")

    )

    .orderBy(

        "OrderDate"

    )

)

# ----------------------------------------------------------
# Write Gold Table
# ----------------------------------------------------------

overwrite_delta(

    sales_summary,

    f"{GOLD_PATH}/sales_summary"

)

print("Sales Summary Created Successfully")

display(sales_summary)

# COMMAND ----------

# DBTITLE 1,Cell 13
# ==========================================================
# Customer Sales Summary
# ==========================================================

# ----------------------------------------------------------
# Read Fact Sales
# ----------------------------------------------------------

fact_sales = read_delta(

    f"{GOLD_PATH}/fact_sales"

)

# ----------------------------------------------------------
# Read Customer Dimension
# ----------------------------------------------------------

customer_config = (

    table_config

    .filter(

        col("table_name") == "customers"

    )

    .first()

)

customer_dim = (

    read_delta(

        f"{GOLD_PATH}/{customer_config['gold_object']}"

    )

    .filter(

        col(

            technical_dict["SCD_CURRENT_COLUMN"]

        ) == True

    )

)

# ----------------------------------------------------------
# Create Customer Sales Summary
# ----------------------------------------------------------

customer_sales_summary = (

    fact_sales.alias("f")

    .join(

        customer_dim.alias("c"),

        col("f.CustomerSK") == col("c.CustomerSK"),

        "left"

    )

    .groupBy(

        col("c.CustomerSK"),

        col("c.CustomerID"),

        col("c.CustomerName"),

        col("c.City"),

        col("c.State"),

        col("c.Country")

    )

    .agg(

        countDistinct(

            "OrderID"

        ).alias(

            "TotalOrders"

        ),

        sum(

            "Quantity"

        ).alias(

            "TotalQuantity"

        ),

        round(

            sum(

                "TotalAmount"

            ),

            2

        ).alias(

            "TotalSales"

        ),

        round(

            avg(

                "TotalAmount"

            ),

            2

        ).alias(

            "AverageOrderValue"

        )

    )

    .orderBy(

        desc(

            "TotalSales"

        )

    )

)

# ----------------------------------------------------------
# Write Gold Table
# ----------------------------------------------------------

overwrite_delta(

    customer_sales_summary,

    f"{GOLD_PATH}/customer_sales_summary"

)

print("Customer Sales Summary Created Successfully")

display(customer_sales_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer Audit 

# COMMAND ----------

# ==========================================================
# Gold Layer Audit Log
# ==========================================================

audit_data = []

gold_objects = [

    "customer_dim",
    "product_dim",
    
    "fact_sales"

]

for obj in gold_objects:

    df = read_delta(

        f"{GOLD_PATH}/{obj}"

    )

    audit_data.append(

        (

            pipeline_run_id,

            batch_id,

            "Gold",

            obj,

            df.count(),

            "SUCCESS",

            datetime.now()

        )

    )

audit_schema = StructType([

    StructField("PipelineRunId", StringType()),

    StructField("BatchId", StringType()),

    StructField("Layer", StringType()),

    StructField("ObjectName", StringType()),

    StructField("RecordCount", LongType()),

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

display(audit_df)

print("Gold Audit Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC  Gold Layer Validation

# COMMAND ----------

# ==========================================================
# Gold Layer Validation
# ==========================================================

gold_objects = [

    "customer_dim",

    "product_dim",

    "fact_sales"

]

for obj in gold_objects:

    print("=" * 60)

    print(f"Validating : {obj}")

    print("=" * 60)

    df = read_delta(

        f"{GOLD_PATH}/{obj}"

    )

    print(f"Record Count : {df.count()}")

   

    display(df.limit(10))

print("=" * 60)

print("Gold Layer Validation Completed")

print("=" * 60)