# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------



import json
from datetime import datetime
import pyspark.sql.functions as F

today = datetime.utcnow().strftime("%Y-%m-%d")

# ── Watermark Checks ───────────────────────────────────────

# Check 1: New CSV files in landing
try:
    orders_df = read_delta(get_path("bronze", "orders_bronze"))
    last_order_date = orders_df.agg(F.max("_LoadDate")).collect()[0][0]
    ingest_csv = str(last_order_date) < today
except:
    ingest_csv = True

# Check 2: New customers in SQL
try:
    silver_customers = read_delta(get_path("silver", "customers"))
    last_customer_update = silver_customers.agg(F.max("LastUpdated")).collect()[0][0]
    ingest_customers = str(last_customer_update)[:10] < today
except:
    ingest_customers = True

# Check 3: Exchange rate for today exists
try:
    fx_df = read_delta(get_path("silver", "exchange_rates_silver"))
    last_rate_date = fx_df.agg(F.max("RateDate")).collect()[0][0]
    ingest_exchange_rate = str(last_rate_date) < today
except:
    ingest_exchange_rate = True

# Check 4: Publish to SQL
try:
    read_delta(get_path("gold", "fact_sales"))
    publish_to_sql = True
except:
    publish_to_sql = True

# ── Build config ───────────────────────────────────────────
config_dict = {
    "ingest_csv"           : str(ingest_csv).lower(),
    "ingest_customers"     : str(ingest_customers).lower(),
    "ingest_exchange_rate" : str(ingest_exchange_rate).lower(),
    "publish_to_sql"       : str(publish_to_sql).lower(),
    "generation_mode"      : "daily_incremental"
}

print(f"Today                : {today}")
print(f"ingest_csv           : {ingest_csv}")
print(f"ingest_customers     : {ingest_customers}")
print(f"ingest_exchange_rate : {ingest_exchange_rate}")
print(f"publish_to_sql       : {publish_to_sql}")

dbutils.notebook.exit(json.dumps(config_dict))