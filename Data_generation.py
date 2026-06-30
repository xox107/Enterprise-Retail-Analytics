# Databricks notebook source
# MAGIC %md
# MAGIC Databricks notebook in 4 parts:
# MAGIC
# MAGIC Part 1: Imports, Config, Products, Customers
# MAGIC Part 2: Orders, Schemas, DataFrames
# MAGIC Part 3: Save to ADLS, Azure SQL, Exchange Rate API
# MAGIC Part 4: Validation, Sample Data, Final Verification

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Part 1: Imports, Config, Products, Customers

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ==========================================================
# Retail Data Generation
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import *

import random
import uuid

from datetime import datetime, timedelta

from faker import Faker

fake = Faker()

random.seed(100)

# ==========================================================
# Number of Records
# ==========================================================

NUM_PRODUCTS = 500

NUM_CUSTOMERS = 5000

NUM_ORDERS = 50000

print("Retail Data Generation Started")

# COMMAND ----------

# MAGIC %md
# MAGIC Products data 

# COMMAND ----------

# ==========================================================
# Generate Products
# ==========================================================

categories = {

    "Electronics": [
        "Laptop",
        "Mobile",
        "Tablet",
        "Camera",
        "Headphones"
    ],

    "Furniture": [
        "Chair",
        "Table",
        "Sofa",
        "Bed"
    ],

    "Fashion": [
        "Shirt",
        "Jeans",
        "Shoes",
        "Jacket"
    ],

    "Groceries": [
        "Rice",
        "Oil",
        "Sugar",
        "Tea"
    ]

}

brands = [

    "Samsung",

    "Apple",

    "Sony",

    "LG",

    "Dell",

    "HP",

    "Nike",

    "Adidas"

]

products = []

for product_id in range(

    1,

    NUM_PRODUCTS + 1

):

    category = random.choice(

        list(categories.keys())

    )

    product_name = random.choice(

        categories[category]

    )

    brand = random.choice(

        brands

    )

    cost_price = round(

        random.uniform(100,5000),

        2

    )

    selling_price = round(

        cost_price *

        random.uniform(

            1.10,

            1.50

        ),

        2

    )

    status = random.choice(

        [

            "Active",

            "Inactive"

        ]

    )

    products.append(

        (

            product_id,

            product_name,

            category,

            brand,

            cost_price,

            selling_price,

            status

        )

    )

print(

    f"Generated {len(products)} Products"

)

# COMMAND ----------

# MAGIC %md
# MAGIC Customers data

# COMMAND ----------

# ==========================================================
# Generate Customers
# ==========================================================

cities = [

    ("Chennai","Tamil Nadu"),

    ("Bengaluru","Karnataka"),

    ("Hyderabad","Telangana"),

    ("Mumbai","Maharashtra"),

    ("Pune","Maharashtra"),

    ("Delhi","Delhi"),

    ("Kolkata","West Bengal"),

    ("Ahmedabad","Gujarat")

]

customers = []

for customer_id in range(

    1,

    NUM_CUSTOMERS + 1

):

    first_name = fake.first_name()

    last_name = fake.last_name()

    customer_name = f"{first_name} {last_name}"

    gender = random.choice(

        [

            "Male",

            "Female"

        ]

    )

    email = (

        first_name.lower()

        + "."

        + last_name.lower()

        + str(customer_id)

        + "@gmail.com"

    )

    phone = (

        "9"

        + "".join(

            random.choices(

                "0123456789",

                k=9

            )

        )

    )

    dob = fake.date_between(

        start_date="-60y",

        end_date="-18y"

    )

    city, state = random.choice(

        cities

    )

    registration_date = fake.date_time_between(

        start_date="-3y",

        end_date="now"

    )

    last_updated = registration_date + timedelta(

        days=random.randint(

            0,

            30

        )

    )

    customers.append(

        (

            customer_id,

            customer_name,

            gender,

            email,

            phone,

            dob,

            city,

            state,

            "India",

            registration_date,

            last_updated

        )

    )

print(

    f"Generated {len(customers)} Customers"

)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate orders

# COMMAND ----------

# ==========================================================
# Generate Orders
# ==========================================================

payment_methods = [

    "Cash",

    "Credit Card",

    "Debit Card",

    "UPI",

    "Net Banking"

]

order_status = [

    "Delivered",

    "Shipped",

    "Cancelled",

    "Returned"

]

orders = []

for order_id in range(

    1,

    NUM_ORDERS + 1

):

    customer_id = random.randint(

        1,

        NUM_CUSTOMERS

    )

    product_id = random.randint(

        1,

        NUM_PRODUCTS

    )

    quantity = random.randint(

        1,

        10

    )

    product = products[product_id - 1]

    unit_price = product[5]

    discount = random.choice(

        [0,5,10,15,20]

    )

    total_amount = round(

        quantity *

        unit_price *

        (1 - discount / 100),

        2

    )

    order_date = fake.date_time_between(

        start_date="-2y",

        end_date="now"

    )

    ship_date = order_date + timedelta(

        days=random.randint(

            1,

            7

        )

    )

    payment_method = random.choice(

        payment_methods

    )

    status = random.choice(

        order_status

    )

    orders.append(

        (

            order_id,

            customer_id,

            product_id,

            quantity,

            unit_price,

            discount,

            total_amount,

            order_date,

            ship_date,

            payment_method,

            status

        )

    )

print(

    f"Generated {len(orders)} Orders"

)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Schemas

# COMMAND ----------

# ==========================================================
# Create Schemas
# ==========================================================

products_schema = StructType([

    StructField("ProductID", IntegerType()),

    StructField("ProductName", StringType()),

    StructField("Category", StringType()),

    StructField("Brand", StringType()),

    StructField("CostPrice", DoubleType()),

    StructField("SellingPrice", DoubleType()),

    StructField("Status", StringType())

])


customers_schema = StructType([

    StructField("CustomerID", IntegerType()),

    StructField("CustomerName", StringType()),

    StructField("Gender", StringType()),

    StructField("Email", StringType()),

    StructField("Phone", StringType()),

    StructField("DateOfBirth", DateType()),

    StructField("City", StringType()),

    StructField("State", StringType()),

    StructField("Country", StringType()),

    StructField("RegistrationDate", TimestampType()),

    StructField("LastUpdated", TimestampType())

])


orders_schema = StructType([

    StructField("OrderID", IntegerType()),

    StructField("CustomerID", IntegerType()),

    StructField("ProductID", IntegerType()),

    StructField("Quantity", IntegerType()),

    StructField("UnitPrice", DoubleType()),

    StructField("Discount", DoubleType()),

    StructField("TotalAmount", DoubleType()),

    StructField("OrderDate", TimestampType()),

    StructField("ShipDate", TimestampType()),

    StructField("PaymentMethod", StringType()),

    StructField("OrderStatus", StringType())

])

print("Schemas Created")

# COMMAND ----------

products_df.write \
.option(AUTH_OPTION, STORAGE_ACCOUNT_KEY) \
.mode("overwrite") \
.csv(f"{SOURCE_PATH}/products")

# COMMAND ----------

# MAGIC %md
# MAGIC Create Spark DataFrames

# COMMAND ----------

# ==========================================================
# Create Spark DataFrames
# ==========================================================

products_df = spark.createDataFrame(

    products,

    products_schema

)

customers_df = spark.createDataFrame(

    customers,

    customers_schema

)

orders_df = spark.createDataFrame(

    orders,

    orders_schema

)

print("Spark DataFrames Created")

display(products_df.limit(10))

display(customers_df.limit(10))

display(orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Save Products to Source ADLS

# COMMAND ----------

# ==========================================================
# Save Products to Source ADLS
# ==========================================================

(
    products_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)
    .csv(f"{SOURCE_PATH}/products")
)

print("Products saved successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Save Orders to Source ADLS

# COMMAND ----------

# ==========================================================
# Save Orders to Source ADLS
# ==========================================================

(
    orders_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)
    .csv(f"{SOURCE_PATH}/orders")
)

print("Orders saved successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Load Customers into Azure SQL

# COMMAND ----------

# ==========================================================
# Save Customers to Source ADLS
# ==========================================================

(
    customers_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)
    .csv(f"{SOURCE_PATH}/customers")
)

print("Customers saved successfully.")

# COMMAND ----------

print(STORAGE_ACCOUNT_KEY)

# COMMAND ----------

(
    customers_df.write
    .format("sqlserver")
    .option("host", SQL_SERVER_NAME)
    .option("port", "1433")
    .option("database", SQL_DATABASE_NAME)
    .option("dbtable", "dbo.Customers")
    .option("user", SQL_USERNAME)
    .option("password", SQL_PASSWORD)
    .option("encrypt", "true")
    .option("trustServerCertificate", "false")
    .mode("overwrite")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Exchange rate

# COMMAND ----------

# ==========================================================
# Generate Exchange Rate JSON
# ==========================================================

import requests
import json

response = requests.get(EXCHANGE_RATE_URL)

if response.status_code != 200:

    raise Exception(
        f"API Error : {response.status_code}"
    )

exchange_json = response.json()

print("Exchange Rate API Called Successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC Convert API Response to Spark DataFrame

# COMMAND ----------

# ==========================================================
# Convert API Response
# ==========================================================

base_currency = exchange_json["base_code"]

rate_date = datetime.now().date()

exchange_records = []

for currency, rate in exchange_json["conversion_rates"].items():

    exchange_records.append(

        (

            base_currency,

            currency,

            float(rate),

            rate_date

        )

    )

exchange_schema = StructType([

    StructField("BaseCurrency", StringType()),

    StructField("TargetCurrency", StringType()),

    StructField("ExchangeRate", DoubleType()),

    StructField("RateDate", DateType())

])

exchange_df = spark.createDataFrame(

    exchange_records,

    exchange_schema

)

display(exchange_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Save Exchange Rates to Landing (Raw)

# COMMAND ----------

# ==========================================================
# Save Exchange Rates to Landing
# ==========================================================

(
    exchange_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .option(AUTH_OPTION, STORAGE_ACCOUNT_KEY)
    .json(f"{LANDING_PATH}/exchange_rates")
)

print("Exchange rates saved to Landing.")