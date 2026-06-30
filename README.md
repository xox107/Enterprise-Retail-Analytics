# Enterprise Retail Analytics Platform

A metadata-driven Retail Analytics Platform built using **Azure Data Factory, Azure Databricks (PySpark), Azure Data Lake Storage Gen2, Azure SQL Database, and Power BI**. The platform automates data ingestion, transformation, historical data management, and reporting using the Medallion Architecture.

---

# Technology Stack

* Azure Data Factory
* Azure Databricks
* PySpark
* Azure Data Lake Storage Gen2
* Azure SQL Database
* Power BI
* Delta Lake

---

# Solution Architecture

```
Source Systems
       │
       ▼
Azure Data Factory
       │
       ▼
Landing (Raw)
       │
       ▼
Bronze Layer
       │
       ▼
Silver Layer
       │
 ├── Data Validation
 ├── Reject Records
 ├── Schema Evolution
 ├── Incremental Loading
 └── SCD Type 2
       │
       ▼
Gold Layer
       │
       ▼
Azure SQL Database
       │
       ▼
Power BI Dashboard
```

---

# Project Structure

```
notebooks/
│
├── Data_generation
├── Config
├── Metadata
├── Get_pipeline_config
├── Landing(raw)
├── Bronze
├── Silver
├── SCD
├── Gold
├── Publish_to_SQL
└── Unit_Test

adf/
documentation/
powerbi/
screenshots/
```

---

# Deployment Guide

## Step 1 - Create Azure Resources

Create the following Azure resources:

* Azure Data Factory
* Azure Databricks Workspace
* Azure Data Lake Storage Gen2
* Azure SQL Database
* Azure Key Vault (Optional)

---

## Step 2 - Upload Source Files

Upload the source datasets into the external storage location.

Example:

```
Customers.csv
Products.csv
Orders.csv
ExchangeRates.json
```

---

## Step 3 - Configure Storage

Update the configuration notebook with

* Storage Account Name
* Container Name
* Base Path
* SQL Connection Details

---

## Step 4 - Execute Metadata Notebook

Run

```
Metadata
```

This creates

* pipeline_config
* table_config
* dq_config
* watermark_config
* scd_config
* technical_config

---

## Step 5 - Execute Configuration Notebook

Run

```
Config
```

to initialize project configuration.

---

## Step 6 - Publish Azure Data Factory Pipelines

Publish all ADF pipelines.

Execute

```
pl_daily_master
```

for incremental loads.

or

```
pl_master
```

for complete end-to-end execution.

---

## Step 7 - Verify Data

Verify that data is available in

```
Landing

Bronze

Silver

Gold
```

---

## Step 8 - Publish Gold Layer

Execute

```
Publish_to_SQL
```

to load Gold layer tables into Azure SQL Database.

---

## Step 9 - Validate

Execute

```
Unit_Test
```

to verify

* Bronze Layer
* Silver Layer
* Gold Layer
* Watermark
* Audit
* Reject Records
* SCD Type 2

---

## Step 10 - Connect Power BI

Connect Power BI to Azure SQL Database and refresh the dashboard.

---

# Pipeline Execution Order

```
Metadata

↓

Config

↓

Landing

↓

Bronze

↓

Silver

↓

SCD

↓

Gold

↓

Publish_to_SQL

↓

Unit_Test
```

---

# Metadata-Driven Framework

The solution uses metadata tables to dynamically control pipeline execution without modifying notebook code.

## Metadata Tables

| Table             | Purpose                                       |
| ----------------- | ---------------------------------------------- |
| pipeline_config   | Controls pipeline execution                   |
| table_config      | Stores source and destination details         |
| dq_config         | Defines validation rules                      |
| watermark_config  | Maintains incremental loading information     |
| scd_config        | Stores SCD Type 2 configuration               |
| technical_config  | Stores audit and technical column definitions |

---

# Adding a New Data Source

The framework supports onboarding new data sources without modifying existing notebooks or Azure Data Factory pipelines.

## Step 1

Upload the new source file into the configured external storage location.

---

## Step 2

Register the source in

```
pipeline_config
```

Configure

* Pipeline Name
* Source Name
* Execution Order
* Active Status

---

## Step 3

Add an entry in

```
table_config
```

Configure

* Source Name
* Source Type
* File Format
* Landing Path
* Bronze Table
* Silver Table
* Gold Table
* Primary Key
* Load Type
* Watermark Column

---

## Step 4

Configure Data Quality Rules

Insert validation rules into

```
dq_config
```

Examples

* Null Validation
* Duplicate Validation
* Data Type Validation
* Mandatory Field Validation
* Business Rule Validation

---

## Step 5

Configure Incremental Loading (Optional)

If incremental processing is required, add a record into

```
watermark_config
```

Specify

* Source Name
* Watermark Column
* Last Processed Value

---

## Step 6

Configure Historical Tracking (Optional)

If historical tracking is required, register the table in

```
scd_config
```

Specify

* Business Key
* Tracked Columns
* SCD Type

---

## Step 7

Run the Pipeline

Execute

```
pl_daily_master
```

The framework automatically

* Reads metadata
* Identifies the new source
* Loads data into Bronze
* Performs validation
* Applies incremental loading
* Executes SCD Type 2 (if configured)
* Creates Gold tables
* Updates audit logs
* Publishes data to Azure SQL Database

No notebook or pipeline changes are required.

---

# Features

* Metadata-Driven Framework
* Medallion Architecture
* Incremental Loading (Watermark)
* Schema Evolution
* SCD Type 2
* Data Quality Framework
* Reject Record Handling
* Audit Logging
* Azure SQL Integration
* Power BI Dashboard
* Unit Testing

---

# Author

**S. Thirukumaran**

Enterprise Retail Analytics Platform using Azure Data Engineering Services.
