# 🚀 Logistics Delivery Analytics Pipeline
### End-to-End Medallion Architecture | AWS S3 · Databricks · GitHub Actions · Power BI

<br/>

![Pipeline](https://img.shields.io/badge/Architecture-Medallion-ffd700?style=for-the-badge&logo=databricks&logoColor=white)
![AWS](https://img.shields.io/badge/AWS_S3-Storage-ff9900?style=for-the-badge&logo=amazons3&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-ff3621?style=for-the-badge&logo=databricks&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-f2c811?style=for-the-badge&logo=powerbi&logoColor=black)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-CRON-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10-3776ab?style=for-the-badge&logo=python&logoColor=white)

<br/>

> **6,093 orders · 86.16% Delivery Rate · 77.72% First Attempt Delivery · 12 Agents tracked — fully automated, zero manual steps.**

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Pipeline Stages](#-pipeline-stages)
- [Repository Structure](#-repository-structure)
- [Star Schema](#-star-schema)
- [DAX Measures](#-dax-measures)
- [Setup & Configuration](#-setup--configuration)
- [Credential Map](#-credential-map)
- [Dashboard Preview](#-dashboard-preview)
- [Key Results](#-key-results)
- [Tech Stack](#-tech-stack)

---

## 📦 Project Overview

A production-style **end-to-end logistics delivery analytics pipeline** that ingests raw daily shipment data, processes it through a **Medallion Architecture** (Bronze → Silver → Gold) on Databricks, automatically exports to GitHub via Actions, and renders a live interactive dashboard on **Power BI Service** — refreshed daily with zero manual intervention.

**Core Features:**
- Daily automated ingestion from local Excel/CSV files to AWS S3
- PySpark transformations across 3 Delta Lake layers on Databricks Community Edition
- Idempotent Gold layer writes using `DeltaTable.delete()` + append pattern
- Dual-write strategy: Delta Lake for reliability + Parquet for fast downstream export
- GitHub Actions CRON job exports CSVs daily and commits to repo
- Power BI Service connects via raw GitHub URL — no gateway required
- Checkpoint system prevents duplicate processing

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌───────────────────────────────────────────────┐
│   LOCAL SOURCE  │     │  AWS S3 — BRONZE │     │        DATABRICKS  (main_pipeline.py)         │
│                 │     │                  │     │                                               │
│  xlsx/csv files │────▶│  clg-demo-2026   │────▶│  BRONZE READ  →  SILVER  →  GOLD             │
│  data.py        │     │  raw/shipments/  │     │  spark.read      transform   fact_deliveries  │
│  IAM USER(.env) │     │  {today}.parquet │     │  s3a://          delta        + dim_date      │
└─────────────────┘     └──────────────────┘     └───────────────┬───────────────────────────────┘
                                                                  │ dual-write Parquet
                                                                  ▼
┌──────────────────────────────┐     ┌─────────────────────┐     ┌───────────────────────────────┐
│       POWER BI SERVICE       │     │   GITHUB ACTIONS    │     │    powerbi/fact/               │
│                              │     │   refersh.yml       │     │    powerbi/dim_date/           │
│  Web Connector               │◀────│   CRON 11:30 PM IST │◀────│    s3a://clg-demo-2026/...    │
│  Scheduled refresh           │     │   export_data.py    │     │    IAM ROLE (Spark s3a://)    │
│  DAX: DEL% FAD% UNDEL%       │     │   git commit + push │     └───────────────────────────────┘
└──────────────────────────────┘     └─────────────────────┘
```

---

## 🔄 Pipeline Stages

### Stage 1 — Local Ingestion (`data.py`)

```python
# Xlsx_to_Parquet class handles full ingestion workflow
transfer.file_transfer()      # xlsx/csv → .parquet (local)
transfer.upload_to_s3()       # boto3 upload_file() → S3 Bronze
transfer.delete_local_files() # clean up local parquet
transfer.archive_files()      # move source to archive/
```

- Reads source files from `C:\Users\addep\...\source`
- Converts xlsx/xls/csv → Parquet using pandas
- Uploads to `s3://clg-demo-2026/raw/shipments/{today}.parquet`
- **Auth:** IAM User credentials from `.env` file

---

### Stage 2 — AWS S3 Bronze Zone

| Path | Purpose |
|------|---------|
| `raw/shipments/{today}.parquet` | Daily raw data landing zone |
| `checkpoints/last_checkpoint.txt` | Prevents duplicate processing |
| `checkpoints/runs/{today}.txt` | Run history log |
| `silver/{today}/` | Delta Lake Silver layer |
| `gold/fact/` | Delta Lake Gold fact table |
| `gold/dim/dim_date/` | Delta Lake Gold dim table |
| `powerbi/fact/` | Parquet export for GitHub Actions |
| `powerbi/dim/dim_date/` | Parquet export for GitHub Actions |

---

### Stage 3 — Databricks (`main_pipeline.py`)

**Authentication split inside notebook:**

| Operation | Auth Method |
|-----------|------------|
| `spark.read/write` on `s3a://` | **IAM Role** (cluster-attached) |
| `boto3` checkpoint get/put | **IAM User** (via `dbutils.widgets`) |

#### Bronze → Silver

```python
silver_df = bronze_df \
    .drop("DRS No", "Location", "Created Date", ...)  # Drop 15 raw columns
    .withColumn("Employee_Name", F.lower(F.trim(F.col("Employee Name"))))
    .withColumn("POD_Date", F.to_date(F.col("POD Date"), "d-M-yyyy, HH:mm"))
    .withColumn("Status", F.lower(F.trim(F.col("Status"))))
    .withColumn("RunDate", F.lit(today))

silver_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"s3a://{bucket}/silver/{today}/")
```

#### Silver → Gold (Idempotent)

```python
# Idempotency: delete today's records then append
if DeltaTable.isDeltaTable(spark, gold_fact_path):
    dt = DeltaTable.forPath(spark, gold_fact_path)
    dt.delete(F.col("RunDate") == today)

gold_fact_df.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .save(gold_fact_path)

# Dual-write: Parquet for export pipeline
gold_fact_df.write.format("parquet").mode("append") \
    .save(f"s3a://{bucket}/powerbi/fact")
```

#### Gold → dim_date

```python
dim_date_df = spark.createDataFrame([(min_date, max_date)], ["start", "end"]) \
    .select(F.explode(F.sequence(F.col("start"), F.col("end"))).alias("date")) \
    .withColumn("day",         F.dayofmonth("date")) \
    .withColumn("month",       F.month("date")) \
    .withColumn("year",        F.year("date")) \
    .withColumn("dayofweek",   F.dayofweek("date")) \
    .withColumn("weekofyear",  F.weekofyear("date")) \
    .withColumn("monthname",   F.monthname("date")) \
    .withColumn("quarter",     F.quarter("date")) \
    .withColumn("is_weekend",  F.when(F.col("dayofweek").isin([1,7]), 1).otherwise(0)) \
    .withColumn("month_year",  F.date_format("date", "MMM-yyyy")) \
    .withColumn("quarter_year",F.concat(F.lit("Q"), F.quarter("date").cast("string"),
                                        F.lit("-"), F.year("date").cast("string")))
```

---

### Stage 4 — GitHub Actions (`refersh.yml`)

```yaml
on:
  schedule:
    - cron: '0 18 * * *'   # 11:30 PM IST daily
  workflow_dispatch:         # manual trigger

steps:
  - uses: actions/checkout@v4
  - uses: actions/setup-python@v5
    with: { python-version: '3.10' }
  - run: pip install s3fs pandas pyarrow
  - run: python export_data.py        # reads S3 → writes CSV
  - run: |
      git add *.csv
      git commit -m "Daily refresh $(date)"
      git push
```

**`export_data.py`** reads from `powerbi/` Parquet paths via `s3fs` and writes `fact.csv` + `dim_date.csv` to the repo root using IAM User credentials stored as GitHub Secrets.

---

### Stage 5 — Power BI Service

- **Connector:** Web Connector → `raw.githubusercontent.com/.../main/fact.csv`
- **Refresh:** Scheduled daily refresh, no gateway required
- **Model:** Star schema — `fact_deliveries` ↔ `dim_date` (join: `POD_Date = date`)

---

## 📁 Repository Structure

```
logistics_dashboard/
│
├── main_pipeline.py        # Databricks notebook — full Medallion pipeline
├── data.py                 # Local → S3 ingestion script
├── export_data.py          # S3 Parquet → CSV export for Power BI
│
├── fact.csv                # Auto-generated daily by GitHub Actions
├── dim_date.csv            # Auto-generated daily by GitHub Actions
│
├── .github/
│   └── workflows/
│       └── refersh.yml     # GitHub Actions CRON workflow
│
└── README.md
```

---

## ⭐ Star Schema

```
                    ┌─────────────────────────┐
                    │     fact_deliveries      │
                    │─────────────────────────│
                    │ Waybill_No              │
                    │ Employee_Name           │
                    │ Employee_Number         │
                    │ Status · Sub_status     │
                    │ Attempt_Count           │
                    │ POD_Date  ──────────────┼──┐
                    │ First_attempt           │  │ JOIN
                    │ RunDate                 │  │ POD_Date = date
                    │ Amount_payable          │  │
                    │ Pincode                 │  ▼
                    │ Consignee_Name          │  ┌─────────────────┐
                    │ Delivery_Payment_type   │  │    dim_date     │
                    └─────────────────────────┘  │─────────────────│
                                                 │ date  (PK)      │
                                                 │ day             │
                                                 │ month           │
                                                 │ year            │
                                                 │ dayofweek       │
                                                 │ weekofyear      │
                                                 │ quarter         │
                                                 │ monthname       │
                                                 │ is_weekend      │
                                                 │ is_weekday      │
                                                 │ month_year      │
                                                 │ quarter_year    │
                                                 └─────────────────┘
```

---

## 📐 DAX Measures

```dax
-- Delivery Percentage
%_DEL =
DIVIDE(
    CALCULATE(COUNTROWS(fact_deliveries), fact_deliveries[Status] = "delivered"),
    COUNTROWS(fact_deliveries)
) * 100

-- Undelivery Percentage (excludes waybills with both UNDEL and DEL records)
%_UNDEL =
DIVIDE(
    CALCULATE(COUNTROWS(fact_deliveries), fact_deliveries[Status] = "undelivered"),
    COUNTROWS(fact_deliveries)
) * 100

-- First Attempt Delivery Percentage
%_FAD =
DIVIDE(
    CALCULATE(
        COUNTROWS(fact_deliveries),
        fact_deliveries[Attempt_Count] = 1,
        fact_deliveries[Status] = "delivered"
    ),
    CALCULATE(
        COUNTROWS(fact_deliveries),
        fact_deliveries[Attempt_Count] = 1
    )
) * 100
```

---

## ⚙️ Setup & Configuration

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.10+ | data.py, export_data.py |
| Databricks Community Edition | Latest | PySpark notebook |
| AWS Account | — | S3 bucket |
| Power BI Desktop | Latest | Dashboard authoring |
| GitHub Account | — | Actions + CSV hosting |

### 1. AWS Setup

```bash
# Create S3 bucket
aws s3 mb s3://your-bucket-name --region us-east-1

# Create these folders manually or run the setup cell in main_pipeline.py
raw/shipments/
silver/
gold/fact/
gold/dim/dim_date/
checkpoints/runs/
powerbi/fact/
powerbi/dim/dim_date/
```

**IAM User** — attach `AmazonS3FullAccess`, store credentials in `.env`:
```
AWS_ACCESS_KEY=your_access_key
AWS_SECRECT_KEY=your_secret_key
REGION=us-east-1
BUCKET_NAME=your-bucket-name
AWS_FOLDER=raw/shipments/
```

**IAM Role** — attach to Databricks cluster with `AmazonS3FullAccess` trust policy.

### 2. Databricks Setup

```python
# Run with widgets in Databricks notebook
# Pass IAM User credentials via widget parameters:
AWS_ACCESS_KEY_ID = "your_access_key"
AWS_SECRET_ACCESS_KEY = "your_secret_key"
bucket_name = "clg-demo-2026"
region = "us-east-1"
```

### 3. GitHub Secrets

Add these secrets in `Settings → Secrets → Actions`:

| Secret Name | Value |
|------------|-------|
| `AWS_ACCESS_KEY` | IAM User access key |
| `AWS_SECRECT_KEY` | IAM User secret key |

### 4. Power BI Desktop

1. Open Power BI Desktop
2. **Get Data** → **Web**
3. Enter URL:
```
https://raw.githubusercontent.com/Kalyan-git-2002/logistics_dashboard/main/fact.csv
https://raw.githubusercontent.com/Kalyan-git-2002/logistics_dashboard/main/dim_date.csv
```
4. Create relationship: `fact_deliveries[POD_Date]` → `dim_date[date]`
5. Publish to Power BI Service → enable scheduled refresh

---

## 🔐 Credential Map

| Script | Auth Type | Method | Operations |
|--------|-----------|--------|-----------|
| `data.py` | IAM User | `.env` file | `boto3.upload_file()` to S3 |
| `main_pipeline.py` | IAM User | `dbutils.widgets` | `boto3` checkpoint `get/put_object` |
| `main_pipeline.py` | IAM Role | Cluster-attached | ALL `spark.read/write` on `s3a://` |
| `export_data.py` | IAM User | GitHub Secrets | `s3fs` reads `powerbi/` Parquet |

---

## 📊 Dashboard Preview

| Metric | Value |
|--------|-------|
| Total Assigned | 6,093 |
| Total Delivered | 5,250 |
| %_DEL | **86.16%** |
| %_UNDEL | **13.84%** |
| %_FAD | **77.72%** |
| Agents Tracked | 12 |
| Date Range | 01-03-2026 → 28-03-2026 |

**Dashboard Features:**
- KPI header cards with period comparison
- Agent-level performance table with color-coded delivery bars
- DEL% + FAD% daily trend line chart
- 75% benchmark reference line (dashed)
- Date range slicer + Period toggle (Weekly / Monthly / Yearly)

---

## 📈 Key Results

| Agent | Total Assigned | %_DEL | %_FAD |
|-------|---------------|-------|-------|
| karra basavaiah | 1,497 | 79.09% | 78.66% |
| venu katta | 1,206 | 82.84% | 74.29% |
| simhadri venkatarao | 1,060 | 87.45% | 83.45% |
| kiran srisairam | 402 | 93.83% | 82.11% |
| **Total** | **6,093** | **86.16%** | **77.72%** |

---

## 🛠️ Tech Stack

| Category | Tools |
|----------|-------|
| **Language** | Python 3.10 |
| **Data Processing** | PySpark, Pandas, PyArrow |
| **Storage** | AWS S3, Delta Lake |
| **Compute** | Databricks Community Edition |
| **Orchestration** | GitHub Actions, CRON |
| **Export** | s3fs, boto3 |
| **Analytics** | Power BI Desktop, Power BI Service |
| **Measures** | DAX |
| **Auth** | AWS IAM User, AWS IAM Role |
| **Format** | Parquet, Delta, CSV |

---

## 👤 Author

**Kalyan Addepalli**
Data Analyst · Power BI · Python · Databricks · AWS


---

*Built as a portfolio project to demonstrate end-to-end data engineering and analytics capabilities.*
