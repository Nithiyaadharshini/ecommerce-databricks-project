# E-Commerce Sales Analytics — Project Documentation

## 1. Problem Statement

An e-commerce company generates large volumes of transactional data across products, customers, brands, and categories. This data exists as raw CSV files with no structure, no quality checks, and no single source of truth for reporting.

The business needs a centralized analytics platform that can:

* Ingest raw e-commerce data (products, customers, brands, categories, order items, calendar dates)
* Cleanse and standardize the data through a layered pipeline
* Build a denormalized analytics-ready table for fast querying
* Provide an interactive BI dashboard with KPIs, trends, and breakdowns by product, category, brand, region, time, and day type

**Goal:** Transform raw CSV data into a publishable BI dashboard that gives business stakeholders a 360-degree view of sales performance.

---

## 2. Architecture — Medallion Architecture (Bronze / Silver / Gold)

The project follows the **Medallion Architecture**, a data design pattern that organizes data in three layers of increasing quality and structure:

```
Raw CSVs (UC Volume)
       |
       v
+------------------+      +------------------+      +------------------+
|     BRONZE       | ---> |     SILVER       | ---> |      GOLD        |
|  (Raw ingest)    |      |  (Cleansed)      |      | (Analytics-ready) |
+------------------+      +------------------+      +------------------+
   - Load CSV as-is         - Deduplicate             - Star schema
   - No transforms          - Type cast               - Dim + Fact tables
   - Schema enforced        - Drop nulls/dups         - Denormalized view
   - Full history           - Standardized            - Business metrics
   6 tables                 6 tables                 4 tables + 1 view
```

### Why Medallion?

| Layer | Purpose | Analogy |
| --- | --- | --- |
| Bronze | Capture raw data exactly as-is | Unrefined ore |
| Silver | Cleanse, deduplicate, standardize | Refined metal |
| Gold | Business-ready analytics tables | Polished product |

---

## 3. Unity Catalog Setup

| Object | Name | Description |
| --- | --- | --- |
| Catalog | `ecommerce` | Top-level Unity Catalog |
| Schema | `bronze` | Raw data landing zone |
| Schema | `silver` | Cleansed data layer |
| Schema | `gold` | Analytics-ready layer |
| Schema | `source_data` | Volume metadata |
| Volume | `/Volumes/ecommerce/source_data/raw/` | Stores raw CSV files |

---

## 4. Tables — Layer by Layer

### 4.1 Bronze Layer (`ecommerce.bronze`)

Raw data loaded from CSV files. No transformations — schema enforcement only.

| Table Name | Rows | Description |
| --- | ---: | --- |
| `ecommerce.bronze.brz_brands` | 52 | Brand master data |
| `ecommerce.bronze.brz_category` | 10 | Product categories |
| `ecommerce.bronze.brz_products` | 50,000 | Product catalog |
| `ecommerce.bronze.brz_customers` | 300,000 | Customer records |
| `ecommerce.bronze.brz_calendar` | 95 | Date dimension source |
| `ecommerce.bronze.brz_order_items` | 183,378 | Transaction line items |

### 4.2 Silver Layer (`ecommerce.silver`)

Cleansed and standardized. Duplicates removed, types enforced, nulls handled.

| Table Name | Rows | Description |
| --- | ---: | --- |
| `ecommerce.silver.slv_brands` | 52 | Cleaned brand data |
| `ecommerce.silver.slv_category` | 8 | Cleaned categories (2 invalid removed) |
| `ecommerce.silver.slv_products` | 50,000 | Standardized products |
| `ecommerce.silver.slv_customers` | 299,700 | Deduplicated customers (300 removed) |
| `ecommerce.silver.slv_calendar` | 92 | Cleaned calendar dates |
| `ecommerce.silver.slv_order_items` | 183,378 | Cleaned order items |

### 4.3 Gold Layer (`ecommerce.gold`)

Business-ready star schema with dimensions, facts, and a denormalized view.

| Table Name | Rows | Type | Description |
| --- | ---: | --- | --- |
| `ecommerce.gold.gld_dim_products` | 50,000 | Dimension | Product dim (SKU, category, brand, price) |
| `ecommerce.gold.gld_dim_customers` | 299,700 | Dimension | Customer dim (region, country, state) |
| `ecommerce.gold.gld_dim_date` | 92 | Dimension | Date dim (year, month, day, weekend flag) |
| `ecommerce.gold.gld_fact_order_items` | 183,378 | Fact | Fact table with 19 columns |
| `ecommerce.gold.fact_transactions_denorm` | 183,378 | View | 34-column denormalized view (fact + all dims) |

### 4.4 Key Columns in `fact_transactions_denorm` (34 columns)

| Column | Type | Description |
| --- | --- | --- |
| `transaction_id` | STRING | Unique order identifier |
| `customer_id` | STRING | Customer reference |
| `net_amount` | DOUBLE | Sales amount (original currency) |
| `net_amount_inr` | BIGINT | Sales amount in INR |
| `transaction_date` | DATE | Date of transaction |
| `month_name` | STRING | Month name (Jan, Feb, ...) |
| `day_name` | STRING | Day of week (Monday, ...) |
| `is_weekend` | INT | 1 = weekend, 0 = weekday |
| `hour_of_day` | INT | Hour of transaction (0-23) |
| `sku` | STRING | Product SKU |
| `category_name` | STRING | Product category name |
| `brand_name` | STRING | Product brand name |
| `region` | STRING | Customer region (from dim_customers) |
| `quarter` | STRING | Quarter (Q1, Q2, Q3, Q4) |

---

## 5. ETL Pipeline — Notebooks & Job

### 5.1 Notebooks (6 PySpark + 1 SQL)

| # | Notebook Path | Layer | Purpose |
| --- | --- | --- | --- |
| 1 | `1_medallion_processing_dim/1_dim_bronze` | Bronze | Load dim CSVs (brands, categories, products, customers, calendar) |
| 2 | `1_medallion_processing_dim/2_dim_silver` | Silver | Cleanse and deduplicate dimension data |
| 3 | `1_medallion_processing_dim/3_dim_gold` | Gold | Build gld_dim_products, gld_dim_customers, gld_dim_date |
| 4 | `2_medallion_processing_fact/1_fact_bronze` | Bronze | Load order_items CSV |
| 5 | `2_medallion_processing_fact/2_fact_silver` | Silver | Cleanse order items |
| 6 | `2_medallion_processing_fact/3_fact_gold` | Gold | Build gld_fact_order_items with computed columns |
| 7 | `3_etl_workflow/denormalise_table_query.sql` | Gold | SQL file — creates fact_transactions_denorm view |

All notebooks use `mode("overwrite")` for idempotent reruns.

### 5.2 ETL Job — Task Dependency Graph

```
     dim_bronze -----> dim_silver -----> dim_gold --------\
                                                           \\n                                                             > denorm_view (SQL)
           /                                         /
     fact_bronze ----> fact_silver ----> fact_gold --/
```

Two parallel chains run simultaneously, then converge on the final SQL task.

### 5.3 Job Configuration

| Property | Value |
| --- | --- |
| Job Name | `ecommerce_etl_pipeline` |
| Job ID | 690661785206951 |
| Total Tasks | 7 |
| Compute | Serverless (all notebook tasks) |
| SQL Warehouse | 021d1a25077dfdaa (for denorm_view task) |
| Schedule | None (manual trigger) |
| Reruns | Idempotent (mode overwrite) |
| Status | All 7 tasks SUCCEEDED |

---

## 6. BI Dashboard — E-Commerce Sales Analytics

### 6.1 Dashboard Overview

| Property | Value |
| --- | --- |
| Dashboard Name | E-Commerce Sales Analytics |
| Dashboard ID | 01f1cd526c761666a202bb3c1d5b1064 |
| Pages | 2 (Page 1 + Sales page) |
| Total Widgets | 13 |
| Theme | Terminal (dark mode) |
| Dataset | E-Commerce Sales Metrics (local metric view) |
| Status | Published |

### 6.2 Dataset — Local Metric View

A single local metric view powers all widgets. Built on `fact_transactions_denorm` with a join to `gld_dim_customers` for region.

**Dimensions (9):**

| Dimension | Expression |
| --- | --- |
| `transaction_date` | `transaction_date` |
| `month` | `DATE_TRUNC('MONTH', transaction_date)` |
| `sku` | `sku` |
| `category_name` | `category_name` |
| `brand_name` | `brand_name` |
| `day_type` | `CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END` |
| `day_name` | `day_name` |
| `hour_of_day` | `hour_of_day` |
| `region` | `customers.region` (from join) |

**Measures (4):**

| Measure | Expression |
| --- | --- |
| `total_sales` | `SUM(net_amount_inr)` — INR currency format |
| `net_amount_sum` | `SUM(net_amount)` |
| `total_orders` | `COUNT(DISTINCT transaction_id)` |
| `total_customers` | `COUNT(DISTINCT customer_id)` |

### 6.3 Page 1 — Full Analytics Suite (10 Widgets)

| # | Widget Name | Type | Key Data |
| --- | --- | --- | --- |
| 1 | Total Sales | Counter | Rs 3,406,773,071 |
| 2 | Total Orders | Counter | 104,756 |
| 3 | Total Customers | Counter | 88,495 |
| 4 | Monthly Sales Trend | Line Chart | 3 months (Aug-Oct 2025) |
| 5 | Top 10 Products by Sales | Bar Chart | Top SKU: ECOT-CE-006E4 |
| 6 | Sales by Category | Bar Chart | Electronics leads (Rs 2.58B) |
| 7 | Sales by Brand | Bar Chart | VoltEdge leads (Rs 340M) |
| 8 | Sales by Region | Bar Chart | South region leads (Rs 824M) |
| 9 | Weekend vs Weekday Sales | Bar Chart | Weekday Rs 2.3B vs Weekend Rs 1.1B |
| 10 | Sales by Hour | Bar Chart | 24 hours distribution |

**Layout:**

```
Row 0-1:  [ Total Sales ] [ Total Orders ] [ Total Customers ]
Row 2-7:  [ Monthly Sales Trend (w8)        ] [ Top 10 Products ]
Row 8-13: [ Category  ] [ Brand    ] [ Region  ]
Row 14+:  [ Weekend vs Weekday (w6) ] [ Sales by Hour (w6)  ]
```

### 6.4 Page "Sales" — Tutorial Layout (3 Widgets)

| # | Widget Name | Type | Layout |
| --- | --- | --- | --- |
| 1 | Monthly Sales Trend | Line Chart | Top left (50% width) |
| 2 | Sales by Category | Bar Chart | Top right (50% width) |
| 3 | Hourly Sales Heatmap | Heatmap | Bottom (full width) |

**Heatmap Details:**

* X-axis: `hour_of_day` (0-23)
* Y-axis: `day_name` (Monday-Sunday)
* Color intensity: `SUM(net_amount)` with viridis color ramp
* Data: 168 cells (24 hours x 7 days)

**Layout:**

```
Row 0-5:  [ Monthly Sales Trend (w6) ] [ Sales by Category (w6) ]
Row 6-11: [ Hourly Sales Heatmap (full width w12)              ]
```

---

## 7. Key Features

| Feature | Description |
| --- | --- |
| Medallion Architecture | 3-layer Bronze/Silver/Gold pipeline with clear separation |
| Unity Catalog Governance | Centralized catalog, schemas, and volumes |
| Serverless Compute | All ETL tasks run serverless — no cluster management |
| Idempotent Pipelines | `mode("overwrite")` ensures safe reruns |
| Parallel Execution | Dimension and fact chains run simultaneously |
| Denormalized View | Single 34-column view joins all dims for fast queries |
| Local Metric View | Semantic layer with measures + dimensions (not raw SQL) |
| AI/BI Dashboard | Native Databricks dashboards with 13 widgets across 2 pages |
| Heatmap Visualization | Native heatmap with viridis color ramp for hourly patterns |
| Dark Theme | Terminal dark theme for clean, modern appearance |
| Published & Shared | Dashboard published with shared data permissions |

---

## 8. Tech Stack

| Component | Technology |
| --- | --- |
| Cloud Platform | Databricks Lakehouse |
| Data Storage | Unity Catalog tables + UC Volumes (raw CSVs) |
| Processing | PySpark (notebooks) + SQL (denormalized view) |
| Orchestration | Lakeflow Jobs (7-task serverless job) |
| Visualization | Databricks AI/BI Dashboards |
| Data Modeling | Local Metric View (semantic layer) |
| Compute | Serverless (notebooks) + SQL Warehouse (SQL tasks) |

---

## 9. End-to-End Workflow

```
Step 1: RAW CSVs uploaded to UC Volume
            |
Step 2: Bronze notebooks load CSVs into tables (schema enforced)
            |
Step 3: Silver notebooks cleanse, deduplicate, standardize
            |
Step 4: Gold notebooks build star schema (dim + fact tables)
            |
Step 5: SQL file creates denormalized view (fact + all dimensions)
            |
Step 6: Lakeflow Job orchestrates all 7 tasks (serverless, parallel)
            |
Step 7: Local Metric View built on denormalized view (semantic layer)
            |
Step 8: AI/BI Dashboard with 13 widgets across 2 pages (dark theme)
            |
Step 9: Dashboard published and shared with stakeholders
```

---

## 10. Project Link

**Workspace Project Folder:**
https://dbc-5a4434e4-beed.cloud.databricks.com/browse/folders/4050478014456898?o=7474658929033152

---

## 11. Summary

This project demonstrates a complete end-to-end data engineering and analytics workflow on Databricks:

1. **Ingest** raw e-commerce CSVs into Unity Catalog Bronze tables
2. **Transform** data through Silver (cleansed) and Gold (analytics-ready) layers
3. **Orchestrate** the pipeline with a 7-task serverless Lakeflow Job
4. **Visualize** results in an AI/BI Dashboard with 13 widgets across 2 pages
5. **Publish** the dashboard for stakeholder access

The entire pipeline is serverless, idempotent, and governed by Unity Catalog — requiring zero cluster management and enabling safe, repeatable reruns.