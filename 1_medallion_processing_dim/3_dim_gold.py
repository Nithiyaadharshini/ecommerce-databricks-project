# Databricks notebook source
# DBTITLE 1,Silver to Gold: Dimension Tables
# MAGIC %md
# MAGIC ### Silver to Gold: Building BI Ready Tables

# COMMAND ----------

# DBTITLE 1,Imports and Catalog
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

catalog_name = "ecommerce"

# COMMAND ----------

# DBTITLE 1,Products
# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

# DBTITLE 1,Read Silver Tables
df_products = spark.table(f"{catalog_name}.silver.slv_products")
df_brands = spark.table(f"{catalog_name}.silver.slv_brands")
df_category = spark.table(f"{catalog_name}.silver.slv_category")

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Create gld_dim_products
# MAGIC %sql
# MAGIC -- Build brands x category mapping and write Gold table
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC
# MAGIC WITH brands_categories AS (
# MAGIC   SELECT
# MAGIC     b.brand_name,
# MAGIC     b.brand_code,
# MAGIC     c.category_name,
# MAGIC     c.category_code
# MAGIC   FROM v_brands b
# MAGIC   INNER JOIN v_category c
# MAGIC   ON b.category_code = c.category_code
# MAGIC )
# MAGIC SELECT
# MAGIC   p.product_id,
# MAGIC   p.sku,
# MAGIC   p.category_code,
# MAGIC   COALESCE(bc.category_name, 'Not Available') AS category_name,
# MAGIC   p.brand_code,
# MAGIC   COALESCE(bc.brand_name, 'Not Available') AS brand_name,
# MAGIC   p.color,
# MAGIC   p.size,
# MAGIC   p.material,
# MAGIC   p.weight_grams,
# MAGIC   p.length_cm,
# MAGIC   p.width_cm,
# MAGIC   p.height_cm,
# MAGIC   p.rating_count,
# MAGIC   p.file_name,
# MAGIC   p.ingest_timestamp
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brands_categories bc
# MAGIC   ON p.brand_code = bc.brand_code;

# COMMAND ----------

# DBTITLE 1,Customers
# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

# DBTITLE 1,Create gld_dim_customers
# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {"VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"}
# United Kingdom states
uk_region = {"ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"}
# United States states
us_region = {"MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", "NY": "NorthEast", "TX": "South"}
# UAE states
uae_region = {"AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"}
# Singapore states
singapore_region = {"SG": "Singapore"}
# Canada states
canada_region = {"BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"}

country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}

# Flatten country_state_map into a list of Rows
rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))

df_region_mapping = spark.createDataFrame(rows)

df_silver = spark.table(f'{catalog_name}.silver.slv_customers')

df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')
df_gold = df_gold.fillna({'region': 'Other'})

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

# DBTITLE 1,Date
# MAGIC %md
# MAGIC ### Date/Calendar

# COMMAND ----------

# DBTITLE 1,Create gld_dim_date
df_silver = spark.table(f'{catalog_name}.silver.slv_calendar')

df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
df_gold = df_gold.withColumn(
    "is_weekend",
    F.when(F.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0)
)

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "_ingested_at", "_source_file"]
df_gold = df_gold.select(desired_columns_order)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")