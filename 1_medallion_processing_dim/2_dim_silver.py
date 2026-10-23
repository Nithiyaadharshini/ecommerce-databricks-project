# Databricks notebook source
# DBTITLE 1,Bronze to Silver: Dimension Cleaning
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleaning and Transformation for Dimension Tables

# COMMAND ----------

# DBTITLE 1,Imports and Catalog
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

catalog_name = 'ecommerce'

# COMMAND ----------

# DBTITLE 1,Brands
# MAGIC %md
# MAGIC ### Brands

# COMMAND ----------

# DBTITLE 1,Clean and Write Brands
df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')

df_silver = df_bronze.withColumn("brand_name", F.trim(F.col("brand_name")))
df_silver = df_silver.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]', ''))

anomalies = {"GROCERY": "GRCY", "BOOKS": "BKS", "TOYS": "TOY"}
df_silver = df_silver.replace(to_replace=anomalies, subset=["category_code"])

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# DBTITLE 1,Category
# MAGIC %md
# MAGIC ### Category

# COMMAND ----------

# DBTITLE 1,Clean and Write Category
df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")

df_silver = df_bronze.dropDuplicates(['category_code'])
df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# DBTITLE 1,Products
# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

# DBTITLE 1,Clean and Write Products
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products")

# Fix weight_grams: remove 'g' and cast to int
df_silver = df_bronze.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
)

# Fix length_cm: replace comma with dot and cast to float
df_silver = df_silver.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)

# Uppercase category_code and brand_code
df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code"))) \
    .withColumn("brand_code", F.upper(F.col("brand_code")))

# Fix spelling mistakes in material
df_silver = df_silver.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Alumium", "Aluminum")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)

# Convert negative rating_count to positive
df_silver = df_silver.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
     .otherwise(F.lit(0))
)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# DBTITLE 1,Customers
# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

# DBTITLE 1,Clean and Write Customers
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# Drop rows where customer_id is null
df_silver = df_bronze.dropna(subset=["customer_id"])

# Fill null phone values with 'Not Available'
df_silver = df_silver.fillna("Not Available", subset=["phone"])

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# DBTITLE 1,Calendar
# MAGIC %md
# MAGIC ### Calendar/Date

# COMMAND ----------

# DBTITLE 1,Clean and Write Calendar
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar")

# Convert string to date (handle mixed date formats)
from pyspark.sql.functions import to_date
df_silver = df_bronze.withColumn("date", 
    F.coalesce(
        F.to_date(F.col("date"), "dd-MM-yyyy"),
        F.to_date(F.col("date"), "yyyy-MM-dd")
    )
)

# Remove duplicate rows
df_silver = df_silver.dropDuplicates(['date'])

# Capitalize first letter of each word in day_name
df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))

# Convert negative week_of_year to positive
df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))

# Enhance quarter and week columns
df_silver = df_silver.withColumn("quarter", 
    F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))
df_silver = df_silver.withColumn("week_of_year", 
    F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))

# Rename week_of_year to week
df_silver = df_silver.withColumnRenamed("week_of_year", "week")

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")