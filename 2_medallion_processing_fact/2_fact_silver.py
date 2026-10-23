# Databricks notebook source
# DBTITLE 1,Bronze to Silver: Fact Cleaning
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleansing and Transformation

# COMMAND ----------

# DBTITLE 1,Imports and Catalog
from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

catalog_name = 'ecommerce'

# COMMAND ----------

# DBTITLE 1,Transform Fact Data
df = spark.table(f'{catalog_name}.bronze.brz_order_items')

# Drop any duplicates
df = df.dropDuplicates(["order_id", "item_seq"])

# Convert 'Two' to 2 and cast to Integer
df = df.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")
)

# Remove any '$' or other symbols from unit_price
df = df.withColumn(
    "unit_price",
    F.regexp_replace("unit_price", "[$]", "").cast("double")
)

# Remove '%' from discount_pct and cast to double
df = df.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

# Coupon code processing (convert to lower)
df = df.withColumn(
    "coupon_code", F.lower(F.trim(F.col("coupon_code")))
)

# Channel processing
df = df.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel")),
)

# Convert dt (string to date)
df = df.withColumn(
    "dt",
    F.to_date("dt", "yyyy-MM-dd")
)

# Convert order_ts (string to timestamp, handle mixed formats)
df = df.withColumn(
    "order_ts",
    F.coalesce(
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")
    )
)

# Convert item_seq (string to integer)
df = df.withColumn(
    "item_seq",
    F.col("item_seq").cast("int")
)

# Convert tax_amount (string to double, strip non-numeric)
df = df.withColumn(
    "tax_amount",
    F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double")
)

# Add processed time
df = df.withColumn(
    "processed_time", F.current_timestamp()
)

# COMMAND ----------

# DBTITLE 1,Write to Silver
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_order_items")