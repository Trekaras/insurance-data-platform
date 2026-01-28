# Databricks notebook source
# 01_bronze_ingest.py
# Purpose: Read landing CSV extracts and write Bronze Delta tables with ingestion metadata.

from pyspark.sql import functions as F

# -----------------------------
# 1) Parameters (Databricks Widgets)
# -----------------------------
# In Databricks notebook, widgets allow you to pass parameters from Jobs/ADF
try:
    dbutils.widgets.text("run_date", "")  # format: YYYY-MM-DD (optional)
    dbutils.widgets.text("base_path", "dbfs:/FileStore/insurance/landing")  # landing root
except Exception:
    # Allows running outside notebook context (e.g., local tests)
    pass

def get_widget(name: str, default: str = "") -> str:
    try:
        v = dbutils.widgets.get(name)
        return v if v else default
    except Exception:
        return default

run_date = get_widget("run_date", "")  # if empty -> current_date
base_path = get_widget("base_path", "dbfs:/FileStore/insurance/landing")

# If run_date not provided, use today's date
if not run_date:
    run_date = spark.sql("SELECT date_format(current_date(), 'yyyy-MM-dd') AS d").collect()[0]["d"]

print(f"run_date = {run_date}")
print(f"base_path = {base_path}")

# -----------------------------
# 2) Target database/schema (Bronze)
# -----------------------------
# Create a schema/database to keep tables organized.
# If you use Unity Catalog, you might need: <catalog>.<schema>
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# -----------------------------
# 3) Helper: Read CSV + add metadata
# -----------------------------
def read_csv_with_metadata(path: str):
    df = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(path)
    )

    # Add metadata columns
    df = (
        df.withColumn("ingestion_date", F.lit(run_date))  # string 'YYYY-MM-DD' (good for partition)
          .withColumn("source_file", F.input_file_name())
          .withColumn("load_ts", F.current_timestamp())
    )
    return df

# -----------------------------
# 4) Source paths (Policy / Claims / Billing)
# -----------------------------
policy_path  = f"{base_path}/policy"
claims_path  = f"{base_path}/claims"
billing_path = f"{base_path}/billing"

print("Policy path :", policy_path)
print("Claims path :", claims_path)
print("Billing path:", billing_path)

# -----------------------------
# 5) Read landing files
# -----------------------------
df_policy  = read_csv_with_metadata(policy_path)
df_claims  = read_csv_with_metadata(claims_path)
df_billing = read_csv_with_metadata(billing_path)

# Optional: quick sanity checks
print("Policy rows :", df_policy.count())
print("Claims rows :", df_claims.count())
print("Billing rows:", df_billing.count())

# -----------------------------
# 6) Write to Bronze Delta tables (Rerun-safe overwrite by ingestion_date)
# -----------------------------
# Why overwrite by partition?
# If you rerun the same run_date, it won't duplicate.
# This is a common real-world pattern for Bronze (daily batch loads).

def write_bronze(df, table_name: str):
    full_table = f"bronze.{table_name}"
    (
        df.write.format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .option("replaceWhere", f"ingestion_date = '{run_date}'")
          .saveAsTable(full_table)
    )
    print(f"✅ Written {full_table} for ingestion_date={run_date}")

write_bronze(df_policy,  "bronze_policy")
write_bronze(df_claims,  "bronze_claims")
write_bronze(df_billing, "bronze_billing")

# -----------------------------
# 7) Post-write validation
# -----------------------------
spark.sql("SELECT ingestion_date, COUNT(*) AS cnt FROM bronze.bronze_policy GROUP BY ingestion_date ORDER BY ingestion_date DESC").show()
spark.sql("SELECT ingestion_date, COUNT(*) AS cnt FROM bronze.bronze_claims GROUP BY ingestion_date ORDER BY ingestion_date DESC").show()
spark.sql("SELECT ingestion_date, COUNT(*) AS cnt FROM bronze.bronze_billing GROUP BY ingestion_date ORDER BY ingestion_date DESC").show()