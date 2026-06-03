# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Data Cleansing & Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Objective
# MAGIC
# MAGIC This notebook transforms raw AML transaction data from the Bronze Layer into a clean, validated, and analytics-ready Silver Layer.
# MAGIC
# MAGIC ###  Key Responsibilities
# MAGIC
# MAGIC - Data Quality Validation
# MAGIC - Null Handling
# MAGIC - Duplicate Detection
# MAGIC - Transaction Integrity Checks
# MAGIC - Feature Standardization
# MAGIC
# MAGIC ### Technologies Used
# MAGIC
# MAGIC - PySpark
# MAGIC - Delta Lake
# MAGIC - Databricks
# MAGIC - SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 - Read Bronze Layer Data

# COMMAND ----------

bronze_df = spark.table("aml_bronze_transactions")
display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - Data Quality Assessment

# COMMAND ----------

print("Bronze Record Count:", bronze_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 - Remove Duplicates

# COMMAND ----------

sliver_df = bronze_df.dropDuplicates(["transactionId"])
print("Sliver Record Count:", sliver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 - Remove Null Transaction IDs

# COMMAND ----------

from pyspark.sql.functions import col

sliver_df = sliver_df.filter(
    col("transactionId").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 - Verify Data

# COMMAND ----------

display(sliver_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 - Create Quality Metrics

# COMMAND ----------

total_records = bronze_df.count()

sliver_records = sliver_df.count()

removed_records = total_records - sliver_records

print("Total Bronze Records:", total_records)

print("Sliver Records:", sliver_records)

print("Removed Records:", removed_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 - Writing clean Data into Sliver Layer

# COMMAND ----------

sliver_df.write.format("delta") \
    .mode("overwrite") \
        .saveAsTable("aml_sliver_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 8 - Verify Sliver Table

# COMMAND ----------

sliver_check_df = spark.table("aml_bronze_transactions")
display(sliver_check_df.limit(10))