# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer — Raw Transaction Storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC This notebook is responsible for storing raw AML transaction datasets into Delta Lake Bronze tables for scalable and reliable downstream processing.
# MAGIC
# MAGIC ## Key Responsibilities
# MAGIC - Store raw transaction data
# MAGIC - Preserve ingestion integrity
# MAGIC - Enable scalable distributed processing
# MAGIC - Support enterprise medallion architecture
# MAGIC
# MAGIC ## Technologies Used
# MAGIC - PySpark
# MAGIC - Delta Lake
# MAGIC - Databricks
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

transactions_train_df = spark.read.format("csv") \
    .option("header", "true") \
        .option("inferSchema", "true") \
            .load("/Volumes/workspace/default/nike_data/transactions_train.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Raw Data into Bronze Delta Table

# COMMAND ----------

transactions_train_df.write.format("delta").mode("overwrite").saveAsTable("aml_bronze_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Bronze Delta Table
# MAGIC Read Bronze Table

# COMMAND ----------

bronze_df = spark.table("aml_bronze_transactions")
display(bronze_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count Records

# COMMAND ----------

print("Bronze Layer Record Count:", bronze_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Schema

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Delta Format

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL aml_bronze_transactions;

# COMMAND ----------

# DBTITLE 1,Load Fraud Labels
# MAGIC %md
# MAGIC ### Load Fraud Labels Data

# COMMAND ----------

# DBTITLE 1,Load fraud labels CSV
fraud_labels_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/nike_data/fraud_labels_train.csv")

# COMMAND ----------

# DBTITLE 1,Write fraud labels to Bronze
fraud_labels_df.write.format("delta").mode("overwrite").saveAsTable("aml_fraud_labels")

# COMMAND ----------

# DBTITLE 1,Verify fraud labels table
fraud_labels = spark.table("aml_fraud_labels")
print(f"Fraud Labels Record Count: {fraud_labels.count()}")
fraud_labels.printSchema()