# Databricks notebook source
# MAGIC %md
# MAGIC ## Enterprise AML Transaction Monitoring Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Ingestion Layer
# MAGIC
# MAGIC ### Project Objective
# MAGIC This notebook is responsible for ingesting raw Anti-Money Laundering (AML) transaction datasets into the Databricks environment for scalable distributed processing using PySpark.
# MAGIC
# MAGIC ### Technologies Used
# MAGIC - PySpark
# MAGIC - Databricks
# MAGIC - Delta Lake
# MAGIC - SQL
# MAGIC
# MAGIC ### Dataset Description
# MAGIC The dataset contains anonymized corporate banking transactions used for Anti-Money Laundering (AML) monitoring and fraud analytics.
# MAGIC
# MAGIC ### Key Data Sources
# MAGIC - Transaction Records
# MAGIC - Fraud Labels
# MAGIC - Company Information
# MAGIC - Event Order Data
# MAGIC - Time-Series Transaction IDs

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 - Import Required Libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - Upload Dataset Files into Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 - Load Raw Transaction Dataset using PySpark

# COMMAND ----------

transactions_train_df = spark.read.format("csv") \
    .option("header", True) \
        .option("inferSchema", True) \
            .load("/Volumes/workspace/default/nike_data/transactions_train.csv")

# COMMAND ----------

display(transactions_train_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 - Inspect Dataset Schema

# COMMAND ----------

transactions_train_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 - Count Total Records

# COMMAND ----------

transactions_train_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 -Initial Data Exploration & Quality Checks 

# COMMAND ----------

display(transactions_train_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 - Check Missing Values

# COMMAND ----------

from pyspark.sql.functions import col, count, when

missing_values_df = transactions_train_df.select([count(when(col(c).isNull(), c)).alias(c) for c in transactions_train_df.columns])
display(missing_values_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 8 - Duplicate Transaction Detection

# COMMAND ----------

duplicate_count = transactions_train_df.count() - transactions_train_df.dropDuplicates().count()

print(f"Total Duplicate Records: {duplicate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 9 - Transaction Integrity Validation

# COMMAND ----------

invalid_transaction_df = transactions_train_df.filter(
    (col("transactionId").isNull())
)
display(invalid_transaction_df)

# COMMAND ----------

duplicate_transactions = transactions_train_df.groupBy("transactionId") \
    .count() \
        .filter(col("count") > 1)
display(duplicate_transactions)        

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC