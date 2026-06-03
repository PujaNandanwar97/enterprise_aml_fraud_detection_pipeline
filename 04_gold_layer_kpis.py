# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — AML Analytics & Business KPIs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook creates business-ready AML analytics and fraud monitoring KPIs from the Silver Layer.
# MAGIC
# MAGIC ## Key Responsibilities
# MAGIC
# MAGIC - Fraud Analytics
# MAGIC - Risk Monitoring
# MAGIC - Transaction Trend Analysis
# MAGIC - Business KPI Generation
# MAGIC - Executive Reporting
# MAGIC
# MAGIC ## Technologies Used
# MAGIC
# MAGIC - PySpark
# MAGIC - Delta Lake
# MAGIC - Databricks SQL
# MAGIC - Window Functions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 - Load Sliver Layer Data

# COMMAND ----------

sliver_df = spark.table("aml_sliver_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - Load Fraud Labels Dataset

# COMMAND ----------

fraud_labels_df = spark.read.format("csv") \
    .option("header", "true") \
        .option("inferSchema", "true") \
            .load("/Volumes/workspace/default/nike_data/fraud_labels_train.csv")           

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 - Standardize Column Names

# COMMAND ----------

fraud_labels_clean = fraud_labels_df.withColumnRenamed("_c0","company_window_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 - Fraud Vs Non-Fraud Distribution

# COMMAND ----------

fraud_distribution = fraud_labels_clean.groupBy(
    "isFraudUser").count()
display(fraud_distribution)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 - Load Companies Dataset

# COMMAND ----------

companies_df = spark.read.format("csv") \
    .option("header", "true") \
        .option("inferSchema", "true") \
            .load("/Volumes/workspace/default/nike_data/companies_train.csv")        


# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 - Total Companies KPI

# COMMAND ----------

total_companies = fraud_labels_clean.count()
print("Total Companies:", total_companies)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 - Fraud Companies KPI

# COMMAND ----------

from pyspark.sql.functions import *

fraud_companies = fraud_labels_clean.filter(
    col("isFraudUser") == True).count()
print("Fraud Companies:", fraud_companies)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 8 - Non Fraud Companies KPI

# COMMAND ----------

non_fraud_companies = fraud_labels_clean.filter(
    col("isFraudUser") == False).count()
print("Non Fraud Companies:", non_fraud_companies)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 9 - Fraud Percentage KPI

# COMMAND ----------

fraud_percentage = (fraud_companies / 
                    total_companies) * 100
print("Fraud Percentage:", fraud_percentage.__round__(2),"%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 10 - Fraud Percentage Table

# COMMAND ----------

fraud_percentage_df = fraud_labels_clean.groupBy("isFraudUser").count() \
    .withColumn("percentage", round(col("count") / total_companies * 100,
                                    2 ))
display(fraud_percentage_df)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 11 - Top Risk Windows
# MAGIC

# COMMAND ----------

fraud_windows = fraud_labels_clean.filter(
    col("isFraudUser") == True
)
top_risk_windows = fraud_windows.groupBy("company_window_id").count() \
    .orderBy(col("count").desc())
display(top_risk_windows.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 12 - Fraud Trend Distribution

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

fraud_windows_analysis = fraud_labels_clean.withColumn(
    "window_number",
    regexp_extract(
        col("company_window_id"),
        r'window_(\d+)',
        1
    )
)

fraud_trend = fraud_windows_analysis.groupBy(
    "window_number", "isFraudUser"
).count()
display(fraud_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 12 - Fraud Risk Ranking Using Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

risk_window = Window.orderBy(
    col("count").desc()
)

risk_rank_df = top_risk_windows.withColumn(
    "risk_rank",
    dense_rank().over(risk_window)
)
display(risk_rank_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 13 - Save Gold Fraud Analytics Table

# COMMAND ----------

risk_rank_df.write \
.mode("overwrite") \
    .format("delta") \
        .saveAsTable("aml_gold_risk_ranking")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 14 - Verify Table

# COMMAND ----------

display(
    spark.table(
        "aml_gold_risk_ranking"
    ).limit(20)
)