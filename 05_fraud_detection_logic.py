# Databricks notebook source
# MAGIC %md
# MAGIC ###  Fraud Detection Logic Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enterprise AML Fraud Detection Engine
# MAGIC
# MAGIC ###  Fraud Detection Logic Layer
# MAGIC
# MAGIC ### Objective
# MAGIC Build fraud detection rules and risk scoring logic to identify suspicious companies based on historical transaction behavior.
# MAGIC
# MAGIC ### Technologies
# MAGIC - PySpark
# MAGIC - Delta Lake
# MAGIC - SQL Analytics
# MAGIC - Window Functions
# MAGIC
# MAGIC ### Business Goal
# MAGIC Detect high-risk entities and generate fraud risk scores for AML monitoring.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Sliver and Gold Dataset

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

sliver_df = spark.table("aml_sliver_transactions")

fraud_labels_df = spark.table("aml_fraud_labels")
display(sliver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - Fraud Label Distribution

# COMMAND ----------

fraud_labels_df.groupBy("isFraudUser").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 - Create Fraud Flag

# COMMAND ----------

fraud_df = fraud_labels_df.withColumn(
    "fraud_flag",
    when(col("isFraudUser") == True, 1).otherwise(0)
)
display(fraud_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 - Create Risk Classification

# COMMAND ----------

risk_df = fraud_df.withColumn(
    "risk_category",
    when(col("fraud_flag") == 1, "High Risk")
    .otherwise("Low Risk")
)
display(risk_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 - Risk Category Distribution

# COMMAND ----------

risk_summary = risk_df.groupBy("risk_category").count()
display(risk_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 - Extract Company Identifier

# COMMAND ----------

risk_df = risk_df.withColumn(
    "company_id",
    regexp_extract(
        col("_c0"),
        r'company_(\d+)',
        1
    )
)
display(risk_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 - Fraud Risk Scoring Engine

# COMMAND ----------

risk_df = risk_df.withColumn(
    "risk_score",
    when(col("fraud_flag") == 1, 100)
    .otherwise(20)
)
display(risk_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 8 - Rank Highest Risk Comapanies 
# MAGIC (Top Risk Companies)

# COMMAND ----------

company_risk = risk_df.groupBy("company_id").agg(avg("risk_score").alias("avg_risk_score"))

risk_window = Window.orderBy(
    col("avg_risk_score").desc()
)
company_risk_rank = company_risk.withColumn(
    "risk_rank",
    dense_rank().over(risk_window)
)
display(company_risk_rank.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 9 - Save Fraud Logic Layer

# COMMAND ----------

company_risk_rank.write \
    .mode("overwrite") \
        .format("delta") \
            .saveAsTable("aml_fraud_detection_logic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 10 - Verify

# COMMAND ----------

display(
    spark.table(
        "aml_fraud_detection_logic"
    ).limit(20)
)