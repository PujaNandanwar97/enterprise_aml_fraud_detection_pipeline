# Databricks notebook source
# MAGIC %md
# MAGIC # Window Function Analytics Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Window Function Analytics Layer
# MAGIC
# MAGIC ### Objective
# MAGIC Apply advanced PySpark Window Functions to identify fraud trends, risk rankings, and transaction behavior patterns.
# MAGIC
# MAGIC ### Techniques Used
# MAGIC - Row Number
# MAGIC - Rank
# MAGIC - Dense Rank
# MAGIC - Lag
# MAGIC - Lead
# MAGIC - Partition By
# MAGIC - Order By
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 - Load Fraud Detection Dataset

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

risk_df = spark.table(
    "aml_fraud_detection_logic"
)
display(risk_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - Row Number

# COMMAND ----------

from pyspark.sql.functions import row_number as rn, col
from pyspark.sql.window import Window

row_window = Window.orderBy(col("avg_risk_score").desc())
row_number_df = risk_df.withColumn(
    "row_number",
    rn().over(row_window)
)
display(row_number_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 - Rank()

# COMMAND ----------

rank_df = risk_df.withColumn(
    "rank",
    rank().over(row_window)
)
display(rank_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 - Dense Rank()

# COMMAND ----------

dense_rank_df = risk_df.withColumn(
    "dense_rank",
    dense_rank().over(row_window)
)
display(dense_rank_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 - Create Company Risk History Dataset (Lag)

# COMMAND ----------

lag_df = risk_df.withColumn(
    "pervious_risk_score",
    lag("avg_risk_score", 1).over(row_window)
)
display(lag_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 - Lead

# COMMAND ----------

lead_df = risk_df.withColumn(
    "next_risk_score",
    lead("avg_risk_score" ,1).over(row_window)
)
display(lead_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 - Running Risk Score Analysis

# COMMAND ----------

from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

running_window = Window.orderBy(
    col("avg_risk_score").desc()
).rangeBetween(
    Window.unboundedPreceding,
    Window.currentRow
)

running_df = risk_df.withColumn(
    "running_avg_risk",
    avg("avg_risk_score").over(running_window)
)
display(running_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 8 - Top 10 High Risk Companies

# COMMAND ----------

top_risk_companies = risk_df.orderBy(col("avg_risk_score").desc())
display(top_risk_companies.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 9 - Save Window Analytics Table

# COMMAND ----------

running_df.write \
    .mode("overwrite") \
        .format("delta") \
            .saveAsTable("aml_window_analytics")
        

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 10 - Verify Table

# COMMAND ----------

display(
    spark.table("aml_window_analytics").limit(20)
)