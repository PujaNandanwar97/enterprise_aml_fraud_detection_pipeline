# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Analytics Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Analytics Layer
# MAGIC
# MAGIC ### Objective
# MAGIC Perform advanced SQL-based fraud analytics on curated Delta Tables.
# MAGIC
# MAGIC ### Technologies
# MAGIC - Spark SQL
# MAGIC - Delta Lake
# MAGIC - Databricks SQL
# MAGIC
# MAGIC ### Business Goal
# MAGIC Generate actionable fraud intelligence using SQL queries.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 - Verify Fraud Detection Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM aml_fraud_detection_logic
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - KPI 1 - Total Companies

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC COUNT(*) AS total_companies
# MAGIC FROM aml_fraud_detection_logic

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 - KPI 2 - Average Risk Score

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC ROUND(AVG(avg_risk_score),2) AS avg_risk_score
# MAGIC FROM aml_fraud_detection_logic

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 - KPI 3 - Top Risk Companies

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM aml_fraud_detection_logic
# MAGIC ORDER BY avg_risk_score DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 - KPI 4 - Risk Rank Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC risk_rank,
# MAGIC COUNT(*) AS companies
# MAGIC FROM aml_fraud_detection_logic
# MAGIC GROUP BY risk_rank
# MAGIC ORDER BY risk_rank;

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 - KPI 5 - Top 10 Ranked Companies

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC company_id,
# MAGIC avg_risk_score,
# MAGIC risk_rank
# MAGIC FROM aml_fraud_detection_logic
# MAGIC ORDER BY risk_rank
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 - KPI 6 - Risk Categories

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   company_id,
# MAGIC   avg_risk_score,
# MAGIC   CASE
# MAGIC     WHEN avg_risk_score >= 80 THEN 'Critical'
# MAGIC     WHEN avg_risk_score >= 60 THEN 'High'
# MAGIC     WHEN avg_risk_score >= 40 THEN 'Medium'
# MAGIC     ELSE 'Low'
# MAGIC   END AS risk_category
# MAGIC FROM aml_fraud_detection_logic
# MAGIC ORDER BY avg_risk_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 8 - KPI 7 - Risk Category Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  CASE
# MAGIC     WHEN avg_risk_score >= 80 THEN 'Critical'
# MAGIC     WHEN avg_risk_score >= 60 THEN 'High'
# MAGIC     WHEN avg_risk_score >= 40 THEN 'Medium'
# MAGIC     ELSE 'Low'
# MAGIC   END AS risk_category,
# MAGIC COUNT(*) AS company_count
# MAGIC FROM aml_fraud_detection_logic
# MAGIC GROUP BY 
# MAGIC  CASE
# MAGIC     WHEN avg_risk_score >= 80 THEN 'Critical'
# MAGIC     WHEN avg_risk_score >= 60 THEN 'High'
# MAGIC     WHEN avg_risk_score >= 40 THEN 'Medium'
# MAGIC     ELSE 'Low'
# MAGIC   END
# MAGIC ORDER BY company_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 9 - KPI 8 - Highest Fraud Exposure

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC company_id,
# MAGIC avg_risk_score,
# MAGIC risk_rank
# MAGIC FROM aml_fraud_detection_logic
# MAGIC WHERE avg_risk_score > 80 
# MAGIC ORDER BY avg_risk_score DESC;