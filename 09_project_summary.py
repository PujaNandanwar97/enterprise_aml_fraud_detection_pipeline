# Databricks notebook source
# MAGIC %md
# MAGIC #Enterprise AML Fraud Detection & Risk Analytics Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Built Using Databricks, PySpark, Delta Lake & SQL
# MAGIC Author
# MAGIC Puja Nandanwar
# MAGIC
# MAGIC Project Type
# MAGIC Enterprise-Scale Data Engineering Project
# MAGIC
# MAGIC Domain
# MAGIC ### Banking | Anti-Money Laundering (AML) | Fraud Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Problem
# MAGIC
# MAGIC Financial institutions process millions of transactions daily.
# MAGIC
# MAGIC Identifying suspicious entities and detecting money laundering activities manually is inefficient and highly error-prone.
# MAGIC
# MAGIC This project builds an enterprise-scale AML monitoring pipeline capable of processing large transaction datasets, generating fraud intelligence, and identifying high-risk entities using scalable big-data technologies.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset Overview
# MAGIC
# MAGIC ### Dataset Name
# MAGIC Time Series of Transactions - Money Laundering
# MAGIC
# MAGIC ### Dataset Size
# MAGIC Approximately 2.3 GB
# MAGIC
# MAGIC ### Total Records Processed
# MAGIC ~940,000+
# MAGIC
# MAGIC ### Data Sources
# MAGIC
# MAGIC - Transactions
# MAGIC - Fraud Labels
# MAGIC - Company Information
# MAGIC - Time Series IDs
# MAGIC - Event Order Data
# MAGIC
# MAGIC ### Characteristics
# MAGIC
# MAGIC - Differential Privacy Protected
# MAGIC - PCA Transformed Features
# MAGIC - AML Research Dataset
# MAGIC - Enterprise-Scale Banking Data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medallion Architecture
# MAGIC
# MAGIC ### Bronze Layer
# MAGIC Raw transaction ingestion and storage.
# MAGIC
# MAGIC ### Silver Layer
# MAGIC Data cleansing, validation, and duplicate handling.
# MAGIC
# MAGIC ### Gold Layer
# MAGIC Fraud analytics, business KPIs, and risk monitoring.
# MAGIC
# MAGIC ### Fraud Detection Layer
# MAGIC Risk scoring and suspicious company identification.
# MAGIC
# MAGIC ### Window Analytics Layer
# MAGIC Advanced ranking and trend analysis using Spark Window Functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technology Stack
# MAGIC
# MAGIC ### Data Processing
# MAGIC - PySpark
# MAGIC
# MAGIC ### Storage
# MAGIC - Delta Lake
# MAGIC
# MAGIC ### Platform
# MAGIC - Databricks
# MAGIC
# MAGIC ### Query Engine
# MAGIC - Spark SQL
# MAGIC
# MAGIC ### Architecture
# MAGIC - Medallion Architecture
# MAGIC
# MAGIC ### Optimization
# MAGIC - Repartition
# MAGIC - Cache
# MAGIC - Persist
# MAGIC
# MAGIC ### Analytics
# MAGIC - Window Functions
# MAGIC - Risk Ranking
# MAGIC - Fraud Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Key Features Implemented
# MAGIC
# MAGIC ### Data Engineering
# MAGIC
# MAGIC - Enterprise Data Ingestion
# MAGIC - Bronze-Silver-Gold Architecture
# MAGIC - Delta Lake Storage
# MAGIC - Data Quality Validation
# MAGIC - Duplicate Detection
# MAGIC
# MAGIC ### Fraud Analytics
# MAGIC
# MAGIC - Fraud Distribution Analysis
# MAGIC - Fraud Risk Scoring
# MAGIC - High-Risk Company Detection
# MAGIC - Risk Ranking
# MAGIC
# MAGIC ### Advanced Analytics
# MAGIC
# MAGIC - Row Number
# MAGIC - Rank
# MAGIC - Dense Rank
# MAGIC - Lag
# MAGIC - Lead
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC
# MAGIC - Repartitioning
# MAGIC - Caching
# MAGIC - Persistence
# MAGIC - Execution Plan Analysis
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Impact
# MAGIC
# MAGIC This solution enables financial institutions to:
# MAGIC
# MAGIC - Detect suspicious entities faster
# MAGIC - Improve AML monitoring efficiency
# MAGIC - Generate fraud intelligence
# MAGIC - Prioritize investigations using risk scores
# MAGIC - Support compliance and regulatory reporting
# MAGIC
# MAGIC The architecture is scalable and can process large volumes of transaction data using distributed computing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Achievements
# MAGIC  
# MAGIC * Processed ~940K transaction records
# MAGIC
# MAGIC * Implemented Medallion Architecture
# MAGIC
# MAGIC * Built Fraud Detection Logic
# MAGIC
# MAGIC * Applied Spark Window Functions
# MAGIC
# MAGIC * Implemented Delta Lake Storage
# MAGIC
# MAGIC * Performed Spark Performance Optimization
# MAGIC
# MAGIC * Developed SQL Analytics Layer
# MAGIC
# MAGIC * Generated Enterprise AML KPIs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Interview Highlights
# MAGIC
# MAGIC ### Why PySpark?
# MAGIC
# MAGIC PySpark provides distributed processing capabilities for handling large-scale datasets efficiently.
# MAGIC
# MAGIC ### Why Delta Lake?
# MAGIC
# MAGIC Delta Lake provides ACID transactions, schema enforcement, and optimized storage for analytical workloads.
# MAGIC
# MAGIC ### Why Medallion Architecture?
# MAGIC
# MAGIC Separates raw, cleansed, and business-ready datasets while improving maintainability and scalability.
# MAGIC
# MAGIC ### Why Window Functions?
# MAGIC
# MAGIC Window Functions enable advanced analytical calculations such as ranking, trend analysis, and fraud prioritization.
# MAGIC
# MAGIC ### Why This Project Matters?
# MAGIC
# MAGIC This project simulates a real-world AML monitoring system used by financial institutions to detect suspicious activities and prioritize investigations.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenges Faced During Development
# MAGIC
# MAGIC ### Challenge 1
# MAGIC Understanding anonymized AML datasets with PCA-transformed features.
# MAGIC
# MAGIC ### Solution
# MAGIC Performed schema inspection, data exploration, and feature analysis before designing KPIs.
# MAGIC
# MAGIC ### Challenge 2
# MAGIC Handling large datasets efficiently.
# MAGIC
# MAGIC ### Solution
# MAGIC Implemented repartitioning, caching, and Delta Lake storage.
# MAGIC
# MAGIC ### Challenge 3
# MAGIC Designing fraud analytics without business-friendly columns.
# MAGIC
# MAGIC ### Solution
# MAGIC Built risk-scoring logic and window-based fraud analytics using available features.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC Successfully designed and implemented an enterprise-scale AML Fraud Detection and Risk Analytics Pipeline using Databricks, PySpark, Delta Lake, SQL, and Spark Window Functions.
# MAGIC
# MAGIC The solution demonstrates modern Data Engineering practices including scalable data processing, medallion architecture, fraud analytics, performance optimization, and business-focused reporting.