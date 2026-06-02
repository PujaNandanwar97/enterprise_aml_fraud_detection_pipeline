Enterprise AML Fraud Detection & Risk Analytics Pipeline 
Overview

This project demonstrates an end-to-end Data Engineering pipeline for Anti-Money Laundering (AML) fraud analytics using Databricks, PySpark, Delta Lake, and SQL.

The solution processes large-scale banking transaction data, applies Medallion Architecture (Bronze → Silver → Gold), generates fraud intelligence, performs risk scoring, and delivers analytical insights using Spark SQL and Window Functions.

---

Business Problem

Financial institutions process millions of transactions daily. Detecting suspicious activities and identifying high-risk entities is critical for Anti-Money Laundering (AML) compliance.

This project builds a scalable fraud analytics pipeline capable of processing large transaction datasets and generating risk-based insights for fraud monitoring.

---

Dataset Information

Dataset: Time Series of Transactions – Money Laundering

Dataset Size: ~2.3 GB

Records Processed: ~940,000+

Data Sources:

- Transaction Records
- Fraud Labels
- Company Information
- Time Series IDs
- Event Order Data

Characteristics:

- Anonymized Banking Data
- AML Research Dataset
- Time-Series Based Transactions
- Fraud and Non-Fraud Labels

---

Architecture

Raw CSV Files
↓
Data Ingestion
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer KPIs
↓
Fraud Detection Logic
↓
Window Analytics
↓
SQL Analytics

---

Medallion Architecture

Bronze Layer

- Raw data ingestion
- Source data storage
- Schema validation

Silver Layer

- Data cleansing
- Null handling
- Duplicate validation
- Data quality checks

Gold Layer

- Fraud KPIs
- Business analytics
- Risk insights

---

Technology Stack

Data Processing

- PySpark

Storage

- Delta Lake

Platform

- Databricks

Query Engine

- Spark SQL

Analytics

- Window Functions
- Risk Ranking
- Fraud Monitoring

Performance Optimization

- Repartition
- Cache
- Persist

---

Key Features

Data Engineering

- End-to-End Data Pipeline
- Medallion Architecture
- Delta Lake Storage
- Data Quality Validation

Fraud Analytics

- Fraud Distribution Analysis
- Fraud Risk Scoring
- Risk Ranking
- High-Risk Company Identification

Advanced Analytics

- Row Number
- Rank
- Dense Rank
- Lag
- Lead

Optimization

- Spark Repartitioning
- Caching
- Persistence
- Execution Plan Analysis

---

Key KPIs

- Total Companies
- Fraud Companies
- Non-Fraud Companies
- Fraud Percentage
- Top Risk Windows
- Fraud Trend Analysis
- Risk Ranking

---

Performance Optimization Techniques

- Repartitioning large datasets
- Spark caching for repeated operations
- Persistence using MEMORY_AND_DISK
- Delta Lake storage optimization
- Spark execution plan analysis

---

Project Structure

01_data_ingestion

02_bronze_layer

03_silver_layer

04_gold_layer_kpis

05_fraud_detection_logic

06_window_analytics

07_optimization

08_sql_analytics

09_project_summary

---

Interview Highlights

Why PySpark?

PySpark enables distributed processing of large datasets and scales efficiently for enterprise workloads.

Why Delta Lake?

Delta Lake provides ACID transactions, schema enforcement, and reliable data storage.

Why Medallion Architecture?

It separates raw, cleansed, and business-ready datasets, improving scalability and maintainability.

Why Window Functions?

Window Functions support advanced analytical calculations such as ranking, trend analysis, and fraud prioritization.

---

Challenges Faced

Challenge 1

Understanding anonymized AML datasets with PCA-transformed features.

Solution

Performed schema exploration and feature analysis before designing KPIs.

Challenge 2

Processing large-scale transaction data efficiently.

Solution

Implemented Spark optimization techniques such as repartitioning, caching, and persistence.

Challenge 3

Designing fraud analytics with limited business-friendly attributes.

Solution

Built rule-based fraud scoring and risk-ranking logic using available transaction relationships.

---

Project Outcome

Successfully designed and implemented an enterprise-scale AML Fraud Detection & Risk Analytics Pipeline using Databricks, PySpark, Delta Lake, and SQL.

The project demonstrates scalable data processing, fraud analytics, Spark optimization, SQL analytics, and modern Data Engineering best practices.
