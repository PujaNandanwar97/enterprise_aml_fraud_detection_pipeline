# Databricks notebook source
# MAGIC %md
# MAGIC # Performance Optimization Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Optimization Layer
# MAGIC
# MAGIC ### Objective
# MAGIC Optimize Spark processing for large-scale AML transaction datasets using partitioning, caching, persistence, and Delta optimization techniques.
# MAGIC
# MAGIC ### Techniques
# MAGIC - Repartition
# MAGIC - Cache
# MAGIC - Persist
# MAGIC - Explain Plan
# MAGIC - Delta Optimization Concepts
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 - Load Sliver Dataset

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.storagelevel import StorageLevel

sliver_df = spark.table(
    "aml_sliver_transactions"
)
print("Records:", sliver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 - Current Partition Analysis

# COMMAND ----------

# Note: Direct partition inspection via RDD is not available on Serverless.
# Alternative: Use spark_partition_id() to understand data distribution
from pyspark.sql.functions import spark_partition_id, count

partition_counts = sliver_df.groupBy(spark_partition_id().alias("partition_id")).agg(count("*").alias("record_count"))
num_partitions = partition_counts.count()
print(f"Current Partitions: {num_partitions}")
display(partition_counts.orderBy("partition_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 - Repartition Dataset

# COMMAND ----------

optimized_df = sliver_df.repartition(8)

# Note: Direct partition inspection via RDD is not available on Serverless.
from pyspark.sql.functions import spark_partition_id, count

optimized_partition_counts = optimized_df.groupBy(spark_partition_id().alias("partition_id")).agg(count("*").alias("record_count"))
num_optimized_partitions = optimized_partition_counts.count()
print(f"New Partitions: {num_optimized_partitions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 - Cache Dataset (Frequently used Dataset)

# COMMAND ----------

# Note: .cache() and .persist() are not supported on Serverless compute.
# Serverless automatically manages caching and query optimization.
# For explicit control, consider using a standard cluster instead.

# Alternative: materialize to a temp view for reuse
optimized_df.createOrReplaceTempView("optimized_df_cached")
optimized_count = spark.table("optimized_df_cached").count()
print(f"Optimized DataFrame records: {optimized_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 - Persist Dataset

# COMMAND ----------

# Note: .persist() is not supported on Serverless compute.
# Serverless automatically manages storage and query optimization.
# Using the temp view created in the previous step for reuse.
persisted_count = spark.table("optimized_df_cached").count()
print(f"Persisted DataFrame records: {persisted_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 - Query Execution Plan

# COMMAND ----------

optimized_df.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 - Save Optimized Delta Table

# COMMAND ----------

optimized_df.write \
    .mode("overwrite") \
        .format("delta") \
            .saveAsTable("aml_optimized_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 8 - Verfiy Table

# COMMAND ----------

display(
    spark.table(
        "aml_optimized_transactions"
    ).limit(10)
)