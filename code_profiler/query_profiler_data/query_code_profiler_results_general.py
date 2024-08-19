# Databricks notebook source


# DBTITLE 1,Remove All Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Library Imports
import glob, os, json, time, ast
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Local Parameters
catalog = "hive_metastore" # MODIFY
schema = "default" # MODIFY
table_name = "code_profiler_data" # MODIFY
unqiue_app_id = "xxxxxxxxxxxxx" # MODIFY

# COMMAND ----------

# DBTITLE 1,Analyze Delta Table Code Profiler Results
# read the code profiler data
code_profiler_df = spark.sql(f"SELECT * FROM {catalog}.{schema}.{table_name}")
display(code_profiler_df)

# COMMAND ----------

# DBTITLE 1,Sed Widgets to be Used With Databricks SQL
dbutils.widgets.text("catalog_name", catalog, "Catalog Name")
dbutils.widgets.text("schema_name", schema, "Schema Name")
dbutils.widgets.text("table_name", table_name, "Table Name")
dbutils.widgets.text("unqiue_app_id", unqiue_app_id, "Unique App ID")

# COMMAND ----------

# DBTITLE 1,Declare Databricks SQL Variables
# MAGIC %sql
# MAGIC
# MAGIC DECLARE OR REPLACE catalog_name = getArgument('catalog_name');
# MAGIC DECLARE OR REPLACE schema_name = getArgument('schema_name');
# MAGIC DECLARE OR REPLACE table_name = getArgument('table_name');
# MAGIC DECLARE OR REPLACE tenant_id =  getArgument('tenant_id');
# MAGIC SELECT tenant_id, catalog_name, schema_name, table_name

# COMMAND ----------

# DBTITLE 1,How many total unique threads by unique_app_id
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, count(DISTINCT(thread_id)) as total_threads 
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC GROUP BY tenant_id
# MAGIC ORDER BY tenant_id ASC;

# COMMAND ----------

# DBTITLE 1,Get a Unqiue List of Python Functions Profiled with Code Profiler and How Many Times They Are Called Across All Threads
# MAGIC %sql
# MAGIC
# MAGIC SELECT function_name, COUNT(function_name) as function_count
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC GROUP BY function_name
# MAGIC ORDER BY function_count DESC

# COMMAND ----------

# DBTITLE 1,Function Count and Function Total Execution Time Grouped by Function, Tenant and Thread ID
# MAGIC %sql
# MAGIC
# MAGIC SELECT tenant_id, thread_id, function_name, COUNT(function_name) as function_count,
# MAGIC         ROUND(SUM(execution_time), 2) AS total_execution_time_seconds
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE tenant_id = 'xxxxxxxxxxxxx'
# MAGIC GROUP BY tenant_id, thread_id, function_name
# MAGIC ORDER BY function_name ASC, total_execution_time_seconds DESC;

# COMMAND ----------

# DBTITLE 1,Function Count and Average Execution Time For All Threads Grouped by Function and unique_app_id
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, function_name, COUNT(function_name) as function_count, COUNT(DISTINCT(thread_id)) as total_threads,
# MAGIC         ROUND((SUM(execution_time)/60)/total_threads,2) AS avg_execution_time_mins_allthreads
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name) a
# MAGIC GROUP BY unique_app_id, function_name
# MAGIC ORDER BY avg_execution_time_mins_allthreads DESC 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC ORDER BY cpu_usage_percent DESC

# COMMAND ----------

# DBTITLE 1,What is the Total Execution Time by unique_app_id
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, COUNT(DISTINCT(thread_id)) as unique_thread_count, 
# MAGIC        ROUND(((SUM(execution_time) / unique_thread_count)/60)/60, 4) AS avg_execution_time_hours_allthreads,
# MAGIC        MIN(cpu_usage_percent) AS max_cpu_usage_percent, 
# MAGIC        ROUND(MAX(memory_usage_bytes/1048576),4) AS max_memory_usage_mb
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC GROUP BY unique_app_id
# MAGIC ORDER BY avg_execution_time_hours_allthreads DESC

# COMMAND ----------

# DBTITLE 1,Individual Total Function Time by What is the Total Execution Time by unique_app_id and Function
# MAGIC %sql
# MAGIC
# MAGIC -- Do this at the thread level too
# MAGIC SELECT unique_app_id, function_name, 
# MAGIC         ROUND(SUM(execution_time)/60, 4) AS total_execution_time_mins,
# MAGIC         COUNT(function_name) AS total_function_calls,
# MAGIC         ROUND(total_execution_time_mins / total_function_calls, 4) AS avg_execution_time_per_function_call
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE unique_app_id = 'xxxxxxxxxxxxx'
# MAGIC GROUP BY unique_app_id, function_name
# MAGIC ORDER BY total_execution_time_mins DESC

# COMMAND ----------

# DBTITLE 1,What is the Total Execution Time by unique_app_id?
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, thread_id, ROUND((SUM(execution_time)/60)/60, 4) as total_time_hours
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE unique_app_id = 'xxxxxxxxxxxxx'
# MAGIC GROUP BY unique_app_id, thread_id
# MAGIC ORDER BY total_time_hours DESC

# COMMAND ----------

# DBTITLE 1,What is the Total Execution Time by unique_app_id and Thread?
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, thread_id, function_name,
# MAGIC        ROUND((SUM(execution_time)/60)/60, 4) AS execution_time_hours,
# MAGIC        ROUND(AVG(cpu_usage_percent),4) AS cpu_usage_percent, 
# MAGIC        ROUND(AVG(memory_usage_bytes)/1048576,4) AS memory_usage_mb
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE unique_app_id = 'xxxxxxxxxxxxx' and thread_id = '140534042109504'
# MAGIC GROUP BY unique_app_id, thread_id, function_name
# MAGIC ORDER BY execution_time_hours DESC