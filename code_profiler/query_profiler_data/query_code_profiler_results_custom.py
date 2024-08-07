# Databricks notebook source
# DBTITLE 1,Remove Databricks Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Library Imports
import glob, os, json, time, ast
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Local Parameters
catalog = "hive_metastore" # update / change
schema = "default" # update / change
table_name = "code_profiler_ecdomain_local_run_largest_tenant_original"#code_profiler_local_run_largest_tenant" # update / change
tenant_id = "5d856b7a_ab5d_4338_9401_0394dd1da677" # update / change

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
dbutils.widgets.text("tenant_id", tenant_id, "Tenant ID")

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

# DBTITLE 1,What is the Total Threads, Average Runtime, Max CPU %, and Max Memory by Tenant
# MAGIC %sql
# MAGIC
# MAGIC SELECT sq.tenant_id, COUNT(DISTINCT(sq.thread_id)) as unique_thread_count,
# MAGIC        COUNT(DISTINCT(sq.function_name)) as unique_function_count,
# MAGIC        SUM(sq.total_function_calls) as total_function_calls,
# MAGIC        ROUND(MAX(sq.total_execution_time_hours)*60, 4) AS total_execution_time_mins,
# MAGIC        ROUND(MAX(sq.total_execution_time_hours), 4) AS total_execution_time_hours,
# MAGIC        ROUND(MAX(sq.max_cpu_usage_percent) / unique_thread_count, 4) AS max_cpu_usage_percent, 
# MAGIC        ROUND(MAX(sq.max_memory_usage_mb)/unique_thread_count, 4) AS max_memory_usage_mb
# MAGIC FROM (
# MAGIC   SELECT tenant_id, thread_id, function_name,
# MAGIC         COUNT(function_name) AS total_function_calls,
# MAGIC         ROUND(((SUM(execution_time))/60)/60, 4) AS total_execution_time_hours,
# MAGIC         MAX(cpu_usage_percent) AS max_cpu_usage_percent, 
# MAGIC         MAX(memory_usage_bytes/1048576) AS max_memory_usage_mb
# MAGIC   FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC   GROUP BY tenant_id, thread_id, function_name
# MAGIC ) sq
# MAGIC GROUP BY tenant_id
# MAGIC ORDER BY total_execution_time_hours DESC
# MAGIC
# MAGIC -- # %sql
# MAGIC -- # SELECT function_name, MAX(cpu_usage_percent) as max_percent
# MAGIC -- # FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC -- # GROUP BY function_name
# MAGIC -- # ORDER BY max_percent DESC

# COMMAND ----------

# %sql

# SELECT tenant_id, thread_id, COUNT(DISTINCT(function_name)) as total_function_count,
#       ROUND((SUM(execution_time)/60)/60, 4) as total_execution_time_hours_allthreads
# FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# GROUP BY tenant_id, thread_id
# ORDER BY total_execution_time_hours_allthreads DESC

# COMMAND ----------

# DBTITLE 1,Get a Unqiue List of Python Functions Profiled and How Many Times They Are Called Across All Threads
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
# MAGIC WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, thread_id, function_name
# MAGIC ORDER BY function_name ASC, total_execution_time_seconds DESC;

# COMMAND ----------

# DBTITLE 1,Function Count and Average Execution Time For All Threads Grouped by Function and Tenant
# MAGIC %sql
# MAGIC
# MAGIC SELECT tenant_id, function_name, COUNT(function_name) as function_count, COUNT(DISTINCT(thread_id)) as total_threads,
# MAGIC         ROUND((SUM(execution_time)/60)/total_threads,2) AS avg_execution_time_mins_allthreads
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name) a
# MAGIC GROUP BY tenant_id, function_name
# MAGIC ORDER BY avg_execution_time_mins_allthreads DESC

# COMMAND ----------

# DBTITLE 1,How Many Unique HDLFS Write Delta Table Paths Exist?
# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT(arguments[2])) as total_unique_hdlfs_table_paths
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE function_name = 'write_delta()' AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'

# COMMAND ----------

# DBTITLE 1,How Many Tables Processed Per Thread, and Identify Any Duplicates
# MAGIC %sql
# MAGIC
# MAGIC SELECT tenant_id, thread_id,
# MAGIC        COUNT(DISTINCT arguments[2]) as total_unique_tables,
# MAGIC        COUNT(arguments[2]) as total_tables_with_dups,
# MAGIC        total_tables_with_dups - total_unique_tables AS total_table_dups
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE function_name = 'write_delta()' AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, thread_id
# MAGIC ORDER BY tenant_id ASC, total_unique_tables DESC

# COMMAND ----------

# DBTITLE 1,Which Tables Were Duplicated Loads in the Query Above?
# MAGIC %sql
# MAGIC
# MAGIC SELECT tenant_id, thread_id, table_name, COUNT(table_name) as dup_table_count
# MAGIC FROM
# MAGIC (
# MAGIC     SELECT tenant_id, thread_id, function_name, 
# MAGIC           split(split(arguments[2],"v0/")[1],"/")[0] as table_name,
# MAGIC           arguments[2] as table_path
# MAGIC     FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC     WHERE function_name = 'write_delta()' AND tenant_id = IDENTIFIER('tenant_id')
# MAGIC ) AS subquery
# MAGIC WHERE table_name IS NOT NULL AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, thread_id, table_name
# MAGIC HAVING COUNT(table_name) > 1
# MAGIC ORDER BY thread_id ASC, table_name ASC

# COMMAND ----------

# DBTITLE 1,How Long Did Each Table Take to Load and What Thread Loaded the Table?
# MAGIC %sql
# MAGIC
# MAGIC SELECT tenant_id, thread_id, function_name,
# MAGIC        split(split(arguments[2],"v0/")[1],"/")[0] as table_name,
# MAGIC        ROUND(SUM(execution_time), 4) AS execution_time_seconds,
# MAGIC        ROUND(SUM(cpu_usage_percent), 4) AS cpu_usage_percent, 
# MAGIC        ROUND(SUM(memory_usage_bytes) / 1048576, 4) AS memory_usage_mb,
# MAGIC        arguments[2] as table_path
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE function_name = 'write_delta()' AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, thread_id, function_name, table_name, table_path
# MAGIC ORDER BY execution_time_seconds DESC

# COMMAND ----------

# DBTITLE 1,Individual Total Function Time by Tenant and Function
# MAGIC %sql
# MAGIC
# MAGIC -- Do this at the thread level too
# MAGIC SELECT tenant_id, function_name, 
# MAGIC         ROUND(SUM(execution_time)/60, 4) AS total_execution_time_mins,
# MAGIC         COUNT(function_name) AS total_function_calls,
# MAGIC         ROUND(total_execution_time_mins / total_function_calls, 4) AS avg_execution_time_per_function_call
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, function_name
# MAGIC ORDER BY total_execution_time_mins DESC

# COMMAND ----------

# DBTITLE 1,What is the Total Execution Time by Tenant and Thread?


# COMMAND ----------

# DBTITLE 1,What is the Total Execution Time by Thread
# MAGIC %sql
# MAGIC
# MAGIC SELECT tenant_id, thread_id, function_name,
# MAGIC        ROUND((SUM(execution_time)/60), 4) AS total_execution_time_mins,
# MAGIC        ROUND((SUM(execution_time)/60)/60, 4) AS total_execution_time_hours,
# MAGIC        ROUND(AVG(cpu_usage_percent),4) AS cpu_usage_percent, 
# MAGIC        ROUND(AVG(memory_usage_bytes)/1048576,4) AS memory_usage_mb
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677' and thread_id = '140634034013760'
# MAGIC GROUP BY tenant_id, thread_id, function_name
# MAGIC ORDER BY total_execution_time_hours DESC

# COMMAND ----------

