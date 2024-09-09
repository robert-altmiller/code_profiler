# Databricks notebook source
# DBTITLE 1,Remove All Widgets
# MAGIC dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Library Imports
# MAGIC import ast, glob, json, os, time, zlib
# MAGIC from datetime import datetime
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Local Parameters
# MAGIC catalog = "hive_metastore" # MODIFY
# MAGIC schema = "default" # MODIFY
# MAGIC table_name = "code_profiler_data" # MODIFY
# MAGIC unqiue_app_id = "5d856b7a_ab5d_4338_9401_0394dd1da677" # MODIFY

# COMMAND ----------

# DBTITLE 1,Analyze Delta Table Code Profiler Results
# read the code profiler data
# MAGIC code_profiler_df = spark.sql(f"SELECT * FROM {catalog}.{schema}.{table_name}")
# MAGIC display(code_profiler_df)

# COMMAND ----------

# DBTITLE 1, Decode the 'source_code_compressed' column for the top 5 slowest running functions
# Get the top 5 records with the highest execution time and decode the source_code_compressed column
# Define UDF to decode the compressed source code
# MAGIC def decode_source_code(source_code_compressed):
# MAGIC    import base64
# MAGIC    # Decode from base64 to compressed bytes
# MAGIC    source_code_decompressed = base64.b64decode(source_code_compressed)
# MAGIC    # Decompress using zlib
# MAGIC    source_code_decompressed = zlib.decompress(source_code_decompressed).decode('utf-8')
# MAGIC    return source_code_decompressed

# Register the UDF
# MAGIC decode_source_code_udf = udf(decode_source_code, StringType())

# MAGIC total_slow_running_fxns = 5
# MAGIC top_5_slowest_fxns_df = code_profiler_df \
# MAGIC    .orderBy(col("execution_time").desc()).limit(total_slow_running_fxns) \
# MAGIC    .select("class_name", "function_name", "source_code_compressed")
# MAGIC top_5_slowest_fxns_df = top_5_slowest_fxns_df \
# MAGIC    .withColumn("source_code_decompressed", decode_source_code_udf(top_5_slowest_fxns_df["source_code_compressed"]))
# MAGIC display(top_5_slowest_fxns_df)

# COMMAND ----------

# DBTITLE 1, Get LLM function code optimizations and an example of optimized code for the top 5 slowest running functions
# Large language model (LLM) connection and instruct parameters
# Large language model (LLM) connection and instruct parameters
# MAGIC my_api_key = "" # insert your api key here
# Update the base URL to your own Databricks Serving Endpoint
# MAGIC workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"
# MAGIC llm_model_name = "databricks-dbrx-instruct"
# MAGIC endpoint_url = f"{workspace_url}/serving-endpoints"

# Call to the LLM model UDFs for optimization recommendations and optimized code
# MAGICtop_5_slowest_fxns_df_optimized = top_5_slowest_fxns_df \
# MAGIC  .withColumn("llm_opt_suggestions", spark_get_llm_code_recs_response(lit(my_api_key), lit(endpoint_url), lit(code_recs_prompt), top_5_slowest_fxns_df.source_code_decompressed, lit(llm_model_name))) \
# MAGIC  .withColumn("llm_opt_code", spark_get_llm_opt_code_response(lit(my_api_key), lit(endpoint_url), lit(code_opt_prompt), top_5_slowest_fxns_df.source_code_decompressed, lit(llm_model_name)))
# MAGIC display(MAGICtop_5_slowest_fxns_df_optimized)

# COMMAND ----------

# DBTITLE 1,Sed Widgets to be Used With Databricks SQL
# MAGIC dbutils.widgets.text("catalog_name", catalog, "Catalog Name")
# MAGIC dbutils.widgets.text("schema_name", schema, "Schema Name")
# MAGIC dbutils.widgets.text("table_name", table_name, "Table Name")
# MAGIC dbutils.widgets.text("tenant_id", tenant_id, "Tenant ID")

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

# MAGIC SELECT tenant_id, thread_id, COUNT(DISTINCT(function_name)) as total_function_count,
# MAGIC     ROUND((SUM(execution_time)/60)/60, 4) as total_execution_time_hours_allthreads
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, thread_id
# MAGIC ORDER BY total_execution_time_hours_allthreads DESC

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