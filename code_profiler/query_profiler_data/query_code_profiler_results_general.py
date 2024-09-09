# Databricks notebook source
# DBTITLE 1,Remove All Widgets
# MAGIC dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Library Imports
# MAGIC import ast, glob, json, os, time, zlib
# MAGIC from datetime import datetime
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC from code_profiler.llm_integration.llm_optimization import *

# COMMAND ----------

# DBTITLE 1,Local Parameters
# MAGIC catalog = "hive_metastore" # MODIFY
# MAGIC schema = "default" # MODIFY
# MAGIC table_name = "code_profiler_data" # MODIFY
# MAGIC unqiue_app_id = "xxxxxxxxxxxxx" # MODIFY

# COMMAND ----------

# DBTITLE 1,Analyze Delta Table Code Profiler Results
# read the code profiler data
# MAGIC code_profiler_df = spark.sql(f"SELECT * FROM {catalog}.{schema}.{table_name}")
# MAGIC display(code_profiler_df)

# COMMAND ----------

# DBTITLE 1, Decode the 'source_code_compressed' column for the top 5 slowest runnign functions
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

# DBTITLE 1, Decode the 'source_code_compressed' column for the top 5 slowest runnign functions
# Large language model (LLM) connection and instruct parameters
# MAGIC my_api_key = ""
# Update the base URL to your own Databricks Serving Endpoint
# MAGIC workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"
# MAGIC llm_model_name = "databricks-dbrx-instruct"
# MAGIC endpoint_url = f"{workspace_url}/serving-endpoints"

# Call to the LLM model UDFs for optimization recommendations and optimized code
# MAGIC top_5_slowest_fxns_df = top_5_slowest_fxns_df.withColumn("llm_opt_suggestions", spark_get_llm_code_recs_response(lit(my_api_key), lit(endpoint_url), lit(code_recs_prompt), df.source_code, lit(llm_model_name)))
# MAGIC top_5_slowest_fxns_df = top_5_slowest_fxns_df.withColumn("llm_opt_code", spark_get_llm_opt_code_response(lit(my_api_key), lit(endpoint_url), lit(code_opt_prompt), df.source_code, lit(llm_model_name)))
# MAGIC top_5_slowest_fxns_df.select("llm_opt_suggestions").show(1, truncate = False)
# MAGIC top_5_slowest_fxns_df.select("llm_opt_code").show(1, truncate = False)

# COMMAND ----------

# DBTITLE 1,Sed Widgets to be Used With Databricks SQL
# MAGIC dbutils.widgets.text("catalog_name", catalog, "Catalog Name")
# MAGIC dbutils.widgets.text("schema_name", schema, "Schema Name")
# MAGIC dbutils.widgets.text("table_name", table_name, "Table Name")
# MAGIC dbutils.widgets.text("unqiue_app_id", unqiue_app_id, "Unique App ID")

# COMMAND ----------

# DBTITLE 1,Declare Databricks SQL Variables
# MAGIC %sql
# MAGIC
# MAGIC DECLARE OR REPLACE catalog_name = getArgument('catalog_name');
# MAGIC DECLARE OR REPLACE schema_name = getArgument('schema_name');
# MAGIC DECLARE OR REPLACE table_name = getArgument('table_name');
# MAGIC DECLARE OR REPLACE unqiue_app_id =  getArgument('unqiue_app_id');
# MAGIC SELECT unqiue_app_id, catalog_name, schema_name, table_name



# COMMAND ----------

# DBTITLE 1,How Many Total Unique Threads Exist Grouped By unique_app_id?
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, count(DISTINCT(thread_id)) as total_threads 
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC GROUP BY unique_app_id
# MAGIC ORDER BY unique_app_id ASC;

# COMMAND ----------

# DBTITLE 1,Get a Unique List of Python Functions Profiled with Code Profiler and How Many Times They Are Called Across All Threads.
# MAGIC %sql
# MAGIC
# MAGIC SELECT function_name, COUNT(function_name) as function_count
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC GROUP BY function_name
# MAGIC ORDER BY function_count DESC

# COMMAND ----------

# DBTITLE 1,Function Count and Function Total Execution Time Grouped by unique_app_id, Function, and Thread.
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, thread_id, function_name, COUNT(function_name) as function_count,
# MAGIC         ROUND(SUM(execution_time), 2) AS total_execution_time_seconds
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE unique_app_id = 'xxxxxxxxxxxxx'
# MAGIC GROUP BY unique_app_id, thread_id, function_name
# MAGIC ORDER BY function_name ASC, total_execution_time_seconds DESC;

# COMMAND ----------

# DBTITLE 1,Function Count and Average Execution Time For All Threads Grouped by Function and unique_app_id.
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, function_name, COUNT(function_name) as function_count, COUNT(DISTINCT(thread_id)) as total_threads,
# MAGIC         ROUND((SUM(execution_time)/60)/total_threads,2) AS avg_execution_time_mins_allthreads
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name) a
# MAGIC GROUP BY unique_app_id, function_name
# MAGIC ORDER BY avg_execution_time_mins_allthreads DESC 

# COMMAND ----------

# DBTITLE 1,Which Python Functions Had the Highest CPU Consumption Percentage.
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, function_name, 
# MAGIC    ROUND(AVG(cpu_usage_percent), 2) AS cpu_usage_percent,
#MAGIC     ROUND(AVG(memory_usage_bytes/1048576), 2) AS memory_usage_mb,
# MAGIC    COUNT(function_name) as total_function_calls
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE unique_app_id = 'xxxxxxxxxxxxx'
# MAGIC GROUP BY unique_app_id, function_name
# MAGIC ORDER BY cpu_usage_percent DESC

# COMMAND ----------

# DBTITLE 1,What is the Total Execution Time by unique_app_id for All Threads?
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, COUNT(DISTINCT(thread_id)) as unique_thread_count,
#MAGIC         ROUND(((SUM(execution_time) / unique_thread_count)/60), 4) AS avg_execution_time_mins_allthreads,
# MAGIC        ROUND(((SUM(execution_time) / unique_thread_count)/60)/60, 4) AS avg_execution_time_hours_allthreads,
# MAGIC        ROUND(MAX(cpu_usage_percent), 4) AS max_cpu_usage_percent,
# MAGIC        ROUND(MAX(memory_usage_bytes/1048576), 4) AS max_memory_usage_mb
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC GROUP BY unique_app_id
# MAGIC ORDER BY avg_execution_time_hours_allthreads DESC

# COMMAND ----------

# DBTITLE 1,Individual Total Function Time, and Average Function Time Per Function Call Grouped by unique_app_id and Function.
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

# DBTITLE 1,What is the Total Execution Time by unique_app_id and All Threads?
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, thread_id, 
# MAGIC         ROUND((SUM(execution_time)/60), 4) as total_time_mins,
# MAGIC         ROUND((SUM(execution_time)/60)/60, 4) as total_time_hours
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE unique_app_id = 'xxxxxxxxxxxxx'
# MAGIC GROUP BY unique_app_id, thread_id
# MAGIC ORDER BY total_time_hours DESC

# COMMAND ----------

# DBTITLE 1,What is the Function Total Execution Time by unique_app_id and a Single Thread?
# MAGIC %sql
# MAGIC
# MAGIC SELECT unique_app_id, thread_id, function_name,
# MAGIC        ROUND((SUM(execution_time)/60)/60, 4) AS execution_time_hours,
# MAGIC        ROUND(AVG(cpu_usage_percent), 4) AS cpu_usage_percent, 
# MAGIC        ROUND(AVG(memory_usage_bytes)/1048576, 4) AS memory_usage_mb
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE unique_app_id = 'xxxxxxxxxxxxx' and thread_id = '134291254474304'
# MAGIC GROUP BY unique_app_id, thread_id, function_name
# MAGIC ORDER BY execution_time_hours DESC