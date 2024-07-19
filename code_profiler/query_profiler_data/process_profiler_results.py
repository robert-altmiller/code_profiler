# Databricks notebook source
# DBTITLE 1,Library Imports
import glob, os, json, time, ast
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Local Parameters
catalog = "hive_metastore" # update / change
schema = "default" # update / change
table_name = "code_profiler_local_run_compoundemp" #update / change

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
# MAGIC SELECT tenant_id

# COMMAND ----------

# DBTITLE 1,Get a Unqiue List of Python Functions Profiled with Code Profiler
# MAGIC %sql
# MAGIC
# MAGIC SELECT function_name, COUNT(function_name) as count 
# MAGIC from FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC GROUP BY function_name
# MAGIC ORDER BY function_name

# COMMAND ----------

# DBTITLE 1,Tenant, Function, and Thread Total Execution Time Summary (Linear)
# MAGIC %sql
# MAGIC
# MAGIC SELECT a.tenant_id, a.function_name, a.thread_id, b.table_count, 
# MAGIC         ROUND(SUM(execution_time)/60, 2) AS total_execution_time_mins_allthreads, 
# MAGIC         total_execution_time_mins_allthreads/b.table_count AS avg_execution_time_mins_per_table_allthreads
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name) a
# MAGIC JOIN (
# MAGIC     SELECT tenant_id, thread_id, count(function_name) as table_count
# MAGIC     FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC     WHERE function_name = 'write_delta()' AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC     GROUP BY tenant_id, thread_id
# MAGIC ) b
# MAGIC ON a.tenant_id = b.tenant_id AND a.thread_id = b.thread_id
# MAGIC GROUP BY a.tenant_id, a.thread_id, a.function_name, b.table_count
# MAGIC ORDER BY function_name ASC, total_execution_time_mins_allthreads DESC ;

# COMMAND ----------

# DBTITLE 1,Unique Function Total Execution Time For All Threads
# MAGIC %sql
# MAGIC SELECT c.tenant_id, c.function_name, SUM(c.table_count) as table_count, 
# MAGIC        ROUND(SUM(c.total_execution_time_mins)/COUNT(DISTINCT thread_id),2) AS avg_execution_time_mins_allthreads, 
# MAGIC        ROUND(SUM(c.avg_execution_time_mins_per_table)/COUNT(DISTINCT thread_id),2) AS avg_execution_time_mins_per_table_allthreads
# MAGIC FROM
# MAGIC ( 
# MAGIC     SELECT a.tenant_id, a.function_name, a.thread_id, 
# MAGIC            ROUND(SUM(execution_time)/60, 2) AS total_execution_time_mins, b.table_count, 
# MAGIC            total_execution_time_mins/b.table_count AS avg_execution_time_mins_per_table
# MAGIC     FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name) a
# MAGIC     JOIN (
# MAGIC         SELECT tenant_id, thread_id, count(function_name) as table_count
# MAGIC         FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC         WHERE function_name = 'write_delta()' AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC         GROUP BY tenant_id, thread_id
# MAGIC     ) b
# MAGIC     ON a.tenant_id = b.tenant_id AND a.thread_id = b.thread_id
# MAGIC     GROUP BY a.tenant_id, a.thread_id, a.function_name, b.table_count
# MAGIC     ORDER BY function_name ASC, total_execution_time_mins DESC 
# MAGIC ) as c
# MAGIC GROUP BY c.tenant_id, c.function_name
# MAGIC ORDER BY avg_execution_time_mins_allthreads DESC

# COMMAND ----------

# DBTITLE 1,How Many Tables Processed Per Thread?
# MAGIC %sql
# MAGIC SELECT tenant_id, thread_id, function_name,
# MAGIC        COUNT(DISTINCT arguments[2]) as total_unique_tables,
# MAGIC        COUNT(arguments[2]) as total_tables_with_dups,
# MAGIC        total_tables_with_dups - total_unique_tables AS total_table_dups,
# MAGIC       --Robert Altmiller Needs to check this logic below if we divide by 'total_unique_tables' here
# MAGIC        ROUND(SUM(execution_time)/total_unique_tables,2) as total_execution_time_mins_unique_tables
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE function_name = 'write_delta()' AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, thread_id, function_name
# MAGIC ORDER BY thread_id

# COMMAND ----------

# DBTITLE 1,Which Tables Were Duplicated Loads in the Query Above?
# MAGIC %sql
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

# DBTITLE 1,How Long Did Each Table Take to Load by Thread?
# MAGIC %sql
# MAGIC SELECT tenant_id, thread_id, function_name, 
# MAGIC        split(split(arguments[2],"v0/")[1],"/")[0] as table_name,
# MAGIC        ROUND(AVG(execution_time),4) AS execution_time_seconds,
# MAGIC        ROUND(AVG(cpu_usage_percent),4) AS cpu_usage_percent, 
# MAGIC        ROUND(AVG(memory_usage_bytes)/1048576,4) AS memory_usage_mb,
# MAGIC        arguments[2] as table_path
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE function_name = 'write_delta()' AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, thread_id, function_name, table_name, table_path
# MAGIC ORDER BY execution_time_seconds DESC
# MAGIC -- ORDER BY table_name ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE arguments[2] = "hdlfs://23c1d1f4-7f89-4dfb-a76c-a251e774c0f8.files.hdl.canary-eu21.hanacloud.ondemand.com/silver/datasource/table/delta/workforceperson/external/v0/wf_emp_personalinfocol/_table_"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(arguments[2]) as table_path
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE function_name = 'write_delta()' AND tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC -- ORDER BY table_name ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tenant_id, count(DISTINCT(thread_id)) as total_threads 
# MAGIC FROM hive_metastore.default.code_profiler_data_workflow
# MAGIC GROUP BY tenant_id;

# COMMAND ----------

# DBTITLE 1,What is the Total Execution Time by Tenant
# MAGIC %sql
# MAGIC SELECT tenant_id, 
# MAGIC        ((ROUND(SUM(execution_time),4) / COUNT(DISTINCT(thread_id)))/60)/60 AS execution_time_hours,
# MAGIC        ROUND(AVG(cpu_usage_percent),4) AS cpu_usage_percent, 
# MAGIC        ROUND(AVG(memory_usage_bytes)/1048576,4) AS memory_usage_mb
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id
# MAGIC -- ORDER BY table_name ASC

# COMMAND ----------

# DBTITLE 1,Individual Total Function Time by Tenant and Function
# MAGIC %sql
# MAGIC
# MAGIC -- Do this at the thread level too
# MAGIC SELECT tenant_id, function_name, 
# MAGIC         ROUND(SUM(execution_time)/60, 4) AS total_execution_time_mins,
# MAGIC         COUNT(function_name) AS total_function_calls
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, function_name
# MAGIC ORDER BY total_execution_time_mins DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tenant_id, thread_id, SUM(execution_time) as total_time
# MAGIC FROM hive_metastore.default.code_profiler_data_workflow
# MAGIC WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677'
# MAGIC GROUP BY tenant_id, thread_id
# MAGIC ORDER BY total_time DESC

# COMMAND ----------

# DBTITLE 1,What is the Total Execution Time by Thread
# MAGIC %sql
# MAGIC SELECT tenant_id, thread_id, function_name,
# MAGIC        (ROUND(SUM(execution_time),4)/60)/60 AS execution_time_hours,
# MAGIC        ROUND(AVG(cpu_usage_percent),4) AS cpu_usage_percent, 
# MAGIC        ROUND(AVG(memory_usage_bytes)/1048576,4) AS memory_usage_mb
# MAGIC FROM IDENTIFIER(catalog_name || '.' || schema_name || '.' || table_name)
# MAGIC WHERE tenant_id = '5d856b7a_ab5d_4338_9401_0394dd1da677' and thread_id = '140070325519936'--'140069922911808'--'140070333912640'
# MAGIC GROUP BY tenant_id, thread_id, function_name
# MAGIC -- ORDER BY table_name ASC