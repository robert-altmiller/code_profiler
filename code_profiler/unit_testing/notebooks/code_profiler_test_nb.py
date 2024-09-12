# Databricks notebook source
# DBTITLE 1,Import the test_functions notebook
# MAGIC %run ../../initialize/unit_test/test_functions_nb

# COMMAND ----------

# DBTITLE 1,Import the test_class notebook
# MAGIC %run ../../initialize/unit_test/test_class_nb


# COMMAND ----------

# DBTITLE 1,Check if running locally in Databricks and set the log_file_write_path
if is_running_in_databricks() == False: # running in local IDE
    from code_profiler.initialize.unit_test.test_functions_nb import *
    from code_profiler.initialize.unit_test.test_class_nb import *

# COMMAND ----------

# DBTITLE 1,Decorate All the In Scope Standalone Python Functions and Class Functions
# all custom and standard library imports need to execute first....
from code_profiler.main import *

if is_running_in_databricks() == True:
    # Clear the widgets
    dbutils.widgets.removeAll()

# update the log_file_write_path environment variable
log_file_write_path =  "./code_profiling/code_profiler_unit_test_nb"
# Check if the path exists
if os.path.exists(log_file_write_path):
    # Delete the directory and all its contents
    shutil.rmtree(log_file_write_path)
print(log_file_write_path)

# add the @timer decorator to all Python functions using automation and update globals() namespace dictionary.
original_globals = globals()
current_globals, function_results, nb_class_results, python_class_results = add_timer_to_all_functions(original_globals, log_file_write_path, class_scopes = python_class_and_fxns_scopes_unittesting)
print(f"standalone decorated functions: {function_results}")
print(f"notebook class decorated functions: {nb_class_results}")
print(f"python file class decorated functions: {python_class_results}")

# COMMAND ----------

# DBTITLE 1,Testing Class for Timer Decorator Call
# Class usage example:
print("Class usage example:")
inventory = Inventory()
inventory.add_item("001", {"name": "Laptop", "quantity": 10, "price": 1200.00})
inventory.update_item("001", {"name": "Laptop", "quantity": 12, "price": 1150.00})
print(inventory.search_item("001"))
inventory.remove_item("001")
print(inventory.list_items())

# COMMAND ----------

# DBTITLE 1,Testing Functions For Timer Decorator Call
# Functions usage example:
print("Functions usage example:")
print("Checking if 29 is prime:", is_prime(29))
print("10°C to Fahrenheit:", celsius_to_fahrenheit(10))
print("Factorial of 5:", factorial_calc(5))
print("First 5 Fibonacci numbers:", fibonacci(5))
print("Reversed string of 'hello':", reverse_string('hello'))
print("Sum of list [1, 2, 3, 4]:", sum_of_list([1, 2, 3, 4]))
print("Is 'radar' a palindrome?", is_palindrome('radar'))
print("Max in list [1, 99, 34, 56]:", max_in_list([1, 99, 34, 56]))
print("120 miles in kilometers:", miles_to_kilometers(120))
print("Count vowels in the word 'aggregation':", count_vowels("aggregation"))

# COMMAND ----------

# DBTITLE 1,Create Code Profiling Logs and Write Code Profiling Results to Delta Table
log_message_df = write_all_code_profiling_logs_and_create_delta_table(
    spark = spark,
    global_thread_queue_dict = global_thread_queue_dict,
    mqueue_batch_size = mqueue_batch_size, 
    catalog = "hive_metastore",
    schema = "default",
    table_name = "code_profiler_unit_test_nb",
    overwrite_profiling_data = True,
    log_file_path = log_file_write_path
)
print(f"log_message_df count: {log_message_df.count()}")
log_message_df_pandas = log_message_df.toPandas()
log_message_df_pandas.to_csv(f"{log_file_write_path}/log_message_df_pandas.csv", index=False, header = True)
print(log_message_df_pandas.head())