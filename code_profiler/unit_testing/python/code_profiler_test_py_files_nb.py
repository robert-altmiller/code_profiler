# Databricks notebook source
# DBTITLE 1,Delete Existing Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Import the Python Code Profiler
from code_profiler.main import *

# COMMAND ----------

# DBTITLE 1,Import the test_functions and test_class Python Files
from code_profiler.initialize.unit_test.test_functions import *
from code_profiler.initialize.unit_test.test_class import *

# COMMAND ----------

log_file_write_path = "/Workspace/Users/robert.altmiller@databricks.com/ayt-data-engineering-local-run/profiling/code_profiler_unit_test_py_files_in_nb"
print(log_file_write_path)

# COMMAND ----------

# DBTITLE 1,Apply the Time Decorator to All Python Standalone Functions
# Example usage: Call these functions after all imports
globals, function_results = apply_timer_decorator_to_all_python_functions(globals(), log_file_path = log_file_write_path) # python standalone functions 

# COMMAND ----------

# DBTITLE 1,Apply the Time Decorator to All Python Class Functions
# Example usage: Call these functions after all imports
globals, python_class_results = apply_timer_decorator_to_all_python_class_functions(globals, python_class_and_fxns_scopes_unittesting, log_file_path = log_file_write_path) # python class functions

# COMMAND ----------

# DBTITLE 1,Testing Class for Timer Decorator Call
# Class usage example:
inventory = Inventory()
inventory.add_item("001", {"name": "Laptop", "quantity": 10, "price": 1200.00})
inventory.update_item("001", {"name": "Laptop", "quantity": 12, "price": 1150.00})
print(inventory.search_item("001"))
inventory.remove_item("001")
print(inventory.list_items())

# COMMAND ----------

# DBTITLE 1,Testing Functions For Timer Decorator Call
# Functions usage example:
print("Checking if 29 is prime:", is_prime(29))
print("10°C to Fahrenheit:", celsius_to_fahrenheit(10))
print("Factorial of 5:", factorial(5))
print("First 5 Fibonacci numbers:", fibonacci(5))
print("Reversed string of 'hello':", reverse_string('hello'))
print("Sum of list [1, 2, 3, 4]:", sum_of_list([1, 2, 3, 4]))
print("Is 'radar' a palindrome?", is_palindrome('radar'))
print("Max in list [1, 99, 34, 56]:", max_in_list([1, 99, 34, 56]))

# COMMAND ----------

# DBTITLE 1,Create Code Profiling Logs and Write Code Profiling Results to Delta Table
write_all_code_profiling_logs_and_create_delta_table(
    spark = spark,
    global_thread_queue_dict = global_thread_queue_dict,
    mqueue_batch_size = mqueue_batch_size, 
    catalog = "hive_metastore",
    schema = "default",
    table_name = "code_profiler_unit_test_py_files_in_nb",
    overwrite_profiling_data = True,
    log_file_path = log_file_write_path
)