# Databricks notebook source
# DBTITLE 1,Import the Python Code Profiler
from code_profiler.main import *

# COMMAND ----------

# DBTITLE 1,Import the test_functions notebook
# MAGIC %run ../../initialize/unit_test/test_functions_nb

# COMMAND ----------

# DBTITLE 1,Import the test_class notebook
# MAGIC %run ../../initialize/unit_test/test_class_nb

# COMMAND ----------

# DBTITLE 1,Check if running locally in Databricks and set the log_file_write_path
if is_running_in_databricks() == True:
    # Clear the widgets
    dbutils.widgets.removeAll()
else: 
    from code_profiler.initialize.unit_test.test_functions_nb import *
    from code_profiler.initialize.unit_test.test_class_nb import *
# Change the log_file_write_path
log_file_write_path = "./code_profiling/code_profiler_unit_test_nb"

# Check if the path exists
if os.path.exists(log_file_write_path):
    # Delete the directory and all its contents
    shutil.rmtree(log_file_write_path)
print(log_file_write_path)

# COMMAND ----------

# DBTITLE 1,Apply Timer() Decorator to All Standalone Python Functions
# Example usage: Call these functions after all imports
original_globals = globals()
current_globals, function_results = apply_timer_decorator_to_all_python_functions(original_globals, log_file_path = log_file_write_path) # python standalone functions 
print(f"\ndecorated_standalone_functions: {function_results}\n")

# COMMAND ----------

# DBTITLE 1,Add Timer Decorator to All Notebook Class Functions
# Example usage: Call these functions after all imports
current_globals, nb_class_results = apply_timer_decorator_to_all_nb_class_functions(current_globals, python_class_and_fxns_scopes_unittesting, log_file_path = log_file_write_path) # notebook class functions
print(f"\ndecorated notebook class functions: {nb_class_results}\n")

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
log_message_df.show()