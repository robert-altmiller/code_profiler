# Databricks notebook source
# DBTITLE 1,Import the Python Code Profiler
from code_profiler.profiler_tools import *

# COMMAND ----------

# DBTITLE 1,Import the test_functions and test_class Python Files
from code_profiler.initialize.unit_test.test_functions import *
from code_profiler.initialize.unit_test.test_class import *

# COMMAND ----------

# DBTITLE 1,Apply the Time Decorator to All Python Standalone Functions
# Example usage: Call these functions after all imports
globals, function_results = apply_timer_decorator_to_all_python_functions(globals()) # python standalone functions 

# COMMAND ----------

# DBTITLE 1,Apply the Time Decorator to All Python Class Functions
# Example usage: Call these functions after all imports
globals, python_class_results = apply_timer_decorator_to_all_python_class_functions(globals, python_class_and_fxns_scopes_unittesting) # python class functions

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

# DBTITLE 1,Write Code Profiling Results to Delta Table
create_code_profiling_results_delta_table(
    spark = spark,
    catalog = "hive_metastore", 
    schema = "default", 
    table_name = "code_profiler_unit_test_py_files_in_nb",
    overwrite_profiling_data = True
)