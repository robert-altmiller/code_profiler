# library imports
import glob, importlib, inspect, json, os, psutil, queue, re, sys, threading, time, types
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from functools import wraps

# get the tenant_id or some other python application identifier
from code_profiler.initialize.custom_identifers import unique_app_id
print(f"unique_app_id: {unique_app_id}")

# create a new local Spark session
spark_local = SparkSession.builder \
    .appName("CodeProfiling") \
    .getOrCreate()

# local parameters
thread_local = threading.local()
pid = os.getpid()
process = psutil.Process(pid)
original_recursion_limit = sys.getrecursionlimit()
global_thread_queue_dict = {}

# define the message queue batch size when writing code profiling data logs
mqueue_batch_size = 500
print(f"message queue batch size: {mqueue_batch_size}")

# explicity print the recursion limit
print_recursion_limit = False
print(f"\nprint_recursion_limit: {print_recursion_limit}\n")

# functions to ignore
functions_to_ignore = [
    # --- DO NOT MODIFY THESE FUNCTIONS TO IGNORE ---
    # notebook class function helpers
    "get_classes_from_globals", "apply_timer_decorator_to_nb_class_function", "apply_timer_decorator_to_all_nb_class_functions",
    # Python file class function helpers
    "check_items_in_string", "get_imported_classes", "dynamic_import_and_set_global",
    "apply_timer_decorator_to_python_class_function", "apply_timer_decorator_to_all_python_class_functions",
    # all standalone Python functions helpers
    "is_library_defined_function", "apply_timer_decorator_to_all_python_functions",
    # create delta table function helpers
    "get_profiling_result_paths", "get_all_profiling_results_joined", "write_profiling_results_to_delta_table", 
    "create_code_profiling_results_delta_table", "add_timer_to_all_functions",
    "write_all_code_profiling_logs_and_create_delta_table",
    # timer decorator and write to log function
    "timer",  "wrapper", "write_to_log", "safe_str", "wraps", "process_global_thread_queue_dict", "iterate_queue", 
    # standard functions
    "open", "getpid", "system", "type", "exit", "displayHTML", "udf", "builtins", "os", "psutil", "sys", "__builtin__"
]
print(f"functions_to_ignore: {functions_to_ignore}\n")

# log file write path variable
log_file_write_path = f"/Workspace/Users/robert.altmiller@databricks.com/code_profiling/local_run/{datetime.now().date()}"
print(f"log_file_write_path: {log_file_write_path}\n")

# unit testing python class and functions scope
python_class_and_fxns_scopes_unittesting = ["Inventory", "is_prime", "celsius_to_fahrenheit", "factorial", "fibonacci", "reverse_string", "sum_of_list", "is_palindrome", "max_in_list", "miles_to_kilometers", "count_vowels"]
print(f"python_class_and_fxns_scopes_unittesting: {python_class_and_fxns_scopes_unittesting}\n")

# python class scopes for python application to run code profiler on
python_class_scopes = ["aytframework", "notebooks", "__main__"] # IMPORTANT: the 'notebooks' and '__main__' are mandatory
print(f"python_class_scopes: {python_class_scopes}\n")