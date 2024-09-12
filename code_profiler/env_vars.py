# library imports
import builtins, glob, hashlib, importlib, inspect, json
import os, psutil, queue, shutil, sys, threading, time, types, zlib
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from functools import wraps
from code_profiler.llm_integration.llm_optimization import *

def is_running_in_databricks():
    """check if code is running in Databricks or locally"""
    # Databricks typically sets these environment variables
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        print("code is running in databricks....\n")
        return True
    else:
        #print("code is running locally....\n")
        return False

# create spark session and install requirements (e.g. openai)
if is_running_in_databricks() == False:
    # create a new local Spark session (DO NOT MODIFY)
    spark = SparkSession.builder \
        .appName("CodeProfiling") \
        .getOrCreate()
    install_requirements(requirements_file = f'{os.path.dirname(os.path.abspath(__file__))}/llm_integration/requirements.txt')
else:  install_requirements(requirements_file='requirements.txt')

# unique identifer for python code / application name (OPTIONAL MODIFY)
unique_app_id = "xxxxxxxxxxxxx" 
print(f"unique_app_id: {unique_app_id}")

# local parameters (DO NOT MODIFY)
thread_local = threading.local()
pid = os.getpid() # DO NOT MODIFY
process = psutil.Process(pid) # DO NOT MODIFY
original_recursion_limit = sys.getrecursionlimit() # DO NOT MODIFY
global_thread_queue_dict = {} # DO NOT MODIFY

# define the message queue batch size when writing code profiling data logs (OPTIONAL MODIFY)
mqueue_batch_size = 500
print(f"message queue batch size: {mqueue_batch_size}")

# explicity print the recursion limit (OPTIONAL MODIFY)
print_recursion_limit = False
print(f"\nprint_recursion_limit: {print_recursion_limit}\n")

# get builtin types using builtins class
builtin_types = [t.__name__ for t in vars(builtins).values() if isinstance(t, type)] # DO NOT MODIFY
print(f"builtin_types: {builtin_types}")

# functions to ignore
functions_to_ignore = [
    # --- DO NOT MODIFY THESE FUNCTIONS TO IGNORE ---
    # notebook class function helpers (DO NOT MODIFY)
    "get_classes_from_globals", "apply_timer_decorator_to_nb_class_function", "apply_timer_decorator_to_all_nb_class_functions",
    # Python file class function helpers (DO NOT MODIFY)
    "check_items_in_string", "get_imported_classes", "dynamic_import_and_set_global",
    "apply_timer_decorator_to_python_class_function", "apply_timer_decorator_to_all_python_class_functions",
    # all standalone Python functions helpers (DO NOT MODIFY)
    "is_library_defined_function", "apply_timer_decorator_to_all_python_functions", "is_running_in_databricks",
    # llm helper functions (DO NOT MODIFY)
    "install_requirements", "get_llm_model_response", "sanitize_code_recs_llm_response", "spark_get_llm_code_recs_udf", "spark_get_llm_opt_code_udf",
    # create delta table function helpers (DO NOT MODIFY)
    "get_profiling_result_paths", "get_all_profiling_results_joined", "write_profiling_results_to_delta_table", 
    "create_code_profiling_results_delta_table", "add_timer_to_all_functions",
    "write_all_code_profiling_logs_and_create_delta_table",
    # timer decorator and write to log function (DO NOT MODIFY)
    "timer",  "wrapper", "write_to_log", "safe_str", "wraps", "process_global_thread_queue_dict", "iterate_queue", 
    # standard functions (DO NOT MODIFY)
    "open", "getpid", "system", "type", "exit", "displayHTML", "udf", "builtins", "os", "psutil", "sys", "__builtin__", "__repr__"
]
print(f"functions_to_ignore: {functions_to_ignore}\n")

# log file write path variable (MODIFY)
log_file_write_path = f"/Workspace/Users/robert.altmiller@databricks.com/code_profiling/local_run/{datetime.now().date()}"
print(f"log_file_write_path: {log_file_write_path}\n")

# unit testing python class and functions scope (DO NOT MODIFY)
python_class_and_fxns_scopes_unittesting = [
    "Inventory", "is_prime", "factorial", "celsius_to_fahrenheit", 
    "fibonacci", "reverse_string", "sum_of_list", "is_palindrome", "max_in_list", 
    "miles_to_kilometers", "count_vowels"
]
print(f"python_class_and_fxns_scopes_unittesting: {python_class_and_fxns_scopes_unittesting}\n")

# python class scopes for python application to run code profiler on (MODIFY)
python_class_scopes = ["aytframework", "notebooks", "__main__"] # IMPORTANT: the 'notebooks' and '__main__' are mandatory
print(f"python_class_scopes: {python_class_scopes}\n")