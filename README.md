# Single or Multi-Threaded Python Code Profiler

## Single or Multi-threaded Code Profiling Using Python Timer() Class Function Decorators and Globals() Namespace<br><br>

There are many different code profilers that exist.  Some of them are c-profile, scalene, memory_profiler, py-spy, and yappi.  

  ![other_profilers.png](/code_profiler/readme_images/other_profilers.png)

- __cProfile__ is a deterministic profiler provided by Python's standard library, and it is implemented in C which makes it relatively efficient compared to pure Python profilers. __cProfile__ records every function call and the time it takes, providing a comprehensive view of where your program spends its time.  It also does granular line-by-line code profiling.

  ![c_profiler_results.png](/code_profiler/readme_images/c_profiler_results.png)

- __Scalene__ is a high-performance CPU, memory, and GPU profiler for Python. It’s designed to be fast and accurate, with low overhead, and provides detailed insights into the performance characteristics of Python programs.  It also does granular line-by-line code profiling.

  ![scalene_results.png](/code_profiler/readme_images/scalene_results.png)

- __Memory Profiler__ is used to profile the memory usage of your code, and it provides line-by-line memory consumption analysis.

  ![memory_profiler_results.png](/code_profiler/readme_images/memory_profiler_results.png)

- __Yappi__ is a multithreaded function level profiler that can profile both CPU and wall-clock time, and it works well with multi-threaded applications.

  ![yappi_results.png](/code_profiler/readme_images/yappi_results.png)

## What are some issue with line-by-line profilers?

  - Profiling every line can generate a large amount of data, much of which might be irrelevant to the actual performance bottlenecks.
  -  Line-by-line profiling introduces significant overhead because the profiler needs to track the execution time and memory usage of each individual line of code. This can slow down the execution of the program, and make the profiling data less representative of real-world performance.
  - In more complex multi-threaded applications, asynchronous operations, or large codebases, line-by-line profiling can become difficult to interpret.
  - Understanding the results of line-by-line profiling requires a good understanding of the codebase and the underlying system. Sometimes inaccurate line-by-line interpretations can lead to wasted effort on optimizing parts of the code that don't significantly impact overall performance.

  ## How is this code profiler different from other code profilers?

- The @timer code profiling Python decorator is for standalone Python functions, Databricks notebook class functions, and Python file class functions.  It can be found in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) Python file.  We initially decorated the functions and class functions _manually_ with the @timer decorator but this was cumbersome for very large codebases and frameworks.
- It is important that we do not decorate standard library functions such as those found in the pyspark or pandas Python libraries so we added Python functions to filter out standard library functions so they do not get decorated with the @timer Python decorator.
- We added automation to add the @timer decorator to all standalone Python functions and class functions and also update the globals() dictionary namespace with the newly decorated functions.
- The code profiler can profile code bases and frameworks which run in single or multi-threaded environment.
- Here are the @timer decorator attributes that are captured for each decorated Python function.

  ![code_profiler_dataset_schema.png](/code_profiler/readme_images/code_profiler_dataset_schema.png)

- The sections in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) to pay attention to are the follwing:
  
  - __Code Profling timer() Decorator Function__
  - __Apply Timer Function to all Databricks Notebook Classes__
  - __Apply Timer Code Profiling Function to all Python File Classes__
  - __Apply Timer Code Profiling Function to all Standalone Functions__
  - __Create Delta Table From Code Profiling Log Files__

- When running the code profiler against large code bases that use deep recursion (e.g. complex json payload flatteners) it is important to dynamically control the recursion limit to prevent out of memory errors during code profiler execution.  This is accomplished directly in the timer() Python function in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) Python file.  The __'original_recursion_limit'__ is set in the [env-vars.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/env_vars.py) Python file.  See explanation below for how we dynamically control the recursion limit.

  ```python
  # step 1: original_recursion_limit is set in the env_vars.py file.
  original_recursion_limit = sys.getrecursionlimit()

  # step 2: when the @timer() decorator is called so we increment the recursion limit by 1.
  sys.setrecursionlimit(sys.getrecursionlimit() + 1)

  # step 3: set and keep track of the thead_local.depth by thread.  This is set one time, and each thread has its own thread_local.depth variable.
  if not hasattr(thread_local, 'depth'):
    thread_local.depth = 0

  # step 4: when the @timer decorator is called we increment the thread_local.depth by 1.
  thread_local.depth += 1

  # step 5: when the @timer decorator call finishes we decrement the thread_local.depth by 1, 
  # and when the thread_local.depth = 0 indicates code execution (e.g. recursion) has finished so reset the recursion limit back to the original_recursion_limit.
  finally: 
      thread_local.depth -= 1
      # Restore the original recursion limit
      if thread_local.depth == 0:
          sys.setrecursionlimit(original_recursion_limit)
  ```

## How to update the local environment variables?

- Environment variables are defined in the [env-vars.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/env_vars.py) Python file.  Please update and set __'log_file_write_path'__ and __'python_class_scopes'__ environment variables prior to running the code profiler.  The '__unique_app_id__' variable default value is 'xxxxxxxxxxxxx', '__mqueue_batch_size__' variable default value is 500, the '__print_recursion_limit__' default value is false.  It is optional to change these last three variables.
- The __'log_file_write_path'__ is used to specify the Databricks File System (DBFS) location where to write the code profiling data log files.
- The __'python_class_scopes'__ is used to specify the Python classes and/or Python code frameworks that have the Python functions defined that need to be decorated with the@ timer decorator.  The 'notebooks' and `'__main__'` attributes of the __'python_class_scopes'__  environment variable are mandatory and _SHOULD NOT BE REMOVED_.

  ![env_vars.png](/code_profiler/readme_images/env_vars.png)

-   If you set the '__print_recursion_limit__' to true, the dynamic recursion limit will print each time the @timer decorators is called by a thread.  This code below is in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) Python file.
  
    ```python
    if print_recursion_limit == True: # print the recursion limit within a running thread
      print(f"function_name: {func.__name__}()")
      print(f"thread id: {thread_id} recursion limit: {sys.getrecursionlimit()}\n")
    ```

## How to install the code profiler in your Python code base?

- Step 1: Clone down the [code_profiler](https://github.com/robert-altmiller/code_profiler) Github repository into a Databricks workspace.

  - git clone https://github.com/robert-altmiller/code_profiler.git

  ![clone_repo_in_databricks.png](/code_profiler/readme_images/clone_repo_in_databricks.png)

- Step 2: Click into the '__code_profiler__' cloned repo, and copy or move the _second level_ '__code_profiler__' folder as a root level folder in your Python framework, project, or code base.

    ![copy_code_profiler_folder.png](/code_profiler/readme_images/copy_code_profiler_folder.png)

- Step 3: Open your '__main.py__' program for loading all your Python imports and executing your code.  Make sure all the imports run first, and then add the following lines to import the environment variables and code_profiler modules, and decorate all in-scope Python functions.  Make sure to _update the email_ in the '__log_file_write_path__' below to match your Databricks workspace requirements.

  ```python
  # all custom and standard library imports need to execute first....
  from code_profiler.main import *
  
  # update the log_file_write_path environment variable
  log_file_write_path = f"/Workspace/Users/robert.altmiller@databricks.com/code_profiling/{unique_app_id}/{datetime.now().date()}"
  # Check if the path exists
  if os.path.exists(log_file_write_path):
      # Delete the directory and all its contents
      shutil.rmtree(log_file_write_path)
  print(log_file_write_path)

  # add the @timer decorator to all Python functions using automation and update globals() namespace dictionary.
  original_globals = globals()
  current_globals, function_results, nb_class_results, python_class_results = add_timer_to_all_functions(original_globals, log_file_write_path)
  print(f"standalone decorated functions: {function_results}")
  print(f"notebook class decorated functions: {nb_class_results}")
  print(f"python file class decorated functions: {python_class_results}")
  ```

- Step 4: After the main program code executes add the following lines to the end of your '__main.py__' to join all the code profiler data log text files together in Spark dataframe and write the results to a Unity Catalog Delta table.

  ```python
  write_all_code_profiling_logs_and_create_delta_table(
      spark = spark,
      global_thread_queue_dict = global_thread_queue_dict, # DO NOT MODIFY
      mqueue_batch_size = mqueue_batch_size, # DO NOT MODIFY
      catalog = "hive_metastore", # MODIFY
      schema = "default", # MODIFY
      table_name = "code_profiler_data", # MODIFY
      overwrite_profiling_data = True, # MODIFY (true = overwrite table, false = append table)
      log_file_path = log_file_write_path # DO NOT MODIFY
  )
  ```

## How is all the code profiling data captured after the code profiler finishes?

- Each time a decorated Python function is called by a thread a record gets added to a Python queue.Queue() for that specific thread_id.  Each thread_id has a separate Python queue for storing profiling data in a 'global_thread_queue_dict' global Python dictionary.  See code snippet below for how the code profiling data is stored by thread_id.
 
  ```python
  # initialize global dictionary for capturing code profiling data
  global_thread_queue_dict = {}
  # each key in the dict corresponds to a thread_id and the value is queue.Queue()
  global_thread_queue_dict[thread_id] = queue.Queue()
  # how each code profiling row records is stored when @timer decorator is called
  global_thread_queue_dict[thread_id].put(log_message_dict) 
  ```

- At the end of the code profiler execution all the captured profiling data by thread_id in the 'global_thread_queue_dict' global dictionary is written out to the Databricks File System (DFBS) in a log file named '{thread_id}_log.txt' for each thread.  Multithreading is used to process each thread_id queue.Queue() in the 'global_thread_queue_dict' dictionary in parallel and create all the log files at once.

  ```python
  def write_to_log(queue, thread_id, batch_size, log_file_path):
      """write a message to a log file in Databricks DBFS."""
      # Ensure the directory exists
      os.makedirs(log_file_path, exist_ok=True)
      log_file_thread_path = f"{log_file_path}/{thread_id}_log.txt"

      queue_batch = []
      for message in iterate_queue(queue):
          queue_batch.append(f"{json.dumps(message)},\n")
          if len(queue_batch) >= batch_size:
              with open(log_file_thread_path, 'a') as log_file:
                  log_file.writelines(queue_batch)
              queue_batch.clear()

      # write remaining messages if any
      if queue_batch:
          with open(log_file_thread_path, 'a') as log_file:
              log_file.writelines(queue_batch)

  def process_global_thread_queue_dict(global_log_dict, batch_size, log_file_path = log_file_write_path):
      """process and log all code profiling messages for all threads in the global_thread_queue dictionary"""
      threads = []
      for thread_id, queue in global_log_dict.items():
          thread = threading.Thread(target=write_to_log, args=(queue, thread_id, batch_size, log_file_path))
          threads.append(thread)
          thread.start()

      # wait for all threads to complete (multithreading)
      for thread in threads:
          thread.join()
  ```

- All these local thread log text files are joined to together in one large Spark dataframe and written out to a Unity Catalog Delta table which can be queried to identify code execution performance bottlenecks.

## How to query the code profiler results from the Unity Catalog Delta table?

- After the code profiler finishes there is a '__query_code_profiler_results_general.py__' Databricks notebook under the '__query_profiler_data__' folder for querying the code profiler results data.  It has a set of standard queries for identifying performance bottlenecks in code.

    ![query_profiler_data.png](/code_profiler/readme_images/query_profiler_data.png)

- Before running any of the queries please update the following variables in the '__query_code_profiler_results_general.py__' Databricks notebook.

  ```python
  catalog = "hive_metastore" # MODIFY
  schema = "default" # MODIFY
  table_name = "code_profiler_data" # MODIFY
  ```

- General Spark.sql() queries exist for the following:
  
  - How Many Total Unique Threads Exist Grouped By unique_app_id?
  - Get a Unique List of Python Functions Profiled with Code Profiler and How Many Times They Are Called Across All Threads.
  - Get Function Count and Function Total Execution Time Grouped by unique_app_id, Function, and Thread.
  - Get Function Count and Average Execution Time For All Threads Grouped by Function and unique_app_id.
  - Which Python Functions Had the Highest CPU Consumption Percentage? 
  - What is the Total Execution Time by unique_app_id for All Threads?
  - Get Individual Total Function Time, and Average Function Time Per Function Call Grouped by unique_app_id and Function.
  - What is the Total Execution Time by unique_app_id and All Threads?
  - What is the Function Total Execution Time by unique_app_id and a Single Thread?

## How to run a code_profiler unit test to test how it works and check the results?

- There are 3 different unit tests that can be run locally or in Databricks to test out the code profiler execution and profiling data results.  If you run these unit tests locally in an integrated development environment (IDE) the code profiler data log text files will be created locally, and a Spark dataframe will be created that joins all the results from all the code profiling log data text files.  A persisted Delta table will __NOT__ be created in Databricks Unity Catalog (UC).  You can find the unit tests in the following location.  

- __IMPORTANT__: When executing the unit tests in Databricks make sure you __DETATCH AND REATTACH TO THE CLUSTER__ each time you re-run the code.  If you do not you will decorate the standalone Python functions and class functions more than once with the @timer decorator.  This is not an issue when running the unit tests in a local IDE environment.

    ![code_profiler_unit_tests.png](/code_profiler/readme_images/code_profiler_unit_tests.png)

- Here is the location of the unit testing notebooks, and the standalone Python functions and class functions used by the code profiler unit tests: '__code_profiler_test_nb.py__', '__code_profiler_test_py_files_nb.py__', and '__code_profiler_test_py_files.py__'

    ![code_profiler_unit_tests_organization.png](/code_profiler/readme_images/code_profiler_unit_tests_organization.png)

- When you run a unit test notebook here is how to verify if the unit test completed successfully in your IDE or in Databricks.  All three unit tests will produce a similar resulting dataset.

    ![unit_test_local_run_results.png](/code_profiler/readme_images/unit_test_local_run_results.png)


## Code profiler version updates

- 8/29/2024 - We record the 'getrecursionlimit()' in the profiler_tools.py when each timer() decorated function is called.  When we create the final Delta table of consolidated code profiling results 'recursion_limit' is a new column which has been added.  Tested changes in local IDE and in Databricks.
- 8/29/2024 - We added a folder called 'llm_integration' to be able to read the Python function code and then submit to a large language model (LLM) to get optimization recommendations.  This folder is standalone and is not integrated with the final code profiling results output Delta table.
- 9/3/2024 - We added some code to read the profiling function code and then store it in a column named 'source_code_compressed' in the final code profiling results output Delta table.  We compress the source code using 'base64' and 'zlib' Python libraries.  There is an example of how to use a Python UDF to decode the 'source_code_compressed' column into a 'source_code_decompressed' column with the original source code.  This example can be found in __query_profiler_data folder --> query_code_profiler_results_general.py__.  This can then be used with any external LLM to get function level optimization recommendations.