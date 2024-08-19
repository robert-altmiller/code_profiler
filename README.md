# Single or Multi-Threaded Python Code Profiler

## Single or Multi-threaded Code Profiling Using Python Timer() Class Function Decorators and Globals() Namespace<br><br>

There are many different code profilers that exist.  Some of them are c-profiler, scalene, memory_profiler, py-spy, and yappi.  

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

- The timer() code profiling Python decorator is for standalone Python functions, Databricks notebook class functions, and Python file class functions.  It can be found in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) Python file.  We initially decorated the functions and class functions _manually_ with the @timer decorator but this was cumbersome for very large codebases.
- It is important that we do not decorate standard library functions such as those found in the pyspark Python library. We added functions to filter out standard library functions so they do not get decorated with the @timer Python decorator.
- We added automation to add the timer() decorator to all standalone Python functions and class functions and also update the globals() dictionary namespace with the newly decorated functions.  
- The sections in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) to pay attention to are the follwing:
  
  - __Code Profling timer() Decorator Function__
  - __Apply Timer Function to all Databricks Notebook Classes__
  - __Apply Timer Code Profiling Function to all Python File Classes__
  - __Apply Timer Code Profiling Function to all Standalone Functions__
  - __Create Delta Table From Code Profiling Log Files__

- When running the code profiler against large code bases that use deep recursion (e.g. complex json payload flatteners) it is important to dynamically contol the recursion limit to prevent out of memory errors during code profiler execution.  This is accomplished directly in the timer() Python function in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) Python file.  The __'original_recursion_limit'__ is set in the [env-vars.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/env_vars.py) Python file.  See explanation below for how we dynamically control the recursion limit.

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

-   If you set the '__print_recursion_limit__' to true the dynamic recursion limit will print each time the @timer decorators is called by a thread.  This code below is in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) Python file.
  
    ```python
    if print_recursion_limit == True: # print the recursion limit within a running thread
      print(f"function_name: {func.__name__}()")
      print(f"thread id: {thread_id} recursion limit: {sys.getrecursionlimit()}\n")
    ```

## How to install the code profiler in your Python code base?

- Step 1: Clone down the [code_profiler](https://github.com/robert-altmiller/code_profiler) Github repository into a Databricks workspace.

    ![clone_repo.png](/code_profiler/readme_images/clone_repo.png)

- Step 2: Click into the '__code_profiler__' cloned repo, and copy or move the _second level_ '__code_profiler__' folder as a root level folder in your Python framework, project, or code base.

    ![copy_code_profiler_folder.png](/code_profiler/readme_images/copy_code_profiler_folder.png)

- Step 3: Open your '__main.py__' program for loading all your Python imports and executing your code.  Make sure all the imports run first, and then add the following lines to import the environment variables and code_profiler modules, and also decorate all in-scope Python functions.

  ```python
  # all custom and standard library imports need to execute first....
  from code_profiler.main import *
  
  # update the log_file_write_path environment variable
  log_file_write_path = f"/Workspace/Users/robert.altmiller@databricks.com/code_profiling/{unique_app_id}/{datetime.now().date()}"
  print(log_file_write_path)

  # add the @timer decorator to all Python functions using automation and update globals() namespace dictionary.
  globals, function_results, nb_class_results, python_class_results = add_timer_to_all_functions(globals(), log_file_path):
  print(f"standalone decorated functions: {function_results}")
  print(f"notebook class decorated functions: {nb_class_results}")
  print(f"python file class decorated functions: {python_class_results}")
  ```

- Step 4: After the main program code executes add the following lines to the end of your '__main.py__' to join all the code profiler data log files together in Spark dataframe and write the results to a Unity Catalog Delta table.

  ```python
  write_all_code_profiling_logs_and_create_delta_table(
      spark = spark,
      global_thread_queue_dict = global_thread_queue_dict, # DO NOT MODIFY
      mqueue_batch_size = mqueue_batch_size, 
      catalog = "hive_metastore", # MODIFY
      schema = "default", # MODIFY
      table_name = "code_profiler_data", # MODIFY
      overwrite_profiling_data = True, # MODIFY ()
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

- All of these local thread log text files are joined to together in one large Spark dataframe and written out to a Unity Catalog Delta table which can be queried to identify code execution performance bottlenecks.

## How can I query the code profiler results from the Unity Catalog Delta table?  What standard queries exist to analyze performance bottlenecks?