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

- The timer() code profiling Python decorator for standalone Python functions, Databricks class functions, and Python file class functions can be found in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) Python file.  We initially decorated the functions and class functions _manually_ with the @timer decorator but this was cumbersome for very large codebases.  
- We added automation to add the timer() decorator to all standalone and class functions and update the globals() dictionary namespace with the newly decorated functions.  The sections in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) to pay attention to are the follwing:
  
  - __Code Profling timer() Decorator Function__
  - __Apply Timer Function to all Databricks Notebook Classes__
  - __Apply Timer Code Profiling Function to all Python File Classes__
  - __Apply Timer Code Profiling Function to all Standalone Functions__
  - __Create Delta Table From Code Profiling Log Files__

## How is all the code profiling data captured after the profiler finishes?

- Each time a decorated Python function is called by a thread a record gets added to a Python queue.Queue() for that specific thread_id.  Each thread_id has a separate Python queue for storing profiling data in a 'global_thread_queue_dict' global Python dictionary.  See code snippet below for how the code profiling data is stored by thread_id.
 
  ```python
  # initialize global dictionary for capturing code profiling data
  global_thread_queue_dict = {}
  # each key in the dict corresponds to a thread_id and the value is queue.Queue()
  global_thread_queue_dict[thread_id] = queue.Queue()
  # how each code profiling row records is stored when @timer decorator is called
  global_thread_queue_dict[thread_id].put(log_message_dict) 
  ```

- At the end of the code profiler execution all the captured profiling data by thread_id in the 'global_thread_queue_dict' global dictionary is written out to the Databricks File System (DFBS) in a log file named '{thread_id}_log.txt' for each thread.  Multithreading is used to process each thread_id queue in the 'global_thread_queue_dict' dictionary in parallel and create all the log files at once.

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