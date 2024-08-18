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

- The timer() code profiling decorator for standalone Python functions, Databricks class functions, and Python file class functions can be found in the [profiler_tools.py](https://github.com/robert-altmiller/code_profiler/blob/main/code_profiler/profiler_tools.py) Python file.