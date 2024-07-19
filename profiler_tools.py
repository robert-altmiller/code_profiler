from code_profiler.env_vars import *

def write_to_log(thread_id, message, log_file_path = log_file_write_path):
    """
    write a message to a log file in Databricks DBFS.
    
    Args:
    message (str): The message to write to the log file.
    log_file_path (str): The path to the log file in DBFS (default: '/dbfs/FileStore/logs/my_log.txt').
    """
    
    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Format the log message with the timestamp
    log_message = f"{json.dumps(message)},\n"
    
    # Append the log message to the log file
    if not os.path.exists(log_file_path):
        try:
            os.makedirs(log_file_path)
        except FileExistsError:
            print(f"The directory {log_file_path} already exists.")
        except Exception as e:
            print(f"An error occurred while creating {log_file_path}: {e}")
    with log_lock:
        with open(f"{log_file_path}/{thread_id}_log.txt", 'a') as log_file:
            log_file.write(log_message)


def timer(func):
    """decorator that measures the execution time of a function and logs it, without altering return values."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        sys.setrecursionlimit(sys.getrecursionlimit() + 1)

        if not hasattr(thread_local, 'depth'):
            thread_local.depth = 0

        thread_local.depth += 1

        try:
            thread_id = threading.get_ident()  # Get the current thread identifier
            if print_recursion_limit == True:
                print(f"function_name: {func.__name__}()")
                print(f"thread id: {thread_id} recursion limit: {sys.getrecursionlimit()}\n")
            start_time = time.time()  # Start time
            start_mem = process.memory_info().rss  # Memory usage at the start
            start_cpu = process.cpu_percent(interval=None)  # CPU usage at the start

            result = func(*args, **kwargs)  # Execute the function and store the result

            end_time = time.time()  # End time
            end_mem = process.memory_info().rss  # Memory usage at the end
            end_cpu = process.cpu_percent(interval=None)  # CPU usage at the end
            execution_time = end_time - start_time  # Calculate the time taken
            # Get the current timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            args_details = ',XSEPX,'.join([str(arg) for arg in args]) # create a detailed string of all positional arguments  
            kwargs_details = ',XSEPX,'.join([f"{k}={v}" for k, v in kwargs.items()])  # create a detailed string of all keyword arguments

            log_message_dict = {
                "ingestion_date": timestamp,
                "thread_id": thread_id,
                "process_id": pid,
                "tenant_id": str(unique_app_id),
                "function_name": f"{func.__name__}()",
                "execution_time": f"{execution_time:.6f}",
                "start_time": str(datetime.fromtimestamp(start_time)),
                "end_time": str(datetime.fromtimestamp(end_time)),
                "memory_usage_bytes": end_mem - start_mem,
                "cpu_usage_percent": end_cpu - start_cpu,
                "arguments": args_details,
                "kwargs": kwargs_details,
                "return_value": str(result)
            }
            write_to_log(thread_id, log_message_dict)
            return result  # Return the original result
        finally:
            thread_local.depth -= 1
            # Restore the original recursion limit
            if thread_local.depth == 0:
                sys.setrecursionlimit(original_recursion_limit)
    return wrapper


##--------------------------------------------------------------------------------------------------------------------------------------


##--------------------------------------- Apply Timer Function to all Databricks Notebook Classes --------------------------------------


def get_classes_from_globals(globals):
    """get all Databricks notebooks imported classes"""
    imported_classes = []
    # Get the global namespace
    globals_dict = globals
    for name, obj in list(globals_dict.items()):
        if inspect.isclass(obj):
            imported_classes.append(f"{name}:{obj}")
    return list(set(imported_classes))


def apply_timer_decorator_to_nb_class_function(globals, nb_class_name, python_class_scopes):
    """apply the code profiler timer() decorator to individual Databricks notebook (nb) class"""

    module = globals.get(nb_class_name)
    results = []
    for attr_name in dir(module):
        try:
            attr_value = getattr(module, attr_name)        
        except Exception as e:
            log_info = f"Error accessing {attr_name} in {module.__name__}: {e}"
            continue
        if isinstance(attr_value, types.FunctionType) and \
                      nb_class_name not in functions_to_ignore and \
                      attr_name not in functions_to_ignore and \
                      f"{nb_class_name}.{attr_name}" not in python_class_scopes:
            print(f"nb_class_function attr_name: {attr_name}")
            print(f"nb_class_function attr_value: {attr_value}\n")
            decorated_function = timer(attr_value)
            # set the decorated function on the module
            setattr(module, attr_name, decorated_function)
            # update the same in the globals() if it's tracked there
            globals[attr_name] = decorated_function
            results.append({"attr_name": attr_name, "attr_value": attr_value})
    return globals, results



def apply_timer_decorator_to_all_nb_class_functions(globals, class_scopes = python_class_and_fxns_scopes_unittesting, python_class_scopes = []):
    """apply the code profiler timer() decorator to all Databricks notebook (nb) classes"""
    cls_functions_list = []
    nb_classes = get_classes_from_globals(globals)
    for cls in nb_classes:
        nb_class_name = cls.split(":")[0]
        nb_class_path = cls.split(":")[1]
        if check_items_in_string(class_scopes, nb_class_path) == True: # check if class object is in class_scopes
            print("------------------------------------------------------------------------\n")
            print(f"nb_class_name: {nb_class_name}\nnb_class_path: {nb_class_path}\n")
            globals, results = apply_timer_decorator_to_nb_class_function(globals, nb_class_name, python_class_scopes)
            # combine class and function names
            cls_functions_list += [f"{nb_class_name}.{result['attr_name']}" for result in results]
    return globals, cls_functions_list


##-------------------------------------------------------------------------------------------------------------------------------------


##---------------------------------- Apply Timer Code Profiling Function to all Python File Classes -----------------------------------


def check_items_in_string(items, string):
    return any(item in string for item in items)


def get_imported_classes():
    """get imported classes and classes from local Databricks *.py files"""
    imported_classes = []
    # iterate over all imported modules
    for module_name, module in list(sys.modules.items()):
        try:
            # Iterate over all members of the module
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj):
                    # append the class or function name and module name to the list
                    imported_classes.append(f"{module_name}.{name}")
        except Exception as e:
            # Handle any exceptions raised during inspection
            log_info = f"Could not inspect module {module_name}: {e}"
    return list(set(imported_classes))


def dynamic_import_and_set_global(class_path: str):
    """dynamically import a class and set it in the global namespace"""
    module_path, class_name = class_path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    imported_class = getattr(module, class_name)
    # globals()[class_name] = imported_class  # Set the class in the global namespace
    return str(imported_class)


def apply_timer_decorator_to_python_class_function(globals, python_class_name, nb_class_results):
    """apply the code profiler timer() decorator to individual Python class functions"""
    cls =  globals.get(python_class_name)
    results = []
    for attr_name in dir(cls):
        try:
            attr_value = getattr(cls, attr_name)
        except Exception as e:
            log_info = f"Error accessing attribute {attr_name} in class {cls.__name__}: {e}"
            continue
        if isinstance(attr_value, types.FunctionType) and \
                      python_class_name not in functions_to_ignore and \
                      attr_name not in functions_to_ignore and \
                      f"{python_class_name}.{attr_name}" not in nb_class_results:
            print(f"python_class_function attr_name: {attr_name}")
            print(f"python_class_function attr_value: {attr_value}\n")
            decorated_function = timer(attr_value)
            # set the decorated function on the module
            setattr(cls, attr_name, decorated_function)
            # update the same in the globals() if it's tracked there
            globals[attr_name] = decorated_function
            results.append({"attr_name": attr_name, "attr_value": attr_value})
    return globals, results


def apply_timer_decorator_to_all_python_class_functions(globals, class_scopes = python_class_and_fxns_scopes_unittesting, nb_class_results = []):
    """
    apply the code profiler timer() decorator to all Python class functions
    nb = notebook
    """
    classes = get_imported_classes() # get all imported classes
    imported_classes = []
    for cls in classes:
        if check_items_in_string(class_scopes, cls) == True: # check if class name is in class_scopes
            imported_classes.append(dynamic_import_and_set_global(cls))
        imported_classes = list(set(imported_classes))

    # this function needs to account for classes only
    cls_functions_list = []
    for python_imported_class in imported_classes:
        if check_items_in_string(class_scopes, python_imported_class) == True: # check if imported_class is in class_scopes
            python_class_name = python_imported_class.split('.')[-1].strip("'>") # get the class name
            print("------------------------------------------------------------------------")
            print(f"python_class_name: {python_class_name}\npython_class_path: {python_imported_class}\n")
            globals, results = apply_timer_decorator_to_python_class_function(globals, python_class_name, nb_class_results)
            # combine class and function names
            cls_functions_list += [f"{python_class_name}.{result['attr_name']}" for result in results]
    return globals, cls_functions_list


##-------------------------------------------------------------------------------------------------------------------------------------


##----------------------------------------- Apply Timer Code Profiling Function to all Standalone Functions ---------------------------

def is_library_defined_function(func):
    """check if a function is library defined."""
    if inspect.isfunction(func):
        module_name = func.__module__
        builtin_mod_names = list(sys.builtin_module_names)
        builtin_mod_names.extend(["builtin", "typing", "pyspark"])
        # Ensure the module has the __file__ attribute before checking its path
        # if hasattr(sys.modules[module_name], '__file__') == True then function comes from a python file (see below)
        if hasattr(sys.modules[module_name], '__file__') and check_items_in_string(functions_to_ignore, module_name) == False: # then function comes from a file
            if check_items_in_string(builtin_mod_names, module_name) == True:
                return True
    return False


def apply_timer_decorator_to_all_python_functions(globals):
    """apply the code profiler timer() decorator to all functions in the current global namespace."""
    functions = []
    decorated_functions = {}
    functions_list = []
    for name, obj in list(globals.items()):  # Find functions in global namespace
        # print(f"name: {name} and object: {obj}\n")
        if inspect.isfunction(obj) and is_library_defined_function(obj) == False and name not in functions_to_ignore:
            # Apply the timer decorator to each function
            decorated_function = timer(obj)
            globals[name] = decorated_function  # Update the global namespace
            functions_list.append(name)
            print(f"Found object: {obj}")
            print(f"Found function: {name}")
            print(f"Decorated function: {decorated_function}\n")
    return globals, functions_list


##-------------------------------------------------------------------------------------------------------------------------------------


##----------------------------------------- Create Delta Table From Code Profiling Log Files ------------------------------------------


def get_profiling_result_paths(directory_path):
    """list all files in the directory"""
    return glob.glob(os.path.join(directory_path, '*'))


def get_all_profiling_results_joined(directory_path):
    """join on the code profiling results into a single dataframe and write to delta managed table"""
    files = get_profiling_result_paths(directory_path)
    log_messages_combined = ""
    for file in files:
        print(file)
        # Open the file in read mode
        with open(f"{file}", "r") as file:
            # Read the contents of the file
            log_messages_combined += file.read() + "\n"
    return log_messages_combined[:-1] # drop the last comma


def write_profiling_results_to_delta_table(spark, directory_path, catalog, schema, table_name, delete_data):
    """write all the joined code profiling results to a delta table"""
    profiling_data = get_all_profiling_results_joined(directory_path)
    # split the profiling_data into individual lines
    counter = 0
    profiling_data_rows = []
    lines = profiling_data.splitlines()
    for line in lines:
        if line.strip() == "":
            continue  # Skip empty lines
        print(line)
        profiling_data_single_event = json.loads(line[:-1]) # drop all the commas
        print(f"counter {counter}: json load successful\n")
        counter += 1
        # convert the JSON data into a list of dictionaries
        profiling_data_rows.append(profiling_data_single_event)

    profiling_data_schema = StructType([
        StructField("ingestion_date", StringType(), True),
        StructField("thread_id", StringType(), True),
        StructField("process_id", StringType(), True),
        StructField("tenant_id", StringType(), True),
        StructField("function_name", StringType(), True),
        StructField("execution_time", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("memory_usage_bytes", StringType(), True),
        StructField("cpu_usage_percent", StringType(), True),
        StructField("arguments", StringType(), True),
        StructField("kwargs", StringType(), True),
        StructField("return_value", StringType(), True)
    ])

    # create DataFrame from the list of Python dictionaries and the schema
    log_message_df = spark.createDataFrame(profiling_data_rows, profiling_data_schema) \
        .withColumn("arguments", split(col("arguments"), ",XSEPX,")) \
        .withColumn("kwargs", split(col("kwargs"), ",XSEPX,"))
    
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")
    delta_table_path = f"`{catalog}`.`{schema}`.`{table_name}`"
    if delete_data: spark.sql(f"DROP TABLE IF EXISTS {delta_table_path}")
    log_message_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(delta_table_path)
    return log_message_df


def create_code_profiling_results_delta_table(spark, catalog, schema, table_name, overwrite_profiling_data, log_file_path = log_file_write_path):
    """create code profiling results delta table"""
    return write_profiling_results_to_delta_table(
        spark = spark,
        directory_path = log_file_path,
        catalog = catalog,
        schema = schema,
        table_name = table_name,
        delete_data = overwrite_profiling_data
    )


##-------------------------------------------------------------------------------------------------------------------------------------