from code_profiler.profiler_tools import *

def add_timer_to_all_functions(globals, log_file_path, class_scopes = python_class_scopes):
    # Example usage: Call this function after all imports to apply timer decorator
    globals, function_results = apply_timer_decorator_to_all_python_functions(globals, log_file_path = log_file_path)
    globals, nb_class_results = apply_timer_decorator_to_all_nb_class_functions(globals, class_scopes, log_file_path = log_file_path) # nb_class_results is first
    globals, python_class_results = apply_timer_decorator_to_all_python_class_functions(globals, class_scopes, nb_class_results, log_file_path = log_file_path) # python_class_results is second
    return globals, function_results, nb_class_results, python_class_results

#or

# def add_timer_to_all_functions(globals, log_file_path):
#     # Example usage: Call this function after all imports to apply timer decorator
#     globals, function_results = apply_timer_decorator_to_all_python_functions(globals, log_file_path = log_file_path)
#     globals, python_class_results = apply_timer_decorator_to_all_python_class_functions(globals, python_class_scopes, log_file_path = log_file_path) # python_class_results is first
#     globals, nb_class_results = apply_timer_decorator_to_all_nb_class_functions(globals, python_class_scopes, python_class_results log_file_path = log_file_path) # nb_class_results is second
#     return globals, function_results, nb_class_results, python_class_results


def write_all_code_profiling_logs_and_create_delta_table(spark, global_thread_queue_dict, mqueue_batch_size, catalog, schema, table_name, overwrite_profiling_data, log_file_path):
    """write all code profiling logs and create code profiling delta table"""
    # create code profiling data log files
    process_global_thread_queue_dict(global_thread_queue_dict, mqueue_batch_size, log_file_path)
    # create_code_profiling_results_delta_table
    log_message_df = create_code_profiling_results_delta_table(spark, catalog, schema, table_name,  overwrite_profiling_data, log_file_path)
    return log_message_df