from code_profiler.profiler_tools import *

def add_timer_to_all_functions(globals):
    # Example usage: Call this function after all imports to apply timer decorator
    globals, function_results = apply_timer_decorator_to_all_python_functions(globals)
    globals, nb_class_results = apply_timer_decorator_to_all_nb_class_functions(globals, python_class_scopes)
    globals, python_class_results = apply_timer_decorator_to_all_python_class_functions(globals, python_class_scopes, nb_class_results)
    return globals, function_results, nb_class_results, python_class_results

# or

# def add_timer_to_code_base(globals):
#     # Example usage: Call this function after all imports to apply timer decorator
#     globals, function_results = apply_timer_decorator_to_all_python_functions(globals)
#     globals, python_class_results = apply_timer_decorator_to_all_python_class_functions(globals, python_class_scopes)
#     globals, nb_class_results = apply_timer_decorator_to_all_nb_class_functions(globals, python_class_scopes, python_class_results)
#     return globals, function_results, nb_class_results, python_class_results