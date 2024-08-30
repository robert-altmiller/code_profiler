# library imports
import inspect

# get function code
def get_function_code(globals, class_name=None, function_name=None):
    """
    Get the source code of a function or a class method using the globals namespace.
    :param globals: The globals dictionary.
    :param class_name: The name of the class (optional if looking for a top-level function).
    :param function_name: The name of the function or method.
    :return: The source code of the function or method.
    :raises KeyError: If the function or class is not found.
    :raises TypeError: If the object is not a function or does not have source code.
    """
    try:
        # If class_name is provided, retrieve the class first
        if class_name:
            # Attempt to retrieve the class from the global namespace
            class_obj = globals[class_name]
            # Attempt to retrieve the method from the class
            function_code = getattr(class_obj, function_name)
        else:
            # If no class_name is provided, assume it's a top-level function
            function_code = globals[function_name]
        # Check if the function is decorated and has the __wrapped__ attribute
        if hasattr(function_code, "__wrapped__"):
            function_code = function_code.__wrapped__
        # Get the source code of the original function
        return inspect.getsource(function_code)
    except KeyError:
        if class_name:
            raise KeyError(f"Class '{class_name}' or method '{function_name}' not found in the global namespace.")
        else:
            raise KeyError(f"Function '{function_name}' not found in the global namespace.")
    except TypeError:
        raise TypeError(f"The object named '{function_name}' is not a function or does not have source code.")
    except Exception as e:
        # Catch-all for any other exceptions
        raise RuntimeError(f"An unexpected error occurred: {e}")