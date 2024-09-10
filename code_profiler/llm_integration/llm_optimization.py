# library imports
from code_profiler.llm_integration.install_requirements import *
from code_profiler.llm_integration.llm_prompts import *
import inspect, json, ast, re
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
import openai


def sanitize_code_recs_llm_response(response):
    """function to sanitize the LLM response for optimized code recommendations"""
    try:
        # find the second occurrence of "]" and chop off everything after it
        second_closing_bracket_index = response.find("]", response.find("]") + 1)
        # if we find the second "]", trim the string after it
        if second_closing_bracket_index != -1:
            response = response[:second_closing_bracket_index + 1]
    except: print('')
    # allow only valid characters (alphanumeric, spaces, commas, square brackets, quotes)
    response = re.sub(r"[^a-zA-Z0-9\s\[\]\",']", "", response)
    # remove excess whitespace
    response = re.sub(r'\s+', ' ', response).strip()
    return response


def get_llm_model_response_udf(my_api_key, my_base_url, my_system_prompt, my_user_prompt, my_model):
    """Function to get LLM model response, refactored to work as a user defined function (UDF)"""
    try:
        # Set up OpenAI API key
        openai.api_key = my_api_key
        openai.api_base = my_base_url

        # Call the OpenAI API to get the response
        response = openai.ChatCompletion.create(
            model=my_model,
            messages=[
                {
                    "role": "system", 
                    "content": my_system_prompt 
                },
                {
                    "role": "user",
                    "content": my_user_prompt
                }
            ]
        )

        # Extract the response content
        return response['choices'][0]['message']['content']
    
    except Exception as e:
        return str(e)


# wrap the function as a PySpark UDF for code recommendations and optimized code generation
spark_get_llm_code_recs_udf = udf(lambda api_key, base_url, system_prompt, user_prompt, model: 
                             sanitize_code_recs_llm_response(get_llm_model_response_udf(api_key, base_url, system_prompt, user_prompt, model)), 
                             StringType())

spark_get_llm_opt_code_udf = udf(lambda api_key, base_url, system_prompt, user_prompt, model: 
                             get_llm_model_response_udf(api_key, base_url, system_prompt, user_prompt, model),
                             StringType())


# get function code (not being used but it is a helpful function)
# def get_function_code(globals, class_name=None, function_name=None):
#     """
#     Get the source code of a function or a class method using the globals namespace.
#     :param globals: The globals dictionary.
#     :param class_name: The name of the class (optional if looking for a top-level function).
#     :param function_name: The name of the function or method.
#     :return: The source code of the function or method.
#     :raises KeyError: If the function or class is not found.
#     :raises TypeError: If the object is not a function or does not have source code.
#     """
#     try:
#         # If class_name is provided, retrieve the class first
#         if class_name:
#             # Attempt to retrieve the class from the global namespace
#             class_obj = globals[class_name]
#             # Attempt to retrieve the method from the class
#             function_code = getattr(class_obj, function_name)
#         else:
#             # If no class_name is provided, assume it's a top-level function
#             function_code = globals[function_name]
#         # Check if the function is decorated and has the __wrapped__ attribute
#         if hasattr(function_code, "__wrapped__"):
#             function_code = function_code.__wrapped__
#         # Get the source code of the original function
#         return inspect.getsource(function_code)
#     except KeyError:
#         if class_name:
#             raise KeyError(f"Class '{class_name}' or method '{function_name}' not found in the global namespace.")
#         else:
#             raise KeyError(f"Function '{function_name}' not found in the global namespace.")
#     except TypeError:
#         raise TypeError(f"The object named '{function_name}' is not a function or does not have source code.")
#     except Exception as e:
#         # Catch-all for any other exceptions
#         raise RuntimeError(f"An unexpected error occurred: {e}")

# Get function optimization recommendation from LLM (e.g. standalone functions)
# for fxn_name in function_results:
#     source_code = get_function_code(current_globals, function_name = fxn_name)
#     print(f"\n{fxn_name}():\n{source_code}\n")