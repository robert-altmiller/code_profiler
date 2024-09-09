# pip install libraries
from code_profiler.llm_integration.install_requirements import *

# library imports
from code_profiler.llm_integration.llm_prompts import *
import inspect, json, ast, re
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from openai import OpenAI

# Initialize Spark session
spark = SparkSession.builder.appName("CompressionExample").getOrCreate()

# get function code
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

# # Get function optimization recommendation from LLM (e.g. class functions)
# for cls_fxn_name in python_class_results:
#     cls_name, fxn_name = cls_fxn_name.split('.')
#     source_code = get_function_code(current_globals, class_name = cls_name, function_name = fxn_name)
#     # Get large language model optimization recommendations
#     optimization_recs_json = get_llm_model_response(my_api_key, endpoint_url, my_system_prompt, source_code, my_model = meta_llama_31_70b_instruct_model)
#     print(f"\n{cls_name}.{fxn_name}():\n{source_code}\n")
#     print(optimization_recs_json)


def sanitize_code_recs_llm_response(response):
    """function to sanitize the LLM response for optimized code recommendations"""
    # find the second occurrence of "]" and chop off everything after it
    second_closing_bracket_index = response.find("]", response.find("]") + 1)
    # if we find the second "]", trim the string after it
    if second_closing_bracket_index != -1:
        response = response[:second_closing_bracket_index + 1]
    
    # allow only valid characters (alphanumeric, spaces, commas, square brackets, quotes)
    response = re.sub(r"[^a-zA-Z0-9\s\[\]\",']", "", response)
    # remove excess whitespace
    response = re.sub(r'\s+', ' ', response).strip()
    return response


def get_llm_model_response_udf(my_api_key, my_base_url, my_system_prompt, my_user_prompt, my_model):
    """function to get LLM model response, refactored to work as a user defined function (UDF)"""
    try:
        # Call the OpenAI API to get the response
        response = OpenAI(api_key = my_api_key, base_url = my_base_url).chat.completions.create(
            model = my_model,
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
        return json.loads(response.json())["choices"][0]["message"]["content"]
    except Exception as e:
        return [str(e)]


# wrap the function as a PySpark UDF for code recommendations and optimized code generation
spark_get_llm_code_recs_response = udf(lambda api_key, base_url, system_prompt, user_prompt, model: 
                             sanitize_code_recs_llm_response(get_llm_model_response_udf(api_key, base_url, system_prompt, user_prompt, model)), 
                             StringType())

spark_get_llm_opt_code_response = udf(lambda api_key, base_url, system_prompt, user_prompt, model: 
                             get_llm_model_response_udf(api_key, base_url, system_prompt, user_prompt, model),
                             StringType())

# sample Python code (as a string)
# source_code = '''
#     def is_prime(n):
#         """Check if a number is prime. """
#         if n <= 1:
#             return False
#         for i in range(2, int(n**0.5) + 1):
#             if n % i == 0:
#                 return False
#         return True
# '''

# create a Spark DataFrame with the source code
# data = [(source_code,)]
# df = spark.createDataFrame(data, ["source_code"])

# # large language model (LLM) connection and instruct parameters
# my_api_key = ""
# # Update the base URL to your own Databricks Serving Endpoint
# workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"
# llm_model_name = "databricks-dbrx-instruct"
# endpoint_url = f"{workspace_url}/serving-endpoints"

# # Call to the LLM model UDFs for optimization recommendations and optimized code
# df = df.withColumn("llm_opt_suggestions", spark_get_llm_code_recs_response(lit(my_api_key), lit(endpoint_url), lit(code_recs_prompt), df.source_code, lit(llm_model_name)))
# df = df.withColumn("llm_opt_code", spark_get_llm_opt_code_response(lit(my_api_key), lit(endpoint_url), lit(code_opt_prompt), df.source_code, lit(llm_model_name)))
# df.select("llm_opt_suggestions").show(1, truncate = False)
# df.select("llm_opt_code").show(1, truncate = False)

