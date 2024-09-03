# library imports
import inspect, json
from openai import OpenAI



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
    

# Next we will configure the OpenAI SDK with Databricks Access Token and our base URL
def get_llm_model_response(my_api_key, my_base_url, my_system_prompt, my_user_prompt, my_model):
  # Now let's invoke inference against the PAYGO (Pay Per Token) endpoint
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

# how to use this Python code in each of the unit_testing notebooks:

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