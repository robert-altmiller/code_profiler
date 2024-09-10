# Library Imports
from code_profiler.main import *


# Sample Python code (as a string)
source_code = '''
    def is_prime(n):
        """Check if a number is prime. """
        if n <= 1:
            return False
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0:
                return False
        return True
'''

# Create a Spark DataFrame with the source code
data = [(source_code,)]
df = spark.createDataFrame(data, ["source_code"])

# Large language model (LLM) connection and instruct parameters
my_api_key = "" # insert your api key here (e.g. Databricks PAT Token)
# Update the base URL to your own Databricks Serving Endpoint
workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"
llm_model_name = "databricks-dbrx-instruct"
endpoint_url = f"{workspace_url}/serving-endpoints"

# Call to the LLM model UDFs for optimization recommendations and optimized code
df = df.withColumn("llm_opt_suggestions", spark_get_llm_code_recs_udf(lit(my_api_key), lit(endpoint_url), lit(code_recs_prompt), df.source_code, lit(llm_model_name)))
df = df.withColumn("llm_opt_code", spark_get_llm_opt_code_udf(lit(my_api_key), lit(endpoint_url), lit(code_opt_prompt), df.source_code, lit(llm_model_name)))
df.select("llm_opt_suggestions").show(1, truncate = False)
df.select("llm_opt_code").show(1, truncate = False)
