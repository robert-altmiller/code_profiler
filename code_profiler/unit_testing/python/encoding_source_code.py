from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import zlib
import base64

# Initialize Spark session
spark = SparkSession.builder.appName("CompressionExample").getOrCreate()

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

# UDF to compress the source code
def compress_code(source_code):
    compressed_code = zlib.compress(source_code.encode('utf-8'))
    return base64.b64encode(compressed_code).decode('utf-8')

# UDF to decompress the source code
def decompress_code(compressed_code):
    decompressed_code = zlib.decompress(base64.b64decode(compressed_code.encode('utf-8')))
    return decompressed_code.decode('utf-8')

# Register UDFs
compress_code_udf = udf(compress_code, StringType())
decompress_code_udf = udf(decompress_code, StringType())

# Apply compression and decompression
df_compressed = df.withColumn("compressed_code", compress_code_udf(df.source_code))
df_compressed.show(truncate=True)
df_decompressed = df_compressed.withColumn("decompressed_code", decompress_code_udf(df_compressed.compressed_code))
df_decompressed.show(truncate=True)