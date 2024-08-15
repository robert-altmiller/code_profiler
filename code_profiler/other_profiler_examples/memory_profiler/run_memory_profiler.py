# library imports
from pyspark.sql import SparkSession
from memory_profiler import profile

# create a Spark session
spark = SparkSession.builder.appName("ProfilerExample").getOrCreate()

# define a function to profile with memory_profiler and decorate it
@profile
def process_data():
    data = [("John", 25), ("Alice", 30), ("Bob", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    df.show()

# initiate process_data function
process_data()

# stop the local spark session
spark.stop()
