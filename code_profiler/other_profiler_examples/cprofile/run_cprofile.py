# execution: 'python3 run_cprofile.py'

# library imports
from pyspark.sql import SparkSession
import cProfile, io, pstats

# create a Spark session
spark = SparkSession.builder.appName("ProfilerExample").getOrCreate()

# define a function to profile
def process_data():
    data = [("John", 25), ("Alice", 30), ("Bob", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    df.show()

# profile the function
def profile_run():
    pr = cProfile.Profile()
    pr.enable()
    process_data()
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats()
    print(s.getvalue())

# initiate profile run
profile_run()

# stop the local spark session
spark.stop()