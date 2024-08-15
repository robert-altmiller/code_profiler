# prerequisites: # need to 'pip install yappi'
# execution: 'python3 run_yappi.py'

# library imports
from pyspark.sql import SparkSession
import io, json, yappi

# create a Spark session
spark = SparkSession.builder.appName("YappiProfilerExample").getOrCreate()

# define a function to profile
def process_data():
    data = [("John", 25), ("Alice", 30), ("Bob", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    df.show()

# profile the function
def profile_run(save_format = "json"):
    """
    start yappi code profile run
    save_format = 'callgrind' or 'pstat' or 'csv' or  'json'
    """
    # start Yappi
    yappi.start()

    # run the function to profile
    process_data()

    # stop Yappi
    yappi.stop()

    # get a StringIO to capture profiling output
    s = io.StringIO()

    # write out the stats to StringIO
    yappi.get_func_stats().print_all(out=s)

    # get function statistics
    func_stats = yappi.get_func_stats()

    # print the func_stats locally
    if save_format == 'callgrind':
        func_stats.save('profile.callgrind', type='callgrind')
    elif save_format == 'pstat':
        func_stats.save('profile.pstat', type='pstat')
    elif save_format == "json":
        # serialize stats to JSON with descriptive field names
        json_data = [
            {
                'function_name': stat.name, # function name
                'num_calls': stat.ncall, # number of times the function was called.
                'exclusive_time': stat.tsub, # total time spent in this function alone, excluding time spent in calls to sub-functions
                'total_time': stat.ttot, # total time spent in this function and all sub-functions called (recursively).
                'average_time': stat.tavg, # average time spent in the function per call.
            } for stat in func_stats
        ]
        # save JSON data to a file
        with open('profile.json', 'w') as f:
            json.dump(json_data, f, indent = 4)
    else:
        print("Unknown format")


    # print a summary of the profiling results (optional)
    return func_stats

# initiate profile run
print("running yappi code profiler run and save as callgrind format....")
callgrind_stats = profile_run(save_format = "callgrind")
print("running yappi code profiler run and save as pstat format....")
pstat_stats = profile_run(save_format = "pstat")
print("running yappi code profiler run and save as json format....")
json_stats = profile_run(save_format = "json")
print(json_stats.print_all())

# stop the local spark session
spark.stop()
