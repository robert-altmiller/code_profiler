def analyze_customer_orders():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    
    # create a Spark session
    spark = SparkSession.builder.appName("ProfilerExample").getOrCreate()
    
    # create a DataFrame for customers
    data_customers = [
        (1, "Alice", "New York"),
        (2, "Bob", "Los Angeles"),
        (3, "Charlie", "Chicago"),
        (4, "David", "Miami"),
        (5, "Eve", "Dallas")
    ]
    columns_customers = ["customer_id", "name", "city"]
    df_customers = spark.createDataFrame(data_customers, schema=columns_customers)

    # create a DataFrame for orders
    data_orders = [
        (101, 1, 250),
        (102, 2, 90),
        (103, 3, 300),
        (104, 1, 450),
        (105, 5, 200)
    ]
    columns_orders = ["order_id", "customer_id", "amount"]
    df_orders = spark.createDataFrame(data_orders, schema=columns_orders)

    # join the DataFrames on customer_id
    df_joined = df_customers.join(df_orders, "customer_id")

    # perform a transformation that causes tasks to be sent to executors
    # for example, calculate the total amount spent by each customer
    df_result = df_joined.groupBy("customer_id", "name").sum("amount")

    # show the result
    df_result.show()
    
    # stop the spark session
    spark.stop()
    
analyze_customer_orders()