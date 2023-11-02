from pyspark.sql import SparkSession

@data_loader
def load_data(*args, **kwargs):
    # If SparkSession is active, get or create will return the active session
    print(kwargs)
    # spark = kwargs['spark']
    # # print(spark.sql('select 1'))

    # df = spark.createDataFrame([(1, "John"), (2, "Mike"), (3, "Sara")], ["ID", "Name"])
    # print(df.show())

    # # Register the DataFrame as a temporary view
    # df.createOrReplaceTempView("names")

    # # Use Spark SQL
    # result = spark.sql("SELECT * FROM names WHERE ID > 1")
    # print(result.show())
    # # print(kwargs['spark'])
    # # ... rest of your code
