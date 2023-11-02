from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    # If not, create a new one
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    # Print the configuration
    print(spark.sparkContext.getConf().getAll())

    # spark = (
    #     SparkSession
    #     .builder
    #     .appName('Test spark')
    #     .getOrCreate()
    # )
    # kwargs['context']['spark'] = spark
    # print(kwargs['spark'].sql('select 1'))
    return None
    # kwargs['spark']
    
