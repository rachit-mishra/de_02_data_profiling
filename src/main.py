from pyspark.sql import SparkSession
from data_extraction import fetch_data_from_api
from data_transformation import transform_data
from data_quality import compute_quality_metrics
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, FloatType

# Define the expected schema
expected_schema = StructType([
    StructField("hex", StringType(), True),
    StructField("reg_number", StringType(), True),
    StructField("flag", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", DoubleType(), True),
    StructField("dir", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("v_speed", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("flight_number", StringType(), True),
    StructField("flight_icao", StringType(), True),
    StructField("flight_iata", StringType(), True),
    StructField("dep_icao", StringType(), True),
    StructField("dep_iata", StringType(), True),
    StructField("arr_icao", StringType(), True),
    StructField("arr_iata", StringType(), True),
    StructField("airline_icao", StringType(), True),
    StructField("airline_iata", StringType(), True),
    StructField("aircraft_icao", StringType(), True),
    StructField("updated", LongType(), True),
    StructField("status", StringType(), True)
])

def standardize_data_type(record):
    """
    Standardizes the data type of each field in the record based on the expected schema.
    """
    standardized_record = {}
    for field in expected_schema:
        field_name = field.name
        field_type = type(field.dataType)
        
        if field_name in record:
            if field_type == StringType:
                standardized_record[field_name] = str(record[field_name])
            elif field_type == DoubleType:
                standardized_record[field_name] = float(record[field_name])
            elif field_type == LongType:
                standardized_record[field_name] = int(record[field_name])
            else:
                standardized_record[field_name] = record[field_name]
        else:
            standardized_record[field_name] = None

    return standardized_record


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("AviationDataPipeline") \
        .getOrCreate()

    # Extraction
    raw_data = fetch_data_from_api()
    standardized_data = [standardize_data_type(record) for record in raw_data]


    print("First 5 records:", raw_data[:5])
    raw_df = spark.createDataFrame(standardized_data, schema = expected_schema)


    # Transformation
    transformed_df = transform_data(raw_df)

    # Quality Metrics Generation
    quality_metrics = compute_quality_metrics(transformed_df)
    for column, null_percent in quality_metrics.items():
        print(f"Null Percentage in {column}: {null_percent}%")

    spark.stop()
