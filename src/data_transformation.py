from pyspark.sql import DataFrame

import requests, os, json,time
from config import API_ENDPOINT, API_KEY
import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, FloatType

API_ENDPOINT = "https://airlabs.co/api/v9/flights"
API_KEY = "853e8713-34fb-407d-a0a1-93f2006557f3"


# def fetch_data_from_api(params=None):
#     if params is None:
#         params = {}
#     params['api_key'] = API_KEY
#     response = requests.get(API_ENDPOINT, params=params)
#     data = response.json()['response']

#     # Ensure the data is a list of dictionaries
#     if not isinstance(data, list) or (data and not isinstance(data[0], dict)):
#         raise TypeError("Unexpected data format from the API.")
#     return data

# import os
# import json
# import time

CACHE_FILE = 'api_cache.json'
CACHE_DURATION = 3600  # 1 hour in seconds

def fetch_data_from_api(params = None):
    # Check if cache file exists
    # Check if cache file exists and has content
    if os.path.exists(CACHE_FILE) and os.path.getsize(CACHE_FILE) > 0:
        # Get the timestamp of the last modification of the file
        file_mod_time = os.path.getmtime(CACHE_FILE)

        # If the file was modified within the cache duration, read from the file
        if (time.time() - file_mod_time) < CACHE_DURATION:
            with open(CACHE_FILE, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    # Handle corrupted cache file. For simplicity, delete it.
                    os.remove(CACHE_FILE)
    # If cache is not available or outdated, make the API call
    if params is None:
        params = {}
    params['api_key'] = API_KEY
    response = requests.get(API_ENDPOINT, params=params)
    data = response.json()['response']

    # Ensure the data is a list of dictionaries
    if not isinstance(data, list) or (data and not isinstance(data[0], dict)):
        raise TypeError("Unexpected data format from the API.")
    
    # Cache the response for future use
    with open(CACHE_FILE, 'w') as f:
        json.dump(data, f)

    return data

def transform_data(df: DataFrame) -> DataFrame:
    # Your transformations here. For now, let's assume a simple cleanup.
    df_clean = df.dropna(subset=["flight_icao", 
    "flight_iata", "airline_icao", "airline_iata"])
    return df_clean

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
    raw_data = fetch_data_from_api(params=None)
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

