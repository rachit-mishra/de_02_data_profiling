import requests, os, json,time
from config import API_ENDPOINT, API_KEY
import json

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
