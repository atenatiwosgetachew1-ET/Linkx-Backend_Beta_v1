import requests
import pandas as pd
from pyspark.sql import SparkSession
from copy import deepcopy

def get_spark_session():
    return SparkSession.builder.appName("API Loader").getOrCreate()


def clean_record(record, exclude_keys=None):
    """
    Recursively clean a record (dict) by:
    - Removing keys in exclude_keys
    - Renaming 'id' -> 'entity_id'
    """
    exclude_keys = exclude_keys or []
    cleaned = {}
    
    for key, value in record.items():
        # Skip unwanted keys
        if key in exclude_keys:
            continue

        # Rename 'id' to 'entity_id'
        if key == "id":
            cleaned["entity_id"] = value
        # Recurse for nested dicts
        elif isinstance(value, dict):
            cleaned[key] = clean_record(value, exclude_keys)
        # Recurse for list of dicts
        elif isinstance(value, list):
            new_list = []
            for item in value:
                if isinstance(item, dict):
                    new_list.append(clean_record(item, exclude_keys))
                else:
                    new_list.append(item)
            cleaned[key] = new_list
        else:
            cleaned[key] = value

    return cleaned


def load_api(url, session_id=None, params=None, headers=None,
             use_spark=False, items_key="items", exclude_keys=None):
    """
    Load data from an API, clean unnecessary keys and rename 'id' to 'entity_id'.
    """
    print("Now this is API loader")

    headers = headers or {"User-Agent": "Mozilla/5.0"}
    params = params or {}
    exclude_keys = exclude_keys or ["remarks", "notes", "description", "comments", "showLetter", "portraitURL", "color", "displayStatus", "topTag", "topTagTip",
    "newUserTag", "newUserTagTip","legal", "retry", "msg", "code", "additionalAd", "tradeLimitDialogType", "self", "tradeLimitTip"]
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print("API request failed:", e)
        return None

    # Extract list of records (default: "items")
    records = data.get(items_key, data)
    
    # Clean each record
    cleaned_records = [clean_record(deepcopy(rec), exclude_keys) for rec in records]

    if use_spark:
        spark = get_spark_session()
        print("Returning Spark DataFrame")
        return spark.createDataFrame(cleaned_records)

    print("Returning Pandas DataFrame")
    return pd.DataFrame(cleaned_records)