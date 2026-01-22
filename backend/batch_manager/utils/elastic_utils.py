import requests
import logging
from pyspark.sql import Row
import polars as pl
import pandas as pd
from batch_manager.utils.hive_utils import hive_keyword_search_count
from batch_manager.utils.spark_utils import ensure_spark_df
def es_keyword_search_count(API_URL, keyword, search_columns, hive_payload, date=None, timeout=30):
    if not isinstance(search_columns, list) or not search_columns:
        return {"error": "search_columns must be a non-empty list"}

    payload = {
        column["field"]: str(column["value"]).strip()
        for column in search_columns
        if column.get("field") and column.get("value")
    }

    if not payload:
        return {"error": "No valid search parameters"}

    try:
        response = requests.post(API_URL, json=payload, timeout=timeout)
        response.raise_for_status()
        result = response.json()
        
        results = result.get("results", [])
        # Filter by date if provided
        if date:
            results = [
                r for r in results
                if r["_source"].get("transactiondate_partition") == date
            ]
        if len(results)>0:
            if len(results)<100000: # ---------------------------------------------------- IF results is under the limit (100,000)
                filtered_results=[];
                filtered_results.append({
                                "name": f"Results found for '{keyword}'",                        
                                "size": len(results),
                            })
                return {
                    "results": filtered_results,
                    "has_more": 0,
                    "offset": 0,
                    "limit": 0,
                    "message": f"{len(results)} results found"
                }
            else:# ---------------------------------------------------- IF results exceeds the limit (100,000) streach for hive scan
                print("Elastic search response exceeded")
                # Forward to hive
                # return hive_keyword_search_count(*hive_payload)            
                return {
                    "results": 0,
                    "has_more": 0,
                    "offset": 0,
                    "limit": 0,
                    "message": f"Result out of bound!"
                }
        else:
            return {
                "results": 0,
                "has_more": 0,
                "offset": 0,
                "limit": 0,
                "message": f"{len(results)} results found"
            }

    except requests.exceptions.Timeout:
        return {"error": "Request timed out"}
    except requests.exceptions.ConnectionError:
        return {"error": "API server not reachable"}
    except requests.exceptions.HTTPError:
        return {
            "error": "API returned an error",
            "status_code": response.status_code,
            "detail": response.text
        }

# def load_elastic_rows(API_URL, spark, keywords, search_columns, date=None, timeout=30):
#     if not isinstance(search_columns, list) or not search_columns:
#         print("empty search_columns")
#         return [], set()

#     dfs_list = []
#     all_columns = set()

#     for keyword in keywords:
#         payload = {
#             row["field"]: keyword
#             for row in search_columns
#             if row.get("field")
#         }
#         try:
#             print("fetching...")
#             print(payload)
#             response = requests.post(API_URL, json=payload, timeout=timeout)
#             response.raise_for_status()
#             result = response.json()

#             results = result.get("results", [])
#             if date:
#                 results = [
#                     r for r in results
#                     if r["_source"].get("transactiondate_partition") == date
#                 ]

#             if results:
#                 print("has results:", len(results))
#                 pandas_df = pd.DataFrame([r["_source"] for r in results])
#                 spark_df = ensure_spark_df(spark,pandas_df)
#                 dfs_list.append(spark_df)
#                 all_columns.update(pandas_df.columns)
#         except requests.exceptions.Timeout:
#             print(f"Request timed out for keyword: {keyword}")
#             continue
#         except requests.exceptions.ConnectionError:
#             print("API server not reachable")
#             continue
#         except requests.exceptions.HTTPError as e:
#             print(f"API returned an error for keyword {keyword}: {e}")
#             continue
#     print(len(dfs_list),len(all_columns))
#     return dfs_list, all_columns

def load_elastic_rows(API_URL, keywords, search_columns, fetch_columns, date=None, timeout=30):

    dfs_list = []
    all_columns = set(fetch_columns)

    for keyword in keywords:
        payload = {row["field"]: keyword for row in search_columns if row.get("field")}
        print("Fetching for keyword:", keyword)

        try:
            response = requests.post(API_URL, json=payload, timeout=timeout)
            response.raise_for_status()
            result = response.json()

            results = result.get("results", [])
            if not results:
                continue

            # Extract _source safely
            records = [r.get("_source", {}) for r in results]
            df = pd.DataFrame(records)

            # Normalize column names (CRITICAL)
            df.columns = [c.upper() for c in df.columns]

            # Ensure fetch_columns exist
            for col in fetch_columns:
                if col not in df.columns:
                    df[col] = None

            # Project only fetch_columns
            df = df[fetch_columns]

            # 🔥 DO NOT DROP EMPTY ROWS 🔥
            dfs_list.append(df)

        except Exception as e:
            print(f"Elastic error for keyword {keyword}:", e)

    return dfs_list, all_columns
