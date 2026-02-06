import requests
import logging
from pyspark.sql import Row
import polars as pl
import pandas as pd
from batch_manager.utils.hive_utils import hive_keyword_search
from batch_manager.utils.spark_utils import ensure_spark_df

def es_keyword_search(id, API_URL, keyword, search_column, strict_mood, date_column, date=None, fetch_columns=None, timeout=30):
    if not search_column:
        print(-2,"search_column1:",search_column)
        return None

    payload = {search_column: keyword}

    try:
        print("DF payload:",payload)
        response = requests.post(API_URL, json=payload, timeout=timeout)
        response.raise_for_status()
        result = response.json()

        results = result.get("results")
        if results is None:
            print("result not found")
            results = result.get("hits", {}).get("hits", [])

        # Date filter        
        if date and date_column:
            results = [
                r for r in results
                if r.get("_source", {}).get(date_column, "").startswith(date)
            ]


        if not results:
            print("result not found1:",API_URL,payload,timeout,response,result,results)
            return None

        if len(results) >= 100000:
            print("Elastic result overflow → require hive fallback")
            return None

        if id == "search":
            filtered_results=[]
            filtered_results.append({
                "name": f"Results found for '{keyword}'",
                "keyword": keyword,                        
                "size": len(results),
                "strict": strict_mood,
                "type": "elastic",
                "column": search_column,
            })
            return {
                "results": filtered_results,
                "has_more": 0,
                "offset": 0,
                "limit": 0,
                "message": f"{len(results)} results found"
            }
                    
        if id == "fetch":
            print("fetching...")
            records = [r.get("_source", {}) for r in results if "_source" in r]
            if not records:
                return None

            df = pd.DataFrame(records)
            df.columns = [c.lower() for c in df.columns]

            if fetch_columns:
                normalized_fetch = [c.lower() for c in fetch_columns]
                existing = [c for c in normalized_fetch if c in df.columns]

                if not existing:
                    print("No matching columns found")
                    print("DF columns:", df.columns.tolist())
                    print("fetch_columns:", normalized_fetch)
                    return None

                df = df[existing]
            return df

        return None

    except Exception as e:
        print("Elastic error:", str(e))
        return None


def load_elastic_rows(API_URL, keyword, search_column, fetch_columns, date=None, timeout=30):
    print(API_URL, keyword, search_column, fetch_columns, date=None, timeout=30)
    if not search_column:
        return {"error": "search_column must be provided"}

    # Build payload (STRICT SEARCH)
    payload = {search_column: keyword}

    if not payload:
        return {"error": "No valid search parameters"}

    try:
        response = requests.post(API_URL, json=payload, timeout=timeout)
        response.raise_for_status()
        result = response.json()

        results = result.get("results", [])
        if not results:
            return {
                "rows": [],
                "count": 0,
                "message": "No results found"
            }

        # Extract _source safely
        records = [r.get("_source", {}) for r in results if "_source" in r]
        if not records:
            return {
                "rows": [],
                "count": 0,
                "message": "No _source data"
            }

        # Convert to DataFrame
        df = pd.DataFrame(records)

        # Normalize column names
        df.columns = [c.upper() for c in df.columns]

        # Optional date filter
        if date and "TRANSACTIONDATE_PARTITION" in df.columns:
            df = df[df["TRANSACTIONDATE_PARTITION"] == date]

        # Ensure fetch_columns exist
        for col in fetch_columns:
            if col not in df.columns:
                df[col] = None

        # Project only fetch_columns
        df = df[fetch_columns]

        return {
            "rows": df.to_dict(orient="records"),
            "count": len(df),
            "columns": fetch_columns,
            "message": f"{len(df)} rows found"
        }

    except requests.exceptions.Timeout:
        print("error:","Request timed out")
        return None
    except requests.exceptions.ConnectionError:
        print("error:","API server not reachable")
        return None
    except requests.exceptions.HTTPError:
        print("error:", "API returned an error",response.text)
        return None