import requests
import logging
from pyspark.sql import Row
import polars as pl
import pandas as pd
from batch_manager.utils.hive_utils import hive_keyword_search
from batch_manager.utils.spark_utils import ensure_spark_df

def es_keyword_search(id, API_URL, keyword, search_column, strict_mood, date_column, date=None, fetch_columns=None, timeout=30, limit=None, offset=0, batch_size=None):
    if not search_column:
        print(-2, "search_column1:", search_column)
        return None

    if isinstance(search_column, (list, tuple, set)):
        search_columns = [col for col in search_column if col]
    else:
        search_columns = [search_column]

    try:
        result = None
        results = None
        used_column = None
        used_payload = None
        for column in search_columns:
            if id == "fetch":
                if strict_mood:
                    used_payload = {column: keyword}
                    print("DF payload ES:", used_payload)
                    response = requests.post(API_URL, json=used_payload, timeout=timeout)
                    response.raise_for_status()
                    result = response.json()
                    candidate_results = _extract_results(result)
                else:
                    candidate_results, used_payload, result = _fetch_es_pages(
                        API_URL,
                        column,
                        keyword,
                        limit=limit,
                        offset=offset,
                        batch_size=batch_size,
                        timeout=timeout,
                    )
            else:
                payload = {column: keyword}
                if not strict_mood:
                    try:
                        request_limit = int(limit) if limit is not None else 50
                    except (TypeError, ValueError):
                        request_limit = 50
                    try:
                        request_offset = int(offset or 0)
                    except (TypeError, ValueError):
                        request_offset = 0
                    payload.update({"limit": request_limit, "offset": request_offset, "size": request_limit, "from": request_offset})
                print("DF payload ES:", payload)
                response = requests.post(API_URL, json=payload, timeout=timeout)
                response.raise_for_status()
                result = response.json()
                candidate_results = _extract_results(result)
                used_payload = payload

            if candidate_results:
                results = candidate_results
                used_column = column
                break
            if used_column is None:
                used_column = column
                results = candidate_results

        search_column = used_column
        payload = used_payload

        if date and date_column:
            results = [
                r for r in (results or [])
                if _row_value(r, date_column).startswith(str(date))
            ]

        if not results:
            print("result not found1:", API_URL, payload, timeout, result, results)
            return None

        if len(results) >= 100000:
            print("Elastic result overflow -> require hive fallback")
            if id == "search":
                return {
                    "results": [{
                        "name": f"Results found for '{keyword}'",
                        "keyword": keyword,
                        "size": len(results),
                        "strict": strict_mood,
                        "type": "hive",
                        "column": search_column,
                    }],
                    "has_more": 1,
                    "offset": 0,
                    "limit": 0,
                    "message": f"{len(results)}+ results found; use Hive fetch",
                }
            # Fetch mode may legitimately collect large batches via scroll/pages.

        if id == "search":
            filtered_results = [{
                "name": f"Results found for '{keyword}'",
                "keyword": keyword,
                "size": len(results),
                "strict": strict_mood,
                "type": "elastic",
                "column": search_column,
            }]
            return {
                "results": filtered_results,
                "has_more": 0,
                "offset": 0,
                "limit": 0,
                "message": f"{len(results)} results found"
            }

        if id == "fetch":
            print("fetching...")
            records = [_record_from_result(r) for r in results]
            records = [record for record in records if record]
            if not records:
                print("Elastic fetch returned no row dictionaries")
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

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else None
        response_text = e.response.text[:500] if e.response is not None else ""
        print("Elastic error:", str(e), response_text)
        if id == "search" and not strict_mood and status_code and status_code >= 500:
            column = search_columns[0] if search_columns else search_column
            return {
                "results": [{
                    "name": f"Large fuzzy results found for '{keyword}'",
                    "keyword": keyword,
                    "size": 100000,
                    "strict": strict_mood,
                    "type": "hive",
                    "column": column,
                }],
                "has_more": 1,
                "offset": 0,
                "limit": 0,
                "message": "Elastic fuzzy search was too broad; use Hive fetch",
            }
        return None
    except Exception as e:
        print("Elastic error:", str(e))
        return None


def _extract_results(result):
    if not isinstance(result, dict):
        return []
    candidate_results = result.get("results")
    if candidate_results is None:
        candidate_results = result.get("hits", {}).get("hits", [])
    return candidate_results or []


def _fetch_es_pages(API_URL, column, keyword, limit=None, offset=0, batch_size=None, timeout=30):
    try:
        total_limit = int(limit) if limit is not None else 100000
    except (TypeError, ValueError):
        total_limit = 100000
    try:
        page_size = int(batch_size) if batch_size is not None else 10000
    except (TypeError, ValueError):
        page_size = 10000
    page_size = max(1, min(page_size, 10000))
    try:
        current_offset = int(offset or 0)
    except (TypeError, ValueError):
        current_offset = 0

    collected = []
    result = None
    last_payload = None
    scroll_id = None

    while len(collected) < total_limit:
        request_limit = min(page_size, total_limit - len(collected))
        payload = {
            column: keyword,
            "limit": request_limit,
            "offset": current_offset,
            "size": request_limit,
            "from": current_offset,
            "batch_size": request_limit,
            "page_size": request_limit,
            "scroll_size": request_limit,
        }
        if scroll_id:
            payload["scroll_id"] = scroll_id
        print("DF payload ES:", payload)
        response = requests.post(API_URL, json=payload, timeout=timeout)
        response.raise_for_status()
        result = response.json()
        page = _extract_results(result)
        last_payload = payload
        scroll_id = result.get("scroll_id") if isinstance(result, dict) else None

        if not page:
            break
        collected.extend(page)

        has_more = bool(result.get("has_more")) if isinstance(result, dict) else False
        total = result.get("total") if isinstance(result, dict) else None
        current_offset += len(page)
        if len(page) < request_limit and not has_more and not scroll_id:
            break
        if total is not None:
            try:
                if current_offset >= int(total):
                    break
            except (TypeError, ValueError):
                pass

    print(f"Elastic fetch collected {len(collected)} rows")
    return collected, last_payload, result


def _record_from_result(row):
    if isinstance(row, dict) and "_source" in row:
        return row.get("_source") or {}
    if isinstance(row, dict):
        return row
    return {}


def _row_value(row, key):
    record = _record_from_result(row)
    return str(record.get(key, ""))


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