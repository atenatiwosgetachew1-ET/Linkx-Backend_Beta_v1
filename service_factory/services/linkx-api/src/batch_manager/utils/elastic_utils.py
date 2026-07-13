import requests
import os
import shutil
import logging
from urllib.parse import urlsplit
from pyspark.sql import Row
import polars as pl
import pandas as pd
from batch_manager.utils.hive_utils import hive_keyword_search
from batch_manager.utils.spark_utils import ensure_spark_df
from security.redaction import redact_value


def _safe_api_label(url):
    try:
        parsed = urlsplit(str(url or ""))
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    except Exception:
        return "<invalid-url>"

def _elastic_diagnostic_logs_enabled():
    value = os.getenv("LINKX_ELASTIC_DIAGNOSTIC_LOGS", os.getenv("LINKX_SEARCH_DIAGNOSTIC_LOGS", "false"))
    return str(value).lower() in {"1", "true", "yes", "on"}


def _log_es_request(label, api_url, payload):
    if not _elastic_diagnostic_logs_enabled():
        return
    safe_payload = dict(payload or {})
    for key in list(safe_payload.keys()):
        if str(key).lower() not in {"limit", "offset", "size", "from", "batch_size", "page_size", "scroll_size"}:
            safe_payload[key] = "***"
    print(f"{label}: api={_safe_api_label(api_url)} payload={redact_value(safe_payload)}", flush=True)


def _log_es_response(label, api_url, response, *, page_size=None, collected=None):
    if not _elastic_diagnostic_logs_enabled():
        return
    if not isinstance(response, dict):
        print(f"{label}: api={_safe_api_label(api_url)} response_type={type(response).__name__}", flush=True)
        return
    summary = {
        "keys": sorted(response.keys()),
        "has_results": bool(response.get("results")),
        "results_count": len(response.get("results") or []),
        "has_scroll_id": bool(response.get("scroll_id")),
        "total": response.get("total"),
        "page_size": page_size,
        "collected": collected,
    }
    print(f"{label}: api={_safe_api_label(api_url)} summary={summary}", flush=True)


def _elastic_request_headers(auth_header=None):
    header_value = str(auth_header or "").strip()
    if not header_value:
        return None
    return {"Authorization": header_value, "Accept": "application/json"}


def es_keyword_search(id, API_URL, keyword, search_column, strict_mood, date_column, date=None, fetch_columns=None, timeout=30, limit=None, offset=0, batch_size=None, auth_header=None):
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
                    _log_es_request("DF payload ES", API_URL, used_payload)
                    response = requests.post(API_URL, json=used_payload, headers=_elastic_request_headers(auth_header), timeout=timeout)
                    response.raise_for_status()
                    result = response.json()
                    _log_es_response("DF response ES", API_URL, result)
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
                        auth_header=auth_header,
                    )
            else:
                payload = {column: keyword}
                if not strict_mood:
                    try:
                        request_limit = int(limit) if limit is not None else 50
                    except (TypeError, ValueError):
                        request_limit = 50
                    request_limit = max(1, min(request_limit, 10000))
                    try:
                        request_offset = int(offset or 0)
                    except (TypeError, ValueError):
                        request_offset = 0
                    request_offset = max(0, request_offset)
                    payload.update({"limit": request_limit, "offset": request_offset, "size": request_limit, "from": request_offset})
                _log_es_request("DF payload ES", API_URL, payload)
                response = requests.post(API_URL, json=payload, headers=_elastic_request_headers(auth_header), timeout=timeout)
                response.raise_for_status()
                result = response.json()
                _log_es_response("DF response ES", API_URL, result)
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
            print("Elastic result not found", {"api": _safe_api_label(API_URL), "timeout": timeout, "result_keys": list(result.keys()) if isinstance(result, dict) else type(result).__name__}, flush=True)
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
        print("Elastic error:", type(e).__name__, {"status_code": status_code, "api": _safe_api_label(API_URL)}, flush=True)
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
        print("Elastic error:", type(e).__name__, {"api": _safe_api_label(API_URL)}, flush=True)
        return None


def _extract_results(result):
    if not isinstance(result, dict):
        return []
    candidate_results = result.get("results")
    if candidate_results is None:
        candidate_results = result.get("hits", {}).get("hits", [])
    return candidate_results or []


def _fetch_es_pages(API_URL, column, keyword, limit=None, offset=0, batch_size=None, timeout=30, auth_header=None):
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
        _log_es_request("DF payload ES", API_URL, payload)
        response = requests.post(API_URL, json=payload, headers=_elastic_request_headers(auth_header), timeout=timeout)
        response.raise_for_status()
        result = response.json()
        _log_es_response("DF response ES page", API_URL, result, page_size=request_limit, collected=len(collected))
        page = _extract_results(result)
        last_payload = payload
        scroll_id = result.get("scroll_id") if isinstance(result, dict) else None

        has_more = bool(result.get("has_more")) if isinstance(result, dict) else False
        total = result.get("total") if isinstance(result, dict) else None
        if not page:
            if scroll_id and scroll_id != last_payload.get("scroll_id"):
                continue
            break
        collected.extend(page)

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


def load_elastic_rows(API_URL, keyword, search_column, fetch_columns, date=None, timeout=30, auth_header=None):
    print("load_elastic_rows", {"api": _safe_api_label(API_URL), "search_column": search_column, "fetch_columns_count": len(fetch_columns or []), "timeout": timeout}, flush=True)
    if not search_column:
        return {"error": "search_column must be provided"}

    # Build payload (STRICT SEARCH)
    payload = {search_column: keyword}

    if not payload:
        return {"error": "No valid search parameters"}

    try:
        response = requests.post(API_URL, json=payload, headers=_elastic_request_headers(auth_header), timeout=timeout)
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
        print("error:", "API returned an error", {"api": _safe_api_label(API_URL), "status_code": response.status_code}, flush=True)
        return None


def es_keyword_search_spark_chunks(
    API_URL,
    keyword,
    search_column,
    strict_mood,
    date_column,
    spark,
    chunk_dir,
    date=None,
    fetch_columns=None,
    timeout=30,
    limit=None,
    offset=0,
    batch_size=None,
    auth_header=None,
):
    """Fetch Elastic rows into backend-owned parquet chunks and return a Spark DataFrame.

    This is intentionally separate from es_keyword_search so existing pure-Elastic
    pandas flows keep their behavior. It is used for mixed HDFS/Spark + Elastic
    dataframe creation to avoid building one huge pandas DataFrame on the driver.
    """
    if not search_column or spark is None or not chunk_dir:
        return None

    print(
        "Elastic chunk fetch start:",
        {
            "api": _safe_api_label(API_URL),
            "search_column": search_column,
            "strict": bool(strict_mood),
            "date": date,
            "limit": limit,
            "offset": offset,
            "batch_size": batch_size,
            "fetch_columns_count": len(fetch_columns or []),
        },
        flush=True,
    )

    if isinstance(search_column, (list, tuple, set)):
        search_columns = [col for col in search_column if col]
    else:
        search_columns = [search_column]

    try:
        if os.path.exists(chunk_dir):
            shutil.rmtree(chunk_dir)
        os.makedirs(chunk_dir, exist_ok=True)
    except Exception as exc:
        print("Elastic chunk directory error:", exc)
        return None

    normalized_fetch = [c.lower() for c in (fetch_columns or [])]

    def page_to_dataframe(page):
        records = [_record_from_result(r) for r in page or []]
        records = [record for record in records if record]
        if not records:
            return None
        df = pd.DataFrame(records)
        df.columns = [c.lower() for c in df.columns]
        if date and date_column:
            date_key = str(date_column).lower()
            if date_key in df.columns:
                df = df[df[date_key].astype(str).str.startswith(str(date))]
        if normalized_fetch:
            existing = [c for c in normalized_fetch if c in df.columns]
            if not existing:
                print("No matching columns found for Elastic chunk page")
                print("DF columns:", df.columns.tolist())
                print("fetch_columns:", normalized_fetch)
                return None
            df = df[existing]
        if df.empty:
            return None
        for col_name in df.columns:
            df[col_name] = df[col_name].astype(str)
        return df

    page_spark_dfs = []

    def write_page(page, mode):
        df = page_to_dataframe(page)
        if df is None:
            return 0
        sdf = spark.createDataFrame(df)
        page_spark_dfs.append(sdf)
        return len(df)

    total_written = 0
    last_result = None
    for column in search_columns:
        try:
            if strict_mood:
                payload = {column: keyword}
                _log_es_request("DF payload ES chunk", API_URL, payload)
                response = requests.post(API_URL, json=payload, headers=_elastic_request_headers(auth_header), timeout=timeout)
                response.raise_for_status()
                last_result = response.json()
                written = write_page(_extract_results(last_result), "overwrite")
                if written:
                    total_written += written
                    break
                continue

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

            scroll_id = None
            mode = "overwrite"
            while total_written < total_limit:
                request_limit = min(page_size, total_limit - total_written)
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
                _log_es_request("DF payload ES chunk", API_URL, payload)
                response = requests.post(API_URL, json=payload, headers=_elastic_request_headers(auth_header), timeout=timeout)
                response.raise_for_status()
                last_result = response.json()
                _log_es_response("DF response ES chunk", API_URL, last_result, page_size=request_limit, collected=total_written)
                page = _extract_results(last_result)
                scroll_id = last_result.get("scroll_id") if isinstance(last_result, dict) else None
                has_more = bool(last_result.get("has_more")) if isinstance(last_result, dict) else False
                total = last_result.get("total") if isinstance(last_result, dict) else None
                if not page:
                    if scroll_id:
                        continue
                    break
                written = write_page(page, mode)
                if written:
                    total_written += written
                    mode = "append"
                current_offset += len(page)
                if len(page) < request_limit and not has_more and not scroll_id:
                    break
                if total is not None:
                    try:
                        if current_offset >= int(total):
                            break
                    except (TypeError, ValueError):
                        pass
            if total_written:
                break
        except requests.exceptions.HTTPError as exc:
            print("Elastic chunk fetch HTTP error:", type(exc).__name__, {"api": _safe_api_label(API_URL), "status_code": exc.response.status_code if exc.response is not None else None}, flush=True)
        except Exception as exc:
            print("Elastic chunk fetch error:", exc)

    if total_written <= 0 or not page_spark_dfs:
        print("Elastic chunk fetch produced no rows", {"api": _safe_api_label(API_URL), "search_column": search_column, "result_keys": list(last_result.keys()) if isinstance(last_result, dict) else type(last_result).__name__}, flush=True)
        return None

    try:
        local_chunk_dir = os.path.abspath(chunk_dir)
        os.makedirs(local_chunk_dir, exist_ok=True)
        spark_chunk_uri = "file:///" + local_chunk_dir.replace("\\", "/")
        merged = page_spark_dfs[0]
        for page_df in page_spark_dfs[1:]:
            merged = merged.unionByName(page_df, allowMissingColumns=True)
        merged.write.mode("overwrite").parquet(spark_chunk_uri)
        print(f"Elastic chunk fetch wrote {total_written} rows to {local_chunk_dir}")
        return spark.read.parquet(spark_chunk_uri)
    except Exception as exc:
        print("Elastic chunk final write/read error:", exc)
        return None
