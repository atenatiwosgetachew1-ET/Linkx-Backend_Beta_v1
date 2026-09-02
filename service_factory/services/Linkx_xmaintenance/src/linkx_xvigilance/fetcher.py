import requests
from datetime import datetime
from typing import Any, Dict, Generator, List


def _get_row_value(row: dict, target_key: str):
    """Case-insensitive lookup for field in a row dict."""
    if not isinstance(row, dict):
        return None
    target_lower = target_key.lower()
    for k, v in row.items():
        if k.lower() == target_lower:
            return v
    return None


def stream_window_records(
    config: dict,
    window_start: datetime,
    window_end: datetime,
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Streams records from the strict Elasticsearch endpoint (api/search/uii)
    for the [window_start, window_end] 1-hour time slice.
    """
    url = f"{config['elastic_base_url']}/{config['search_endpoint'].lstrip('/')}"
    page_size = config.get("page_size", 50000)
    timeout = config.get("request_timeout_seconds", 60)
    auth_header = config.get("auth_header")

    headers = {"Accept": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header

    date_str = window_start.strftime("%Y-%m-%d")
    start_time_str = window_start.strftime("%H:%M:%S")
    end_time_str = window_end.strftime("%H:%M:%S")

    # Strict search payload query by transactiondate
    payload = {
        config["date_column"]: date_str,
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
        
        # 404 is a standard empty result indicator from Elasticsearch API
        if response.status_code == 404:
            return

        response.raise_for_status()
        data = response.json()
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 404:
            return
        raise RuntimeError(f"Elasticsearch query failed [{response.status_code}] at {url}: {http_err}") from http_err
    except Exception as exc:
        raise RuntimeError(f"Failed querying Elasticsearch at {url}: {exc}") from exc

    # Extract returned raw records
    raw_records = []
    if isinstance(data, dict):
        raw_records = data.get("results") or data.get("data") or []
    elif isinstance(data, list):
        raw_records = data

    if not raw_records:
        return

    time_col = config.get("time_column", "transactiontime")

    # Filter records to the exact [start_time, end_time) hour window
    filtered_records = []
    for r in raw_records:
        row_time = _get_row_value(r, time_col)
        if row_time is not None:
            time_val = str(row_time).strip()
            # If time matches [start_time, end_time)
            if start_time_str <= time_val < end_time_str:
                filtered_records.append(r)
        else:
            # If no time column is present, keep the record
            filtered_records.append(r)

    # Yield in bounded pages of page_size
    for i in range(0, len(filtered_records), page_size):
        yield filtered_records[i : i + page_size]
