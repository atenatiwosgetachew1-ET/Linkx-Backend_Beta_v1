import requests
from datetime import datetime
from typing import Any, Dict, Generator, List


def stream_window_records(
    config: dict,
    window_start: datetime,
    window_end: datetime,
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Streams records using the direct Elasticsearch Scroll API
    for the exact [window_start, window_end] time slice.
    """
    es_base = config.get("es_direct_base_url")
    if not es_base:
        raise ValueError("Missing 'es_direct_base_url' in config. Cannot perform direct ES scroll.")

    es_index = config.get("es_direct_index", "mobile_banking_transactions")
    time_col = config.get("es_timestamp_column", "CREATEDDATE")
    page_size = config.get("es_scroll_page_size", 10000)
    timeout = config.get("request_timeout_seconds", 60)
    
    url = f"{es_base.rstrip('/')}/{es_index}/_search?scroll=5m"
    
    # Elasticsearch timestamp filtering (milliseconds)
    start_ms = int(window_start.timestamp() * 1000)
    end_ms = int(window_end.timestamp() * 1000)
    
    query = {
        "size": page_size,
        "query": {
            "range": {
                time_col: {
                    "gte": start_ms,
                    "lt": end_ms
                }
            }
        }
    }
    
    auth_header = config.get("auth_header")
    headers = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header

    try:
        response = requests.post(url, json=query, headers=headers, timeout=timeout)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        raise RuntimeError(f"Failed to initiate Elasticsearch scroll at {url}: {exc}") from exc

    scroll_id = data.get('_scroll_id')
    hits = data.get('hits', {}).get('hits', [])
    
    try:
        while hits:
            # Yield the current batch of records (extracting '_source')
            records = [hit.get('_source', {}) for hit in hits]
            if records:
                yield records
            
            # Fetch next batch
            scroll_url = f"{es_base.rstrip('/')}/_search/scroll"
            scroll_payload = {
                "scroll": "5m",
                "scroll_id": scroll_id
            }
            resp = requests.post(scroll_url, json=scroll_payload, headers=headers, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            scroll_id = data.get('_scroll_id')
            hits = data.get('hits', {}).get('hits', [])
            
    finally:
        # Always clean up the scroll context
        if scroll_id:
            try:
                del_url = f"{es_base.rstrip('/')}/_search/scroll"
                requests.delete(del_url, json={"scroll_id": [scroll_id]}, headers=headers, timeout=5)
            except Exception as e:
                print(f"[xvigilance] Warning: failed to clear scroll context: {e}")
