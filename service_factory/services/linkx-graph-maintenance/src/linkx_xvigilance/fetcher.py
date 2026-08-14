import requests
from datetime import datetime
from typing import Any, Dict, Generator, List


def stream_window_records(
    config: dict,
    window_start: datetime,
    window_end: datetime,
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Streams records from the default Elasticsearch endpoint for the [window_start, window_end] slice.
    Uses paging with page_size to ensure constant bounded memory footprint.
    """
    url = f"{config['elastic_base_url']}/{config['search_endpoint'].lstrip('/')}"
    page_size = config.get("page_size", 50000)
    timeout = config.get("request_timeout_seconds", 60)

    date_str = window_start.strftime("%Y-%m-%d")
    start_time_str = window_start.strftime("%H:%M:%S")
    end_time_str = window_end.strftime("%H:%M:%S")

    offset = 0

    while True:
        payload = {
            config["date_column"]: date_str,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "fetch_columns": config["fetch_columns"],
            "limit": page_size,
            "offset": offset,
        }

        try:
            response = requests.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            # If the endpoint is not reachable or returns error, log and raise
            raise RuntimeError(f"Failed querying Elasticsearch at {url}: {exc}") from exc

        # Extract returned records
        results = None
        if isinstance(data, dict):
            results = data.get("results") or data.get("data") or []
        elif isinstance(data, list):
            results = data

        if not results:
            break

        yield results

        if len(results) < page_size:
            break

        offset += len(results)
