import os
import json
from batch_manager.config_defaults import get_default_session_config
from linkx_xvigilance.db import connect


def get_xvigilance_config():
    """
    Resolves storage, Elasticsearch endpoints, and query parameters
    from LinkX default configuration, environment variables, and live DB overrides.
    """
    defaults = get_default_session_config("xvigilance_system")

    elastic_base = os.getenv("LINKX_ELASTIC_API_BASE_URL") or defaults.get("elastic_api_base_url")
    if not elastic_base:
        host = os.getenv("LINKX_ACTIVE_STORAGE_ADDRESS", "172.27.23.43")
        port = os.getenv("LINKX_API_PORT", "5000")
        elastic_base = f"http://{host}:{port}"

    base_config = {
        "elastic_base_url": elastic_base.rstrip("/"),
        "search_endpoint": os.getenv(
            "LINKX_ES_STRICT_ENDPOINT",
            defaults.get("search_api_endpoint_es_strict", "api/search/uii"),
        ),
        "date_column": os.getenv("LINKX_DATE_COLUMN", defaults.get("date_column", "transactiondate")),
        "time_column": os.getenv("LINKX_TIME_COLUMN", "transactiontime"),
        "fetch_columns": defaults.get("fetch_columns", [
            "TRANSACTIONID",
            "BRANCHNAME",
            "TRANSACTIONDATE",
            "TRANSACTIONTIME",
            "TRANSACTIONTYPE",
            "AMOUNTINBIRR",
            "ACCOWNERNAME",
            "BUSINESSMOBILENO",
            "ACCOUNTNO",
            "BALANCEHELD",
            "BENFULLNAME",
            "BENACCOUNTNO",
            "BENTELNO",
        ]),
        "page_size": int(os.getenv("XVIGILANCE_PAGE_SIZE", "50000")),
        "request_timeout_seconds": int(os.getenv("XVIGILANCE_REQUEST_TIMEOUT", "60")),
        "auth_header": os.getenv("LINKX_ELASTIC_API_AUTHORIZATION"),
        "es_direct_base_url": os.getenv("LINKX_ES_DIRECT_BASE_URL"),
        "es_direct_index": os.getenv("LINKX_ES_DIRECT_INDEX", "mobile_banking_transactions"),
        "es_timestamp_column": os.getenv("LINKX_ES_TIMESTAMP_COLUMN", "CREATEDDATE"),
        "es_scroll_page_size": int(os.getenv("XVIGILANCE_SCROLL_PAGE_SIZE", "10000")),
    }

    # 1. Follow exact LinkX architecture: Check database for live frontend overrides
    try:
        with connect(application_name="xvigilance-config") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT config FROM session_configs WHERE session_id = %s AND window_id = ''",
                    ("xvigilance_system",)
                )
                row = cur.fetchone()
                if row and row[0]:
                    db_config = row[0]
                    # 2. Merge DB config over the base .env config (DB acts as source of truth)
                    for k, v in db_config.items():
                        base_config[k] = v
    except Exception as e:
        print(f"[xvigilance] Warning: failed to fetch DB config overrides: {e}", flush=True)

    # 3. Ensure critical types are respected even if DB sends a string
    try:
        base_config["es_scroll_page_size"] = int(base_config.get("es_scroll_page_size", 10000))
    except (ValueError, TypeError):
        base_config["es_scroll_page_size"] = 10000

    return base_config
