import os
from batch_manager.config_defaults import get_default_session_config


def get_xvigilance_config():
    """
    Resolves default storage, Elasticsearch endpoints, and query parameters
    from LinkX default configuration and environment variables.
    """
    defaults = get_default_session_config("xvigilance_system")

    elastic_base = os.getenv("LINKX_ELASTIC_API_BASE_URL") or defaults.get("elastic_api_base_url")
    if not elastic_base:
        host = os.getenv("LINKX_ACTIVE_STORAGE_ADDRESS", "172.27.23.43")
        port = os.getenv("LINKX_API_PORT", "5000")
        elastic_base = f"http://{host}:{port}"

    return {
        "elastic_base_url": elastic_base.rstrip("/"),
        "search_endpoint": os.getenv(
            "LINKX_ES_FUZZY_ENDPOINT",
            defaults.get("search_api_endpoint_es_fuzzy", "api/search/individual"),
        ),
        "date_column": os.getenv("LINKX_DATE_COLUMN", defaults.get("date_column", "transactiondate")),
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
    }
