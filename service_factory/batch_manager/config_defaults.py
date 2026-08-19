import logging
import os

logger = logging.getLogger("linkx.config")


def _env_list(name, default):
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return default
    return [item.strip() for item in str(value).split(",") if item.strip()]


def get_default_session_config(session_id):
    kafka_servers = os.getenv("LINKX_KAFKA_BOOTSTRAP_SERVERS", "").strip()
    kafka_list = _env_list("LINKX_KAFKA_BOOTSTRAP_SERVERS", [])
    if not kafka_servers:
        logger.warning("[CONFIG DIAGNOSTIC] 'LINKX_KAFKA_BOOTSTRAP_SERVERS' is not set in environment.")

    storage_addr = os.getenv("LINKX_ACTIVE_STORAGE_ADDRESS", "").strip()
    if not storage_addr:
        logger.warning("[CONFIG DIAGNOSTIC] 'LINKX_ACTIVE_STORAGE_ADDRESS' is not set in environment.")

    es_base_url = os.getenv(
        "LINKX_ELASTIC_API_BASE_URL",
        f"http://{storage_addr}:5000" if storage_addr else "",
    ).strip()

    neo4j_url = os.getenv("LINKX_ACTIVE_TOOL_PROTOCOL") or os.getenv("LINKX_NEO4J_URL", "")
    if not neo4j_url:
        logger.warning("[CONFIG DIAGNOSTIC] 'LINKX_ACTIVE_TOOL_PROTOCOL' / 'LINKX_NEO4J_URL' is not set in environment.")

    return {
        "session_id": session_id,
        "user_id": os.getenv("LINKX_DEFAULT_USER_ID", "Unknown"),
        "kafka_addresses": kafka_list,
        "active_kafka_adress": kafka_servers,
        "kafka_bootstrap_servers": kafka_servers,
        "kafka_risk_scoring_input_topic": os.getenv(
            "LINKX_KAFKA_RISK_SCORING_INPUT_TOPIC", "dev.scoring.score.calculated.v1"
        ),
        "kafka_risk_scoring_mapped_topic": os.getenv(
            "LINKX_KAFKA_RISK_SCORING_MAPPED_TOPIC", "dev.analysis.link.mapped.v1"
        ),
        "kafka_risk_scoring_flagged_topic": os.getenv(
            "LINKX_KAFKA_RISK_SCORING_FLAGGED_TOPIC", "dev.analysis.link.mapped.v1"
        ),
        "max_linked_entities": int(os.getenv("LINKX_RISK_SCORING_MAX_LINKED_ENTITIES", "50")),
        "REST APIs": [],
        "active_REST_API": "",
        "storage_addresses": _env_list("LINKX_STORAGE_ADDRESSES", [storage_addr] if storage_addr else []),
        "storage_path": os.getenv("LINKX_STORAGE_PATH", "user/bank/cleaned_partitioned"),
        "storage_databases": _env_list("LINKX_STORAGE_DATABASES", ["bankdb", "bank_db"]),
        "storage_tables": _env_list("LINKX_STORAGE_TABLES", ["individual_transactions", "entity_transactions"]),
        "active_storage_address": storage_addr,
        "active_storage_host": os.getenv("LINKX_ACTIVE_STORAGE_HOST", storage_addr),
        "active_storage_database": os.getenv("LINKX_ACTIVE_STORAGE_DATABASE", "bankdb"),
        "active_storage_tables": _env_list("LINKX_ACTIVE_STORAGE_TABLES", ["individual_transactions", "entity_transactions"]),
        "elastic_api_base_url": es_base_url,
        "elastic_api_authorization": os.getenv("LINKX_ELASTIC_API_AUTHORIZATION", ""),
        "hadoop_rcp_port": os.getenv("LINKX_HADOOP_RCP_PORT", "9870"),
        "hadoop_web_port": os.getenv("LINKX_HADOOP_WEB_PORT", ""),
        "spark_port": os.getenv("LINKX_SPARK_PORT", "4040"),
        "thrift_port": os.getenv("LINKX_THRIFT_PORT", "9083"),
        "hive_port": os.getenv("LINKX_HIVE_PORT", "10000"),
        "api_port": os.getenv("LINKX_API_PORT", "5000"),
        "search_api_endpoint_es_fuzzy": os.getenv("LINKX_ES_FUZZY_ENDPOINT", "api/search/individual"),
        "search_api_endpoint_es_strict": os.getenv("LINKX_ES_STRICT_ENDPOINT", "api/search/uii"),
        "search_api_endpoint_hive_fuzzy": os.getenv("LINKX_HIVE_FUZZY_ENDPOINT", "api/search/individual"),
        "search_api_endpoint_hive_strict": os.getenv("LINKX_HIVE_STRICT_ENDPOINT", "api/search/uii"),
        "search_columns_strict": _env_list(
            "LINKX_SEARCH_COLUMNS_STRICT",
            ["transactionid", "businessmobileno", "accountno", "benaccountno", "bentelno", "transactiondate", "transactiontime"],
        ),
        "search_columns_fuzzy": _env_list(
            "LINKX_SEARCH_COLUMNS_FUZZY",
            ["entity_name", "involver_name", "othername", "accownername", "benfullname", "branchname", "benbranchname", "city", "bencity", "country", "bencountry", "transactiontype", "amountinbirr", "balanceheld"],
        ),
        "fetch_columns": _env_list(
            "LINKX_FETCH_COLUMNS",
            ["TRANSACTIONID", "BRANCHNAME", "TRANSACTIONDATE", "TRANSACTIONTIME", "TRANSACTIONTYPE", "AMOUNTINBIRR", "ACCOWNERNAME", "BUSINESSMOBILENO", "ACCOUNTNO", "BALANCEHELD", "BENFULLNAME", "BENACCOUNTNO", "BENTELNO"],
        ),
        "date_column": os.getenv("LINKX_DATE_COLUMN", "transactiondate"),
        "default_source_col": os.getenv("LINKX_DEFAULT_SOURCE_COL", "accountno"),
        "default_target_col": os.getenv("LINKX_DEFAULT_TARGET_COL", "benaccountno"),
        "default_relationship": os.getenv("LINKX_DEFAULT_RELATIONSHIP", "TRANSACTS_TO"),
        "dataframes_limit": int(os.getenv("LINKX_DATAFRAMES_LIMIT", "1000000")),
        "tools": _env_list("LINKX_TOOLS", ["neo4j"]),
        "active_tool": os.getenv("LINKX_ACTIVE_TOOL", "neo4j"),
        "active_tool_protocol": neo4j_url,
        "active_tool_username": os.getenv("LINKX_ACTIVE_TOOL_USERNAME", "neo4j"),
        "active_tool_password": os.getenv("LINKX_ACTIVE_TOOL_PASSWORD", ""),
        "active_tool_database": os.getenv("LINKX_ACTIVE_TOOL_DATABASE", ""),
        "active_tool_tables": _env_list("LINKX_ACTIVE_TOOL_TABLES", []),
        "tool_protocol_port": os.getenv("LINKX_TOOL_PROTOCOL_PORT", "7687"),
        "tool_web_port": os.getenv("LINKX_TOOL_WEB_PORT", "7473"),
        "rule_names": ["bank transactions", "social media (tweeter)", "call data records"],
        "rule_file_names": ["bank_transactions_rules", "social_media_(tweeter)_rules", "call_data_records_rules"],
        "active_rule": ["bank transactions"],
        "automation": os.getenv("LINKX_AUTOMATION", "true"),
        "remote": os.getenv("LINKX_REMOTE", "false"),
    }
