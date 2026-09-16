import logging
import os

logger = logging.getLogger("linkx.config")


def _auto_load_dotenv():
    candidate_paths = [
        os.path.join(os.getcwd(), ".env"),
        "/opt/linkx-backend-api/.env",
        "/opt/linkx-worker/.env",
        "/opt/linkx-backend-update/.env",
        "/opt/linkx-api/.env",
        "/opt/Linkx_xmaintenance/.env",
        "/opt/Linkx_xmaintenance/src/.env",
        "/var/www/linkx-backend/.env",
    ]
    for env_path in candidate_paths:
        if os.path.isfile(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("'\"")
                        if k and k not in os.environ:
                            os.environ[k] = v
            except Exception:
                pass


_auto_load_dotenv()


def _normalize_search_columns(value):
    if isinstance(value, (list, tuple, set)):
        return [column for column in value if column]
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [col.strip() for col in value.split(",") if col.strip()]
    return [value]


def _env_list(name, default):
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return default
    return _normalize_search_columns(value)


def _fetch_global_entities():
    try:
        from batch_manager.utils.postgres_utils import get_postgres_connection
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config_data FROM global_entity_classification ORDER BY created_at DESC LIMIT 1")
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
    except Exception as e:
        logger.warning(f"Failed to fetch global entity classification: {e}")
    return {}

def update_global_entities(config_dict, actor):
    try:
        from batch_manager.utils.postgres_utils import get_postgres_connection
        import json
        
        # Check if they actually provided entity configurations to update
        entity_keys = ["trusted_entities", "risk_entities", "pep_entities", "sanction_entities"]
        has_entities = any(k in config_dict for k in entity_keys)
                
        if not has_entities:
            return
            
        global_entities = _fetch_global_entities()
        updated = False
        
        for k in entity_keys:
            if k in config_dict and config_dict[k] != global_entities.get(k):
                global_entities[k] = config_dict[k]
                updated = True
                
        if updated:
            actor_name = actor.get('username') or actor.get('id') or 'system'
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO global_entity_classification (config_data, updated_by)
                        VALUES (%s, %s)
                    """, (json.dumps(global_entities), str(actor_name)))
                conn.commit()
            logger.info(f"Global entity classification updated by {actor_name}")
            
    except Exception as e:
        logger.warning(f"Failed to update global entity classification: {e}")

def get_default_session_config(session_id):
    _auto_load_dotenv()
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

    global_entities = _fetch_global_entities()

    return {
        "session_id": session_id,
        "trusted_entities": global_entities.get("trusted_entities", []),
        "risk_entities": global_entities.get("risk_entities", []),
        "pep_entities": global_entities.get("pep_entities", []),
        "sanction_entities": global_entities.get("sanction_entities", []),
        "user_id": os.getenv("LINKX_DEFAULT_USER_ID", "Unknown"),
        "kafka_addresses": kafka_list,
        "active_kafka_adress": kafka_servers,
        "kafka_bootstrap_servers": kafka_servers,
        "kafka_topics": _env_list("LINKX_KAFKA_TOPICS", []),
        "active_kafka_topic": os.getenv("LINKX_ACTIVE_KAFKA_TOPIC", ""),
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
