import json
import os
import time
from datetime import datetime, timezone

from batch_manager.analyzing.analyzer import analyzer, rule_to_node_label
from batch_manager.analyzing.LA_rules_script import TRANSACTION_RELATIONSHIPS
from batch_manager.config_defaults import get_default_session_config
from batch_manager.services.dataframe_workflow import create_dataframe_result
from batch_manager.utils.artifact_utils import ensure_artifact_dir
from batch_manager.utils.elastic_utils import es_keyword_search
from batch_manager.utils.neo4j_utils import create_neo4j_driver
from globals import create_file, load_temp_config, save_temp_config
from security.redaction import redact_value

DEFAULT_INPUT_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_INPUT_TOPIC", "dev.scoring.score.calculated.v1"
)
DEFAULT_MAPPED_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_MAPPED_TOPIC", "dev.analysis.link.mapped.v1"
)
DEFAULT_FLAGGED_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_FLAGGED_TOPIC", "dev.analysis.link.mapped.v1"
)


def _load_env_file_value(key, default=None):
    val = os.getenv(key)
    if val is not None and str(val).strip():
        return str(val).strip()

    candidate_paths = [
        os.path.join(os.getcwd(), ".env"),
        "/opt/linkx-worker/.env",
        "/opt/linkx-backend-api/.env",
        "/opt/linkx-backend-update/.env",
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
                        if k.strip() == key:
                            v = v.strip().strip("'\"")
                            if v:
                                return v
            except Exception:
                pass
    return default


DEFAULT_KAFKA_BROKERS = _load_env_file_value("LINKX_KAFKA_BOOTSTRAP_SERVERS", "")


def get_max_linked_entities_setting(session_id=""):
    raw = _config_value(session_id, "max_linked_entities") or _load_env_file_value("LINKX_RISK_SCORING_MAX_LINKED_ENTITIES", "50")
    try:
        return int(raw)
    except (ValueError, TypeError):
        return 50


def _iso8601_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _bank_source_target_relationship(session_id, custom_source=None, custom_target=None, custom_rel=None):
    source = custom_source or _config_value(session_id, "default_source_col") or "accountno"
    target = custom_target or _config_value(session_id, "default_target_col") or "benaccountno"
    relationship = custom_rel or _config_value(session_id, "default_relationship") or "TRANSACTS_TO"
    return {
        "source": source,
        "target": target,
        "relationship": relationship,
    }


def _prepare_session(session_id):
    # 1. Register session in PostgreSQL to satisfy foreign key constraints for session_configs
    try:
        from batch_manager.utils.postgres_utils import get_postgres_connection
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO analysis_sessions (session_id, status, created_at, last_seen_at)
                VALUES (%s, 'active', NOW(), NOW())
                ON CONFLICT (session_id) DO NOTHING
                """, (session_id,))
            conn.commit()
    except Exception as e:
        print(f"[RiskScoring] Warning: Could not register session in postgres: {e}")

    # 2. Setup legacy config file
    configs = load_temp_config("data", session_id)
    if configs:
        return True
    return create_file(
        "public/temp_config/",
        f"{session_id}_temp_config",
        "json",
        get_default_session_config(session_id),
    )


def _config_value(session_id, key):
    value = load_temp_config(key, session_id)
    if value not in (None, ""):
        return value
    return get_default_session_config(session_id).get(key)


def _kafka_brokers(session_id=""):
    return _config_value(session_id, "active_kafka_adress") or _config_value(session_id, "kafka_bootstrap_servers") or DEFAULT_KAFKA_BROKERS


def _kafka_mapped_topic(session_id=""):
    return _config_value(session_id, "kafka_risk_scoring_mapped_topic") or DEFAULT_MAPPED_TOPIC


def _kafka_flagged_topic(session_id=""):
    return _config_value(session_id, "kafka_risk_scoring_flagged_topic") or DEFAULT_FLAGGED_TOPIC


def _with_port(url, port):
    url = str(url or "")
    if port and "://" in url and not url.rsplit(":", 1)[-1].isdigit():
        return f"{url}:{port}"
    return url


def _neo4j_credentials(session_id):
    credentials = load_temp_config("tool_credentials", session_id)
    if isinstance(credentials, dict):
        return credentials

    url = os.getenv("LINKX_NEO4J_URL") or _config_value(session_id, "active_tool_protocol")
    port = os.getenv("LINKX_TOOL_PROTOCOL_PORT") or _config_value(session_id, "tool_protocol_port")
    credentials = {
        "url": _with_port(url, port),
        "username": os.getenv("LINKX_NEO4J_USERNAME") or _config_value(session_id, "active_tool_username"),
        "password": os.getenv("LINKX_NEO4J_PASSWORD") or _config_value(session_id, "active_tool_password"),
        "session_id": session_id,
    }
    save_temp_config("tool_credentials", credentials, session_id)
    save_temp_config("tool", _config_value(session_id, "active_tool") or "neo4j", session_id)
    return credentials


def _base_analyzer_payload(session_id, credentials):
    return {
        "id": "batch_data",
        "type": "new",
        "session_id": session_id,
        "run_id": session_id,
        "dataframe_dir": load_temp_config("active_dataframe_dir", session_id) or os.path.join(ensure_artifact_dir("dfparts"), f"merged_dfpart_{session_id}"),
        "dataframe_id": load_temp_config("active_dataframe_id", session_id),
        "expected_dataframe_rows": load_temp_config("active_dataframe_rows", session_id),
        "use_spark": bool(load_temp_config("active_dataframe_use_spark", session_id)),
        "spark_conf": {
            "storage_ip": _config_value(session_id, "active_storage_address"),
            "spark_port": _config_value(session_id, "spark_port"),
        },
        "tool": "neo4j",
        "tool_credentials": credentials,
        "log_file": f"{session_id}.log",
    }


def _run_analyzer(payload, step_name):
    print(f"Risk Scoring link analysis {step_name} analyzer payload:", redact_value(payload), flush=True)
    try:
        result = analyzer(payload)
        if result is not True:
            print(f"Risk Scoring link analysis {step_name} analyzer returned False silently. Payload dir: {payload.get('dataframe_dir')}")
        return result is True
    except Exception as exc:
        print(f"Risk Scoring link analysis {step_name} analyzer failed with exception:", exc)
        return False


def _ingest_dataframe_to_neo4j(session_id, custom_source=None, custom_target=None, custom_rel=None):
    rule = "bank transactions"
    credentials = _neo4j_credentials(session_id)
    if not credentials:
        print("Risk Scoring link analysis missing neo4j credentials")
        return False

    link_payload = _base_analyzer_payload(session_id, credentials)
    link_payload.update({"action": "Link Analysis", "rule": rule})
    if not _run_analyzer(link_payload, "link analysis"):
        return False

    source_target = _bank_source_target_relationship(
        session_id,
        custom_source=custom_source,
        custom_target=custom_target,
        custom_rel=custom_rel,
    )
    if source_target:
        relationship_payload = _base_analyzer_payload(session_id, credentials)
        relationship_payload.update({
            "action": "Source / Target Relationship",
            "source": source_target["source"],
            "target": source_target["target"],
            "relationship": source_target["relationship"],
        })
        if not _run_analyzer(relationship_payload, "source target relationship"):
            return False

    return True


def _analysis_summary(session_id):
    credentials = _neo4j_credentials(session_id)
    if not credentials:
        return None

    node_label = rule_to_node_label("bank transactions", session_id)
    safe_label = f"`{str(node_label).replace('`', '')}`"
    driver = create_neo4j_driver(credentials)
    try:
        with driver.session() as session:
            record = session.run(f"""
            MATCH (n:{safe_label})
            WHERE ($session_id = '' OR n.batch_id STARTS WITH $session_id OR n.session_id = $session_id)
            OPTIONAL MATCH (n)-[r]->(target:{safe_label})
            WHERE ($session_id = '' OR r.session_id = $session_id)
            WITH count(DISTINCT n) AS total_nodes,
                 count(DISTINCT CASE WHEN coalesce(n.is_flagged, false) = true THEN n END) AS flagged_nodes,
                 count(DISTINCT CASE WHEN coalesce(n.is_flagged, false) = false THEN n END) AS clean_nodes,
                 count(DISTINCT CASE WHEN coalesce(r.is_flagged, false) = true THEN r END) AS flagged_relationships,
                 count(DISTINCT r) AS all_relationships
            OPTIONAL MATCH ()-[rule_rel]->()
            WHERE ($session_id = '' OR rule_rel.session_id = $session_id)
              AND type(rule_rel) IN $relationship_types
            WITH total_nodes, flagged_nodes, clean_nodes, flagged_relationships, all_relationships,
                 count(DISTINCT rule_rel) AS total_relationship_edges,
                 count(DISTINCT CASE WHEN type(rule_rel) = 'HUB_AND_SPOKE' THEN rule_rel END) AS hub_spoke_edges,
                 count(DISTINCT CASE WHEN type(rule_rel) = 'SMURFING' THEN rule_rel END) AS smurfing_edges,
                 count(DISTINCT CASE WHEN type(rule_rel) = 'CIRCULAR_FLOW' THEN rule_rel END) AS circular_flow_edges,
                 count(DISTINCT CASE WHEN type(rule_rel) = 'HIGH_RISK_LINK' THEN rule_rel END) AS high_risk_edges,
                 count(DISTINCT CASE WHEN type(rule_rel) = 'DORMANT_TO_ACTIVE' THEN rule_rel END) AS dormant_edges,
                 count(DISTINCT CASE WHEN type(rule_rel) = 'ABNORMAL_BALANCE_CHANGE' THEN rule_rel END) AS balance_change_edges,
                 count(DISTINCT CASE WHEN type(rule_rel) = 'SHARED_IDENTIFIER' THEN rule_rel END) AS shared_id_edges
            OPTIONAL MATCH (metric_node:{safe_label})
            WHERE ($session_id = '' OR metric_node.batch_id STARTS WITH $session_id OR metric_node.session_id = $session_id)
            RETURN
                total_nodes,
                flagged_nodes,
                clean_nodes,
                flagged_relationships,
                all_relationships,
                total_relationship_edges,
                hub_spoke_edges,
                smurfing_edges,
                circular_flow_edges,
                high_risk_edges,
                dormant_edges,
                balance_change_edges,
                shared_id_edges,
                min(coalesce(metric_node.degree, 0)) AS degree_min,
                max(coalesce(metric_node.degree, 0)) AS degree_max,
                avg(coalesce(metric_node.degree, 0)) AS degree_avg,
                min(coalesce(metric_node.in_degree, 0)) AS in_degree_min,
                max(coalesce(metric_node.in_degree, 0)) AS in_degree_max,
                avg(coalesce(metric_node.in_degree, 0)) AS in_degree_avg,
                min(coalesce(metric_node.out_degree, 0)) AS out_degree_min,
                max(coalesce(metric_node.out_degree, 0)) AS out_degree_max,
                avg(coalesce(metric_node.out_degree, 0)) AS out_degree_avg,
                min(coalesce(metric_node.pagerank, 0)) AS pagerank_min,
                max(coalesce(metric_node.pagerank, 0)) AS pagerank_max,
                avg(coalesce(metric_node.pagerank, 0)) AS pagerank_avg,
                min(coalesce(metric_node.betweenness, 0)) AS betweenness_min,
                max(coalesce(metric_node.betweenness, 0)) AS betweenness_max,
                avg(coalesce(metric_node.betweenness, 0)) AS betweenness_avg,
                min(coalesce(metric_node.eigenvector, 0)) AS eigenvector_min,
                max(coalesce(metric_node.eigenvector, 0)) AS eigenvector_max,
                avg(coalesce(metric_node.eigenvector, 0)) AS eigenvector_avg,
                count(DISTINCT metric_node.component_id) AS components
            """, session_id=session_id, relationship_types=TRANSACTION_RELATIONSHIPS).single()
    except Exception as exc:
        print("Risk Scoring link analysis summary failed:", exc)
        return None
    finally:
        driver.close()

    if not record:
        return None
    return {
        "total_nodes": int(record.get("total_nodes") or 0),
        "flagged_nodes": int(record.get("flagged_nodes") or 0),
        "clean_nodes": int(record.get("clean_nodes") or 0),
        "flagged_relationships": int(record.get("flagged_relationships") or 0),
        "all_relationships": int(record.get("all_relationships") or 0),
        "total_relationship_edges": int(record.get("total_relationship_edges") or 0),
        "hub_spoke_edges": int(record.get("hub_spoke_edges") or 0),
        "smurfing_edges": int(record.get("smurfing_edges") or 0),
        "circular_flow_edges": int(record.get("circular_flow_edges") or 0),
        "high_risk_edges": int(record.get("high_risk_edges") or 0),
        "dormant_edges": int(record.get("dormant_edges") or 0),
        "balance_change_edges": int(record.get("balance_change_edges") or 0),
        "shared_id_edges": int(record.get("shared_id_edges") or 0),
        "degree_avg": float(record.get("degree_avg") or 0.0),
        "pagerank_avg": float(record.get("pagerank_avg") or 0.0),
        "betweenness_avg": float(record.get("betweenness_avg") or 0.0),
    }


def _get_linked_entities_from_neo4j(
    session_id,
    account_no,
    search_column="accountno",
    flagged_rule_types=None,
    max_entities=None,
):
    max_entities = max_entities if max_entities is not None else get_max_linked_entities_setting(session_id)
    credentials = _neo4j_credentials(session_id)
    if not credentials:
        return []

    node_label = rule_to_node_label("bank transactions", session_id)
    safe_label = f"`{str(node_label).replace('`', '')}`"
    driver = create_neo4j_driver(credentials)
    raw_entities = []
    flagged_rules = list(flagged_rule_types or [])
    try:
        with driver.session() as session:
            records = session.run(f"""
            MATCH (n:{safe_label})-[r]->(target:{safe_label})
            WHERE ($session_id = '' OR n.batch_id STARTS WITH $session_id OR n.session_id = $session_id)
              AND (
                  coalesce(n.ACCOUNTNO, n.accountno) = $account_no
                  OR coalesce(target.ACCOUNTNO, target.BENACCOUNTNO, target.accountno) = $account_no
              )
              AND (
                  $has_flagged_rules = false
                  OR type(r) IN $flagged_rules
                  OR coalesce(r.is_flagged, false) = true
              )
            RETURN 
                coalesce(target.ACCOUNTNO, target.BENACCOUNTNO, target.accountno, n.ACCOUNTNO, n.accountno, 'UNKNOWN') AS target_id,
                type(r) AS rel_type,
                coalesce(r.is_flagged, false) AS is_flagged,
                coalesce(target.is_flagged, false) AS target_flagged,
                target.risk_score AS risk_score
            LIMIT 500
            """, session_id=session_id, account_no=account_no, has_flagged_rules=bool(flagged_rules), flagged_rules=flagged_rules)

            seen = set()
            for rec in records:
                target_id = rec.get("target_id")
                if not target_id or target_id == "UNKNOWN" or str(target_id) == str(account_no) or target_id in seen:
                    continue
                seen.add(target_id)
                rel_type = str(rec.get("rel_type") or "TRANSACTS_TO")
                is_flag = bool(rec.get("is_flagged") or rec.get("target_flagged") or (rel_type in flagged_rules))
                risk_val = float(rec.get("risk_score") or (0.85 if is_flag else 0.2))

                raw_entities.append({
                    search_column: str(target_id),
                    "relationship": rel_type,
                    "risk_contribution": round(risk_val, 2),
                    "is_flagged": is_flag,
                })
    except Exception as exc:
        print(f"Risk Scoring link entities extraction failed: {exc}")
    finally:
        driver.close()

    # Sort strictly by risk_contribution descending so critical risk nodes are never dropped
    raw_entities.sort(key=lambda x: (x["risk_contribution"], x["is_flagged"]), reverse=True)

    # Apply configurable cap
    if max_entities and int(max_entities) > 0:
        raw_entities = raw_entities[:int(max_entities)]

    # Assign sequential entity_id starting from '01'
    entities = []
    for idx, item in enumerate(raw_entities, start=1):
        ent = {
            "entity_id": f"{idx:02d}",
            search_column: item[search_column],
            "relationship": item["relationship"],
            "risk_contribution": item["risk_contribution"],
        }
        if item.get("is_flagged"):
            ent["flagged"] = True
            ent["flag_reason"] = f"flagged rule relationship ({item['relationship']})"
        entities.append(ent)

    return entities


def sanitize_risk_scoring_request(raw_event):
    """
    Sanitization Layer for incoming Risk Scoring score.calculated events.
    Filters and extracts ONLY the strictly required fields needed for
    LinkX graph analysis and distributed tracing, dropping all bulk payloads.
    """
    if isinstance(raw_event, (bytes, bytearray)):
        raw_event = raw_event.decode("utf-8")
    if isinstance(raw_event, str):
        try:
            raw_event = json.loads(raw_event)
        except Exception as exc:
            raise ValueError(f"invalid_json_payload: {exc}")

    if not isinstance(raw_event, dict):
        raise ValueError("payload_must_be_a_dict")

    data = raw_event.get("data") or {}
    meta = raw_event.get("meta") or {}
    agg_key = meta.get("aggregation_key") or {}

    # 1. Extract exact aggregation key as passed (type specifies storage search column directly)
    raw_agg_type = str(agg_key.get("type") or "accountno").strip()
    entity_id = str(
        agg_key.get("value")
        or data.get("entity_id")
        or data.get("accountno")
        or ""
    ).strip()

    if not entity_id:
        raise ValueError("missing_required_entity_id")

    # 2. Extract transaction ID (optional but preserved for provenance)
    transaction_id = str(data.get("transaction_id") or "").strip() or None

    # 3. Determine node type (account vs corporate entity)
    is_entity = bool(data.get("is_entity", False))

    # 4. Extract or generate distributed tracing context
    trace_id = str(meta.get("trace_id") or os.urandom(16).hex())
    span_id = str(meta.get("span_id") or os.urandom(8).hex())
    correlation_id = str(meta.get("correlation_id") or trace_id)
    timestamp = str(meta.get("timestamp") or _iso8601_now())

    # 5. Extract optional custom source/target columns if explicitly requested
    source_col = str(data.get("source_column") or data.get("source_col") or data.get("source") or "").strip() or None
    target_col = str(data.get("target_column") or data.get("target_col") or data.get("target") or "").strip() or None
    custom_rel = str(data.get("relationship") or data.get("relationship_name") or "").strip() or None

    # 6. Extract optional max_linked_entities cap
    max_entities_val = data.get("max_linked_entities") or data.get("max_entities") or data.get("limit")
    max_linked_entities = None
    if max_entities_val is not None:
        try:
            max_linked_entities = int(max_entities_val)
        except (ValueError, TypeError):
            max_linked_entities = None

    sanitized = {
        "event_type": str(raw_event.get("event_type") or "score.calculated"),
        "data": {
            "entity_id": entity_id,
            "is_entity": is_entity,
        },
        "meta": {
            "trace_id": trace_id,
            "span_id": span_id,
            "traceparent": f"00-{trace_id}-{span_id}-01",
            "correlation_id": correlation_id,
            "timestamp": timestamp,
            "aggregation_key": {
                "type": raw_agg_type or "accountno",
                "value": entity_id,
            },
        },
    }

    if transaction_id:
        sanitized["data"]["transaction_id"] = transaction_id
    if source_col:
        sanitized["data"]["source_column"] = source_col
    if target_col:
        sanitized["data"]["target_column"] = target_col
    if custom_rel:
        sanitized["data"]["relationship"] = custom_rel
    if max_linked_entities is not None:
        sanitized["data"]["max_linked_entities"] = max_linked_entities

    return sanitized


def execute_formal_link_analysis(event_data):
    """
    Executes the Formal 7-Step LinkX Analysis Pipeline:
    1. Sanitize & Validate Input Payload
    2. Prepare Controlled Session
    3. Search HDFS / Elasticsearch using exact passed aggregation_key.type column
    4. Construct LinkX DataFrame
    5. Ingest into Neo4j using requested source/target columns (or fallback to rule defaults)
    6. Run Incremental Cypher Rules & Centrality Metrics
    7. Extract Summary Metrics & Route Response Event
    """
    t0 = time.time()

    # Step 0: Sanitization Layer
    try:
        sanitized_event = sanitize_risk_scoring_request(event_data)
    except Exception as sanitize_exc:
        print(f"[RiskScoring] Sanitization failed: {sanitize_exc}")
        return None, f"sanitization_failed: {sanitize_exc}"

    data_section = sanitized_event["data"]
    meta_section = sanitized_event["meta"]
    agg_key = meta_section.get("aggregation_key") or {}

    search_column = str(agg_key.get("type") or "accountno").strip()
    keyword = str(agg_key.get("value") or data_section["entity_id"]).strip()
    account_no = keyword

    date = data_section.get("date") or data_section.get("transactiondate")
    if date and "T" in str(date):
        date = str(date).split("T")[0]

    ts_now = int(time.time())
    session_id = f"scoring-{account_no}-{ts_now}"

    # Step 1: Session Control & Isolation
    if not _prepare_session(session_id):
        return None, "session_prepare_failed"

    # Step 2: Storage Retrieval (HDFS / Elasticsearch)
    strict_mood = True
    date_column = _config_value(session_id, "date_column")

    storage_address = _config_value(session_id, "active_storage_host") or _config_value(session_id, "active_storage_address")
    if storage_address and ":" in storage_address:
        storage_address = storage_address.split(":", 1)[0]

    api_port = _config_value(session_id, "api_port")
    api_search_endpoint = str(_config_value(session_id, "search_api_endpoint_es_strict")).strip("/")
    api_url = os.getenv("LINKX_PUBLIC_ES_API_URL") or os.getenv("LINKX_STR_ELASTIC_API_URL")
    if not api_url:
        elastic_base_url = _config_value(session_id, "elastic_api_base_url")
        if elastic_base_url:
            api_url = f"{str(elastic_base_url).rstrip('/')}/{api_search_endpoint}"
        else:
            api_url = f"http://{storage_address}:{api_port}/{api_search_endpoint}"

    print(f"[RiskScoring] Querying Elasticsearch for {search_column}={keyword} on {api_url}")
    response = es_keyword_search(
        "search",
        api_url,
        keyword,
        search_column,
        strict_mood,
        date_column,
        date,
        auth_header=load_temp_config("elastic_api_authorization", session_id)
        or _config_value(session_id, "elastic_api_authorization")
        or os.getenv("LINKX_ELASTIC_API_AUTHORIZATION"),
    )

    result_size = 0
    if response and isinstance(response, dict):
        results = response.get("results") or []
        if results:
            result_size = int(results[0].get("size", 0) or 0)

    # If no records in storage, treat as clean/clear
    if result_size <= 0:
        duration_ms = (time.time() - t0) * 1000.0
        return build_link_response(
            event_data=event_data,
            account_no=account_no,
            is_flagged=False,
            linked_entities=[],
            linked_count=0,
            flagged_count=0,
            centrality_score=0.0,
            max_path_length=0,
            duration_ms=duration_ms,
            session_id=session_id,
        ), "success"

    # Step 3: DataFrame Generation
    dataframe_payload = {
        "id": "create_DF",
        "session_id": session_id,
        "kind": "hybrid",
        "type": "array",
        "date": date,
        "value": [{"type": "elastic", "column": search_column, "keyword": keyword, "strict": strict_mood}],
    }
    dataframe_result, dataframe_status = create_dataframe_result(dataframe_payload, session_id)
    if dataframe_status != 200:
        print(f"[RiskScoring] Dataframe creation failed for {account_no}. Status: {dataframe_status}, Response: {dataframe_result}")
        duration_ms = (time.time() - t0) * 1000.0
        return None, "dataframe_creation_failed"

    # Step 4: Neo4j Ingestion & Step 5: Incremental Analysis
    custom_source = data_section.get("source_column")
    custom_target = data_section.get("target_column")
    custom_rel = data_section.get("relationship")

    if not _ingest_dataframe_to_neo4j(
        session_id,
        custom_source=custom_source,
        custom_target=custom_target,
        custom_rel=custom_rel,
    ):
        duration_ms = (time.time() - t0) * 1000.0
        return None, "neo4j_ingestion_failed"

    # Step 6: Summary Metrics Extraction
    summary = _analysis_summary(session_id) or {}
    flagged_nodes = int(summary.get("flagged_nodes") or 0)
    flagged_rels = int(summary.get("flagged_relationships") or 0)
    total_nodes = int(summary.get("total_nodes") or 0)
    total_rel_edges = int(summary.get("total_relationship_edges") or 0)
    hub_spoke_edges = int(summary.get("hub_spoke_edges") or 0)
    smurfing_edges = int(summary.get("smurfing_edges") or 0)
    circular_flow_edges = int(summary.get("circular_flow_edges") or 0)
    high_risk_edges = int(summary.get("high_risk_edges") or 0)
    dormant_edges = int(summary.get("dormant_edges") or 0)
    balance_change_edges = int(summary.get("balance_change_edges") or 0)
    shared_id_edges = int(summary.get("shared_id_edges") or 0)

    active_flagged_rules = [
        rule_name for rule_name, is_set in [
            ("HUB_AND_SPOKE", hub_spoke_edges > 0),
            ("SMURFING", smurfing_edges > 0),
            ("CIRCULAR_FLOW", circular_flow_edges > 0),
            ("HIGH_RISK_LINK", high_risk_edges > 0),
            ("DORMANT_TO_ACTIVE", dormant_edges > 0),
            ("ABNORMAL_BALANCE_CHANGE", balance_change_edges > 0),
            ("SHARED_IDENTIFIER", shared_id_edges > 0),
        ] if is_set
    ]

    max_entities = data_section.get("max_linked_entities")
    if max_entities is None:
        max_entities = get_max_linked_entities_setting(session_id)
    linked_entities = _get_linked_entities_from_neo4j(
        session_id,
        account_no,
        search_column=search_column,
        flagged_rule_types=active_flagged_rules,
        max_entities=max_entities,
    )
    is_flagged = bool(
        flagged_nodes > 0
        or flagged_rels > 0
        or total_rel_edges > 0
        or bool(active_flagged_rules)
    )
    
    degree_avg = float(summary.get("degree_avg") or 0.0)
    pagerank_avg = float(summary.get("pagerank_avg") or 0.0)
    centrality_score = round(min(1.0, 0.15 + (degree_avg * 0.05) + (pagerank_avg * 0.3) + (flagged_nodes * 0.2)), 2)
    max_path_length = 2 if total_nodes > 2 else (1 if total_nodes > 0 else 0)
    duration_ms = (time.time() - t0) * 1000.0

    # Step 7: Build Standard Event Payload
    response_event = build_link_response(
        event_data=event_data,
        account_no=account_no,
        is_flagged=is_flagged,
        linked_entities=linked_entities,
        linked_count=len(linked_entities) if linked_entities else total_nodes,
        flagged_count=flagged_nodes,
        centrality_score=centrality_score,
        max_path_length=max_path_length,
        duration_ms=duration_ms,
        session_id=session_id,
        flagged_rules=active_flagged_rules if is_flagged else None,
    )

    return response_event, "success"


def build_link_response(
    event_data,
    account_no,
    is_flagged,
    linked_entities,
    linked_count,
    flagged_count,
    centrality_score,
    max_path_length,
    duration_ms,
    session_id="",
    flagged_rules=None,
):
    input_meta = dict((event_data or {}).get("meta") or {})
    trace_id = input_meta.get("trace_id") or os.urandom(16).hex()
    span_id = os.urandom(8).hex()
    correlation_id = input_meta.get("correlation_id") or trace_id

    event_type = "link.flagged" if is_flagged else "link.mapped"
    flagged_topic = _kafka_flagged_topic(session_id)
    mapped_topic = _kafka_mapped_topic(session_id)
    destination_topic = flagged_topic if is_flagged else mapped_topic
    action_text = "flagged" if is_flagged else "map completed"
    message_text = f"Link {action_text} for account {account_no}: {linked_count} linked"

    input_agg_key = input_meta.get("aggregation_key") or {}
    agg_type = str(input_agg_key.get("type") or "accountno")
    agg_val = str(input_agg_key.get("value") or account_no)

    data_payload = {
        agg_type: agg_val,
        "entity_id": agg_val,
        "linked_accounts_count": linked_count,
        "flagged_entity_links": flagged_count,
        "beneficiary_blacklisted": is_flagged,
    }
    if is_flagged and flagged_rules:
        data_payload["flagged_rules"] = flagged_rules

    data_payload["linked_entities"] = linked_entities
    data_payload["network_centrality_score"] = centrality_score
    data_payload["max_path_length"] = max_path_length

    return {
        "schema_version": "1.0",
        "success": True,
        "event_type": event_type,
        "message": message_text,
        "data": data_payload,
        "meta": {
            "trace_id": trace_id,
            "span_id": span_id,
            "traceparent": f"00-{trace_id}-{span_id}-01",
            "correlation_id": correlation_id,
            "timestamp": _iso8601_now(),
            "service": {
                "name": "link-analysis-ms",
                "version": "1.0.0",
                "namespace": "risk-decision-platform",
            },
            "session_id": session_id,
            "messaging": {
                "system": "kafka",
                "destination_name": destination_topic,
                "operation_name": "publish",
            },
            "source_id": "link",
            "aggregation_key": {
                "type": agg_type,
                "value": agg_val,
            },
            "processing": {
                "duration_ms": round(float(duration_ms), 1),
            },
        },
        "error": None,
    }


def ensure_risk_scoring_evidence_schema():
    try:
        from batch_manager.utils.postgres_utils import get_postgres_connection
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS link_analysis_evidence (
                    id BIGSERIAL PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    correlation_id TEXT,
                    transaction_id TEXT,
                    entity_id TEXT NOT NULL,
                    entity_type TEXT NOT NULL DEFAULT 'accountno',
                    session_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    is_flagged BOOLEAN NOT NULL DEFAULT FALSE,
                    flagged_rules JSONB,
                    linked_accounts_count INT NOT NULL DEFAULT 0,
                    network_centrality_score NUMERIC(5, 2),
                    max_path_length INT,
                    duration_ms NUMERIC(10, 2),
                    request_payload JSONB NOT NULL,
                    response_payload JSONB NOT NULL,
                    analyzed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_evidence_trace_entity UNIQUE (trace_id, entity_id)
                );
                CREATE INDEX IF NOT EXISTS idx_link_evidence_entity ON link_analysis_evidence (entity_id, analyzed_at DESC);
                CREATE INDEX IF NOT EXISTS idx_link_evidence_flagged ON link_analysis_evidence (is_flagged, analyzed_at DESC);
                CREATE INDEX IF NOT EXISTS idx_link_evidence_tx ON link_analysis_evidence (transaction_id);
                """)
            conn.commit()
    except Exception as exc:
        print(f"[RiskScoring] Schema ensure notice: {exc}")


def get_cached_or_persisted_evidence(entity_id, trace_id=None, transaction_id=None):
    if not entity_id:
        return None
    try:
        from batch_manager.utils.postgres_utils import get_postgres_connection
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                if trace_id:
                    cur.execute("""
                    SELECT response_payload
                    FROM link_analysis_evidence
                    WHERE trace_id = %s AND entity_id = %s
                    LIMIT 1
                    """, (str(trace_id), str(entity_id)))
                    row = cur.fetchone()
                    if row and row[0]:
                        return row[0]

                if transaction_id:
                    cur.execute("""
                    SELECT response_payload
                    FROM link_analysis_evidence
                    WHERE transaction_id = %s AND entity_id = %s
                      AND analyzed_at >= NOW() - INTERVAL '2 hours'
                    ORDER BY analyzed_at DESC
                    LIMIT 1
                    """, (str(transaction_id), str(entity_id)))
                    row = cur.fetchone()
                    if row and row[0]:
                        return row[0]
    except Exception as exc:
        print(f"[RiskScoring] Evidence cache check notice: {exc}")
    return None


def persist_link_analysis_evidence(request_event, response_event, session_id="", duration_ms=0.0):
    try:
        from batch_manager.utils.postgres_utils import get_postgres_connection
        ensure_risk_scoring_evidence_schema()

        req_meta = dict((request_event or {}).get("meta") or {})
        req_data = dict((request_event or {}).get("data") or {})
        resp_data = dict((response_event or {}).get("data") or {})

        trace_id = str(req_meta.get("trace_id") or (response_event.get("meta") or {}).get("trace_id") or os.urandom(16).hex())
        correlation_id = str(req_meta.get("correlation_id") or (response_event.get("meta") or {}).get("correlation_id") or trace_id)
        transaction_id = req_data.get("transaction_id") or None
        if transaction_id:
            transaction_id = str(transaction_id)

        agg_key = req_meta.get("aggregation_key") or {}
        entity_type = str(agg_key.get("type") or "accountno")
        entity_id = str(agg_key.get("value") or req_data.get("entity_id") or resp_data.get("entity_id") or "")

        event_type = str(response_event.get("event_type") or "link.mapped")
        is_flagged = bool(event_type == "link.flagged" or resp_data.get("beneficiary_blacklisted"))
        flagged_rules = resp_data.get("flagged_rules") or []
        linked_count = int(resp_data.get("linked_accounts_count") or 0)
        centrality = float(resp_data.get("network_centrality_score") or 0.0)
        max_path = int(resp_data.get("max_path_length") or 0)

        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO link_analysis_evidence (
                    trace_id, correlation_id, transaction_id, entity_id, entity_type,
                    session_id, event_type, is_flagged, flagged_rules, linked_accounts_count,
                    network_centrality_score, max_path_length, duration_ms,
                    request_payload, response_payload, analyzed_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s::jsonb, %s,
                    %s, %s, %s,
                    %s::jsonb, %s::jsonb, NOW()
                )
                ON CONFLICT (trace_id, entity_id) DO UPDATE SET
                    response_payload = EXCLUDED.response_payload,
                    duration_ms = EXCLUDED.duration_ms,
                    analyzed_at = NOW()
                """, (
                    trace_id, correlation_id, transaction_id, entity_id, entity_type,
                    session_id, event_type, is_flagged, json.dumps(flagged_rules), linked_count,
                    centrality, max_path, round(float(duration_ms), 2),
                    json.dumps(request_event), json.dumps(response_event),
                ))

                # Inject into the unified linkx_reports pipeline as SERVICE_EVIDENCE
                report_payload = {
                    "trace_id": trace_id,
                    "entity_id": entity_id,
                    "is_flagged": is_flagged,
                    "flagged_rules": flagged_rules,
                    "network_centrality_score": centrality,
                    "max_path_length": max_path
                }
                cur.execute("""
                    INSERT INTO linkx_reports (report_type, source_system, external_reference_id, payload, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, ('SERVICE_EVIDENCE', 'risk_scoring', trace_id, json.dumps(report_payload), 'FLAGGED' if is_flagged else 'RESOLVED'))
            conn.commit()
    except Exception as exc:
        print(f"[RiskScoring] Evidence persistence notice: {exc}")


def process_risk_scoring_event(event_data, brokers=None, publish=True):
    """
    Automated workflow with Idempotency & Evidence Persistence:
    1. Consumes/ingests the incoming Risk Scoring event.
    2. Checks database for existing analysis (deduplication / replay).
    3. If new: Runs full Link Analysis (Elasticsearch, Neo4j, typology rules).
    4. Persists immutable analysis evidence to PostgreSQL.
    5. Automatically produces response to Kafka topic (dev.analysis.link.mapped.v1).
    """
    try:
        sanitized = sanitize_risk_scoring_request(event_data)
    except Exception:
        sanitized = event_data if isinstance(event_data, dict) else {}

    s_meta = dict(sanitized.get("meta") or {})
    s_data = dict(sanitized.get("data") or {})
    entity_id = (s_meta.get("aggregation_key") or {}).get("value") or s_data.get("entity_id")
    trace_id = s_meta.get("trace_id")
    tx_id = s_data.get("transaction_id")

    # 1. Deduplication / Idempotency check:
    cached = get_cached_or_persisted_evidence(entity_id=entity_id, trace_id=trace_id, transaction_id=tx_id)
    if cached:
        print(f"[RiskScoring] Deduplication: Replaying existing analysis for entity {entity_id}")
        if publish:
            publish_risk_scoring_response(cached, brokers=brokers)
        return cached

    # 2. Fresh Analysis execution:
    response_event, status = execute_formal_link_analysis(event_data)
    if not response_event:
        return {"status": "failed", "detail": status}

    # 3. Persist Evidence to PostgreSQL:
    sess_id = (response_event.get("meta") or {}).get("session_id") or ""
    dur_ms = (response_event.get("meta") or {}).get("processing", {}).get("duration_ms") or 0.0
    persist_link_analysis_evidence(
        request_event=sanitized,
        response_event=response_event,
        session_id=sess_id,
        duration_ms=dur_ms,
    )

    # 4. Auto-publish to Kafka:
    if publish:
        publish_risk_scoring_response(response_event, brokers=brokers)

    return response_event


def publish_risk_scoring_response(response_event, brokers=None, topic=None, session_id=""):
    brokers = brokers or _kafka_brokers(session_id)
    is_flagged = (response_event.get("event_type") == "link.flagged")
    default_topic = _kafka_flagged_topic(session_id) if is_flagged else _kafka_mapped_topic(session_id)
    target_topic = topic or default_topic

    meta = dict(response_event.get("meta") or {})
    account_key = (
        str((response_event.get("data") or {}).get("accountno") or "")
        or str((meta.get("aggregation_key") or {}).get("value") or "")
        or str((response_event.get("data") or {}).get("entity_id") or "")
    )
    key = account_key.encode("utf-8") if account_key else None
    val = json.dumps(response_event).encode("utf-8")

    traceparent = str(meta.get("traceparent") or f"00-{meta.get('trace_id', os.urandom(16).hex())}-{meta.get('span_id', os.urandom(8).hex())}-01")
    correlation_id = str(meta.get("correlation_id") or meta.get("trace_id") or os.urandom(16).hex())

    kafka_headers = [
        ("traceparent", traceparent.encode("utf-8")),
        ("X-Correlation-ID", correlation_id.encode("utf-8")),
        ("content-type", b"application/json"),
    ]

    # 1. Try confluent-kafka
    try:
        from confluent_kafka import Producer
        conf = {"bootstrap.servers": brokers, "client.id": "linkx-link-analysis-producer"}
        p = Producer(conf)
        p.produce(target_topic, key=key, value=val, headers=kafka_headers)
        p.flush(timeout=5.0)
        return True
    except ImportError:
        pass
    except Exception as exc:
        print(f"[RiskScoring] confluent_kafka publish error to {target_topic}: {exc}")

    # 2. Fallback to kafka-python
    try:
        from kafka import KafkaProducer
        server_list = [b.strip() for b in brokers.split(",") if b.strip()]
        kp = KafkaProducer(bootstrap_servers=server_list, client_id="linkx-link-analysis-producer")
        future = kp.send(target_topic, key=key, value=val, headers=kafka_headers)
        future.get(timeout=5.0)
        kp.flush()
        return True
    except Exception as exc:
        print(f"[RiskScoring] kafka-python publish error to {target_topic}: {exc}")
        return False


def start_risk_scoring_consumer(
    brokers=None,
    input_topic=None,
    group_id="dev.analysis.link.consumer",
    auto_publish=True,
    max_messages=None,
):
    """
    Continuous automated runner:
    1. Consumes messages from input_topic (dev.scoring.score.calculated.v1).
    2. Runs Link Analysis pipeline automatically.
    3. Builds response.
    4. Auto-produces response to destination topic (dev.analysis.link.mapped.v1).
    """
    brokers = brokers or _kafka_brokers()
    topic = input_topic or DEFAULT_INPUT_TOPIC
    server_list = [b.strip() for b in brokers.split(",") if b.strip()]

    print(f"[RiskScoringConsumer] Starting automated link analysis worker...")
    print(f"[RiskScoringConsumer] Brokers: {brokers} | Input: {topic} | Group: {group_id}")

    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=server_list,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        )
    except Exception as exc:
        print(f"[RiskScoringConsumer] Failed to initialize Kafka consumer: {exc}")
        return

    count = 0
    try:
        for msg in consumer:
            if not msg.value:
                continue
            count += 1
            print(f"[RiskScoringConsumer] [{count}] Received event on {msg.topic} [partition={msg.partition}, offset={msg.offset}]")
            try:
                result = process_risk_scoring_event(msg.value, brokers=brokers, publish=auto_publish)
                status = "success" if (result and result.get("success")) else "failed"
                evt_type = (result or {}).get("event_type", "unknown")
                print(f"[RiskScoringConsumer] [{count}] Processed event: status={status}, event_type={evt_type}")
            except Exception as proc_err:
                print(f"[RiskScoringConsumer] [{count}] Error processing event: {proc_err}")

            if max_messages and count >= max_messages:
                break
    except KeyboardInterrupt:
        print("[RiskScoringConsumer] Consumer stopped by interrupt.")
    finally:
        consumer.close()

