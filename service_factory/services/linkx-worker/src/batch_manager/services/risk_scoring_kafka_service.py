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

DEFAULT_KAFKA_BROKERS = os.getenv(
    "LINKX_KAFKA_BOOTSTRAP_SERVERS",
    "172.27.23.70:9092,172.27.23.118:9092,172.27.23.100:9092",
)
DEFAULT_INPUT_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_INPUT_TOPIC", "dev.scoring.score.calculated.v1"
)
DEFAULT_MAPPED_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_MAPPED_TOPIC", "dev.analysis.link.mapped.v1"
)
DEFAULT_FLAGGED_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_FLAGGED_TOPIC", "dev.analysis.link.flagged.v1"
)


def _iso8601_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _bank_source_target_relationship(session_id):
    return {
        "source": _config_value(session_id, "default_source_col") or "accountno",
        "target": _config_value(session_id, "default_target_col") or "benaccountno",
        "relationship": _config_value(session_id, "default_relationship") or "TRANSACTS_TO",
    }


def _prepare_session(session_id):
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
        return analyzer(payload) is True
    except Exception as exc:
        print(f"Risk Scoring link analysis {step_name} analyzer failed:", exc)
        return False


def _ingest_dataframe_to_neo4j(session_id):
    rule = "bank transactions"
    credentials = _neo4j_credentials(session_id)
    if not credentials:
        print("Risk Scoring link analysis missing neo4j credentials")
        return False

    link_payload = _base_analyzer_payload(session_id, credentials)
    link_payload.update({"action": "Link Analysis", "rule": rule})
    if not _run_analyzer(link_payload, "link analysis"):
        return False

    source_target = _bank_source_target_relationship(session_id)
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
                 count(DISTINCT rule_rel) AS total_relationship_edges
            OPTIONAL MATCH (metric_node:{safe_label})
            WHERE ($session_id = '' OR metric_node.batch_id STARTS WITH $session_id OR metric_node.session_id = $session_id)
            RETURN
                total_nodes,
                flagged_nodes,
                clean_nodes,
                flagged_relationships,
                all_relationships,
                total_relationship_edges,
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
        "degree_avg": float(record.get("degree_avg") or 0.0),
        "pagerank_avg": float(record.get("pagerank_avg") or 0.0),
        "betweenness_avg": float(record.get("betweenness_avg") or 0.0),
    }


def _get_linked_entities_from_neo4j(session_id, account_no):
    credentials = _neo4j_credentials(session_id)
    if not credentials:
        return []

    node_label = rule_to_node_label("bank transactions", session_id)
    safe_label = f"`{str(node_label).replace('`', '')}`"
    driver = create_neo4j_driver(credentials)
    entities = []
    try:
        with driver.session() as session:
            records = session.run(f"""
            MATCH (n:{safe_label})
            WHERE ($session_id = '' OR n.batch_id STARTS WITH $session_id OR n.session_id = $session_id)
              AND coalesce(n.ACCOUNTNO, n.accountno) = $account_no
            OPTIONAL MATCH (n)-[r]->(target:{safe_label})
            WHERE ($session_id = '' OR r.session_id = $session_id)
            RETURN 
                coalesce(target.ACCOUNTNO, target.BENACCOUNTNO, target.accountno, 'UNKNOWN') AS target_id,
                type(r) AS rel_type,
                coalesce(r.is_flagged, false) AS is_flagged,
                coalesce(target.is_flagged, false) AS target_flagged,
                target.risk_score AS risk_score
            LIMIT 50
            """, session_id=session_id, account_no=account_no)

            seen = set()
            for rec in records:
                target_id = rec.get("target_id")
                if not target_id or target_id == "UNKNOWN" or target_id in seen:
                    continue
                seen.add(target_id)
                is_flag = bool(rec.get("is_flagged") or rec.get("target_flagged"))
                risk_val = float(rec.get("risk_score") or (0.8 if is_flag else 0.2))
                ent = {
                    "entity_id": target_id,
                    "entity_type": "account",
                    "relationship": str(rec.get("rel_type") or "TRANSACTS_TO").lower(),
                    "risk_contribution": round(risk_val, 2),
                }
                if is_flag:
                    ent["flagged"] = True
                    ent["flag_reason"] = "flagged transaction link"
                entities.append(ent)
    except Exception as exc:
        print(f"Risk Scoring link entities extraction failed: {exc}")
    finally:
        driver.close()
    return entities


def sanitize_risk_scoring_request(raw_event):
    """
    Sanitization Layer for incoming Risk Scoring score.calculated events.
    Filters and normalizes ONLY the strictly required fields needed for
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

    # 1. Extract & normalize entity/account identifier
    entity_id = str(
        data.get("entity_id")
        or agg_key.get("value")
        or data.get("accountno")
        or ""
    ).strip()

    if not entity_id:
        raise ValueError("missing_required_entity_id")

    # 2. Extract transaction ID (optional but recommended)
    transaction_id = str(data.get("transaction_id") or "").strip() or None

    # 3. Determine node type (account vs corporate entity)
    is_entity = bool(data.get("is_entity", False))

    # 4. Extract or generate distributed tracing context
    trace_id = str(meta.get("trace_id") or os.urandom(16).hex())
    span_id = str(meta.get("span_id") or os.urandom(8).hex())
    correlation_id = str(meta.get("correlation_id") or trace_id)
    timestamp = str(meta.get("timestamp") or _iso8601_now())

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
                "type": "accountno",
                "value": entity_id,
            },
        },
    }

    if transaction_id:
        sanitized["data"]["transaction_id"] = transaction_id

    return sanitized


def execute_formal_link_analysis(event_data):
    """
    Executes the Formal 7-Step LinkX Analysis Pipeline:
    1. Sanitize & Validate Input Payload
    2. Prepare Controlled Session
    3. Search HDFS / Elasticsearch
    4. Construct LinkX DataFrame
    5. Ingest into Neo4j
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
    account_no = data_section["entity_id"]

    date = meta_section.get("timestamp")
    if date and "T" in str(date):
        date = str(date).split("T")[0]

    ts_now = int(time.time())
    session_id = f"risk_scoring_{account_no}_{ts_now}"

    # Step 1: Session Control & Isolation
    if not _prepare_session(session_id):
        return None, "session_prepare_failed"

    # Step 2: Storage Retrieval (HDFS / Elasticsearch)
    keyword = account_no
    search_column = "accountno"
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

    print(f"[RiskScoring] Querying Elasticsearch for account {account_no} on {api_url}")
    response = es_keyword_search(
        "search",
        api_url,
        keyword,
        search_column,
        strict_mood,
        date_column,
        date,
        auth_header=load_temp_config("elastic_api_authorization", session_id),
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
        duration_ms = (time.time() - t0) * 1000.0
        return None, "dataframe_creation_failed"

    # Step 4: Neo4j Ingestion & Step 5: Incremental Analysis
    if not _ingest_dataframe_to_neo4j(session_id):
        duration_ms = (time.time() - t0) * 1000.0
        return None, "neo4j_ingestion_failed"

    # Step 6: Summary Metrics Extraction
    summary = _analysis_summary(session_id) or {}
    flagged_nodes = int(summary.get("flagged_nodes") or 0)
    flagged_rels = int(summary.get("flagged_relationships") or 0)
    total_nodes = int(summary.get("total_nodes") or 0)

    linked_entities = _get_linked_entities_from_neo4j(session_id, account_no)
    is_flagged = (flagged_nodes > 0 or flagged_rels > 0)
    
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

    data_payload = {
        "accountno": account_no,
        "linked_accounts_count": linked_count,
        "flagged_entity_links": flagged_count,
        "beneficiary_blacklisted": is_flagged,
        "linked_entities": linked_entities,
        "network_centrality_score": centrality_score,
        "max_path_length": max_path_length,
    }
    if is_flagged:
        data_payload["flags"] = {
            "beneficiary_blacklisted": is_flagged,
            "flagged_entity_links": (flagged_count > 0),
        }

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
            "messaging": {
                "system": "kafka",
                "destination_name": destination_topic,
                "operation_name": "publish",
            },
            "source_id": "link",
            "aggregation_key": {
                "type": "accountno",
                "value": account_no,
            },
            "processing": {
                "duration_ms": round(float(duration_ms), 1),
            },
        },
        "error": None,
    }


def process_risk_scoring_event(event_data, brokers=None, publish=False):
    """
    Job handler wrapper to process an incoming Risk Scoring event,
    execute the formal link analysis, and publish the output to Kafka.
    """
    response_event, status = execute_formal_link_analysis(event_data)
    if not response_event:
        return {"status": "failed", "detail": status}

    if publish:
        publish_risk_scoring_response(response_event, brokers=brokers)

    return response_event


def publish_risk_scoring_response(response_event, brokers=None, topic=None, session_id=""):
    brokers = brokers or _kafka_brokers(session_id)
    is_flagged = (response_event.get("event_type") == "link.flagged")
    default_topic = _kafka_flagged_topic(session_id) if is_flagged else _kafka_mapped_topic(session_id)
    target_topic = topic or default_topic

    key = str((response_event.get("data") or {}).get("accountno") or "").encode("utf-8")
    val = json.dumps(response_event).encode("utf-8")

    # 1. Try confluent-kafka
    try:
        from confluent_kafka import Producer
        conf = {"bootstrap.servers": brokers, "client.id": "linkx-link-analysis-producer"}
        p = Producer(conf)
        p.produce(target_topic, key=key, value=val)
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
        future = kp.send(target_topic, key=key, value=val)
        future.get(timeout=5.0)
        kp.flush()
        return True
    except Exception as exc:
        print(f"[RiskScoring] kafka-python publish error to {target_topic}: {exc}")
        return False
