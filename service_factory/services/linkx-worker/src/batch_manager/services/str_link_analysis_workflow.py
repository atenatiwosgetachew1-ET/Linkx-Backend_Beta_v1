import os

from batch_manager.analyzing.analyzer import analyzer, rule_to_node_label
from batch_manager.analyzing.LA_rules_script import TRANSACTION_RELATIONSHIPS
from batch_manager.config_defaults import get_default_session_config
from batch_manager.services.dataframe_workflow import create_dataframe_result
from batch_manager.utils.artifact_utils import ensure_artifact_dir
from batch_manager.utils.elastic_utils import es_keyword_search
from batch_manager.utils.neo4j_utils import create_neo4j_driver
from globals import create_file, load_temp_config, save_temp_config


def _bank_source_target_relationship(session_id):
    return {
        "source": _config_value(session_id, "default_source_col") or "accountno",
        "target": _config_value(session_id, "default_target_col") or "benaccountno",
        "relationship": _config_value(session_id, "default_relationship") or "TRANSACTS_TO",
    }


def _source_target_relationship(entity, session_id):
    if entity == "bank":
        return _bank_source_target_relationship(session_id)
    return None


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
        "dataframe_dir": os.path.join(ensure_artifact_dir("dfparts"), f"merged_dfpart_{session_id}"),
        "spark_conf": {
            "storage_ip": _config_value(session_id, "active_storage_address"),
            "spark_port": _config_value(session_id, "spark_port"),
        },
        "tool": "neo4j",
        "tool_credentials": credentials,
        "log_file": f"{session_id}.log",
    }


def _run_analyzer(payload, step_name):
    print(f"STR link analysis {step_name} analyzer payload:", payload)
    try:
        return analyzer(payload) is True
    except Exception as exc:
        print(f"STR link analysis {step_name} analyzer failed:", exc)
        return False


def _ingest_dataframe_to_neo4j(session_id, entity):
    if entity == "bank":
        rule = "bank transactions"
    else:
        return False

    credentials = _neo4j_credentials(session_id)
    if not credentials:
        print("STR link analysis missing neo4j credentials")
        return False

    link_payload = _base_analyzer_payload(session_id, credentials)
    link_payload.update({"action": "Link Analysis", "rule": rule})
    if not _run_analyzer(link_payload, "link analysis"):
        return False

    source_target = _source_target_relationship(entity, session_id)
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


def _clean_number(value):
    if value is None:
        return 0
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return 0


def _metric_dict(record, prefix):
    return {
        "min": _clean_number(record.get(f"{prefix}_min")),
        "max": _clean_number(record.get(f"{prefix}_max")),
        "avg": _clean_number(record.get(f"{prefix}_avg")),
    }


def _analysis_summary(session_id, entity):
    if entity != "bank":
        return None
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
            WHERE n.batch_id STARTS WITH $session_id OR n.session_id = $session_id
            WITH collect(n) AS nodes, count(n) AS total_nodes
            OPTIONAL MATCH (flagged:{safe_label})-[r]-()
            WHERE flagged IN nodes
              AND r.session_id = $session_id
              AND type(r) IN $relationship_types
            WITH nodes, total_nodes, count(DISTINCT flagged) AS flagged_nodes, count(DISTINCT r) AS flagged_relationships
            CALL {{
                WITH total_nodes
                OPTIONAL MATCH ()-[any_rel]->()
                WHERE any_rel.session_id = $session_id
                RETURN count(DISTINCT type(any_rel)) AS all_relationships, count(DISTINCT any_rel) AS total_relationship_edges
            }}
            UNWIND nodes AS metric_node
            RETURN
                total_nodes,
                flagged_nodes,
                total_nodes - flagged_nodes AS clean_nodes,
                flagged_relationships,
                all_relationships,
                total_relationship_edges,
                min(coalesce(metric_node.degree, 0)) AS degree_min,
                max(coalesce(metric_node.degree, 0)) AS degree_max,
                avg(coalesce(metric_node.degree, 0)) AS degree_avg,
                min(coalesce(metric_node.inDegree, 0)) AS in_degree_min,
                max(coalesce(metric_node.inDegree, 0)) AS in_degree_max,
                avg(coalesce(metric_node.inDegree, 0)) AS in_degree_avg,
                min(coalesce(metric_node.outDegree, 0)) AS out_degree_min,
                max(coalesce(metric_node.outDegree, 0)) AS out_degree_max,
                avg(coalesce(metric_node.outDegree, 0)) AS out_degree_avg,
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
        print("STR link analysis summary failed:", exc)
        return None
    finally:
        driver.close()

    if not record:
        return None
    flagged_nodes = int(record.get("flagged_nodes") or 0)
    return {
        "total_nodes": int(record.get("total_nodes") or 0),
        "flagged_nodes": flagged_nodes,
        "clean_nodes": int(record.get("clean_nodes") or 0),
        "flagged_relationships": int(record.get("flagged_relationships") or 0),
        "all_relationships": int(record.get("all_relationships") or 0),
        "total_relationship_edges": int(record.get("total_relationship_edges") or 0),
        "metrics": {
            "degree": _metric_dict(record, "degree"),
            "inDegree": _metric_dict(record, "in_degree"),
            "outDegree": _metric_dict(record, "out_degree"),
            "pagerank": _metric_dict(record, "pagerank"),
            "betweenness": _metric_dict(record, "betweenness"),
            "eigenvector": _metric_dict(record, "eigenvector"),
            "components": int(record.get("components") or 0),
        },
    }


def _relationship_panel_payload(session_id, entity, status):
    source_target = _source_target_relationship(entity, session_id)
    relationships = []
    if source_target:
        relationship_type = source_target["relationship"]
        relationships.append({
            "id": f"rel_{relationship_type.lower()}",
            "type": relationship_type,
            "textcolor": "#ffffff",
            "bgcolor": "#750b8c",
        })
    return relationships


def run_str_link_analysis(data):
    data = dict(data or {})
    entity = str(data.get("entity", "")).strip().lower()
    type_value = str(data.get("type", "")).strip().lower()
    value = str(data.get("value", "")).strip()
    session_id = str(data.get("str_id") or data.get("session_id") or "")

    if not session_id:
        return {"message": "failed", "status": "failed", "detail": "missing_session_id"}
    if not entity or not type_value or not value:
        return {"message": "failed", "status": "failed", "detail": "missing_required_fields", "session_id": session_id}
    if entity != "bank" or type_value != "account_number":
        return {"message": "failed", "status": "failed", "detail": "unsupported_str_request", "session_id": session_id}
    if not _prepare_session(session_id):
        return {"message": "failed", "status": "failed", "detail": "session_prepare_failed", "session_id": session_id}

    keyword = value
    search_column = "accountno"
    strict_mood = True
    date_column = _config_value(session_id, "date_column")
    date = data.get("date")

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

    print("STR link analysis worker request", {"session_id": session_id, "entity": entity, "type": type_value, "elastic_api_url": api_url})
    response = es_keyword_search("search", api_url, keyword, search_column, strict_mood, date_column, date)
    result_size = 0
    if response and isinstance(response, dict):
        results = response.get("results") or []
        if results:
            result_size = int(results[0].get("size", 0) or 0)

    if result_size <= 0:
        return {"message": "Not found!", "status": "not_found", "session_id": session_id, "result_size": result_size}

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
        dataframe_result = dict(dataframe_result or {})
        dataframe_result.setdefault("status", "failed")
        return {"message": "failed", "status": "failed", "session_id": session_id, "dataframe": dataframe_result}

    if not _ingest_dataframe_to_neo4j(session_id, entity):
        return {"message": "failed", "status": "failed", "session_id": session_id, "detail": "neo4j_ingestion_failed"}

    summary = _analysis_summary(session_id, entity)
    status = "flagged" if summary and summary.get("flagged_nodes", 0) > 0 else "clean"
    return {
        "message": "success",
        "status": "success",
        "session_id": session_id,
        "frontend_session_id": data.get("frontend_session_id"),
        "metadata": {"message": "success", "status": status, "summary": summary},
        "relationships": _relationship_panel_payload(session_id, entity, status),
        "dataframe": dataframe_result,
    }
