import os
from uuid import uuid4

from flask import Blueprint, jsonify, request

from batch_manager.analyzing.analyzer import analyzer, rule_to_node_label
from batch_manager.config_defaults import get_default_session_config
from batch_manager.analyzing.LA_rules_script import TRANSACTION_RELATIONSHIPS
from connection_utils import tools
from batch_manager.services.dataframe_workflow import create_dataframe_response
from batch_manager.utils.elastic_utils import es_keyword_search
from batch_manager.utils.notification_utils import emit_status_payload, emit_str_report_link_analysis
from globals import create_file, load_temp_config, save_temp_config
from batch_manager.utils.artifact_utils import ensure_artifact_dir
from security.payload_validation import COMMON_SCHEMAS, validate_json_payload, validated_json
from service_orchestration import enqueue_worker_job
from security.redaction import redact_value


STR_link_analysis_api = Blueprint("STR_link_analysis_api", __name__)


def _async_worker_jobs_enabled():
    return str(os.getenv("LINKX_ASYNC_WORKER_JOBS", "true")).lower() not in {"0", "false", "no"}


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
    if port and "://" in url and not url.rsplit(":", 1)[-1].isdigit():
        return f"{url}:{port}"
    return url


def _neo4j_credentials(session_id):
    credentials = load_temp_config("tool_credentials", session_id)
    if credentials:
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
    try:
        return analyzer(payload) is True
    except Exception as e:
        return False


def _ingest_dataframe_to_neo4j(session_id, entity):
    if entity == "bank":
        rule = "bank transactions"
    else:
        return False

    credentials = _neo4j_credentials(session_id)
    if not credentials:
        return False

    link_payload = _base_analyzer_payload(session_id, credentials)
    link_payload.update({
        "action": "Link Analysis",
        "rule": rule,
    })
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

    driver = tools("neo4j", "check", {"session_id": session_id})
    if not driver:
        return None

    node_label = rule_to_node_label("bank transactions", session_id)
    safe_label = f"`{str(node_label).replace('`', '')}`"

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
    except Exception as e:
        return None


def _source_target_relationship_panel_item(entity, session_id):
    source_target = _source_target_relationship(entity, session_id)
    if not source_target:
        return None

    relationship_type = source_target["relationship"]
    return {
        "id": f"rel_{relationship_type.lower()}",
        "type": relationship_type,
        "textcolor": "#ffffff",
        "bgcolor": "#750b8c",
    }


def _relationship_panel_payload(session_id, entity, status):
    relationships = []
    source_target_item = _source_target_relationship_panel_item(entity, session_id)
    if source_target_item:
        relationships.append(source_target_item)

    if entity != "bank" or status == "clean":
        return relationships

    driver = tools("neo4j", "check", {"session_id": session_id})
    if not driver:
        return relationships

    try:
        with driver.session() as session:
            result = session.run("""
            MATCH ()-[r]->()
            WHERE r.session_id = $session_id AND type(r) IN $relationship_types
            WITH type(r) AS type, collect(r)[0] AS rep
            RETURN
                elementId(rep) AS id,
                type,
                coalesce(rep.textcolor, '#111827') AS textcolor,
                coalesce(rep.bgcolor, '#e5e7eb') AS bgcolor
            ORDER BY type
            """, session_id=session_id, relationship_types=TRANSACTION_RELATIONSHIPS)
            relationships.extend([
                {
                    "id": record.get("id") or f"rel_{str(record.get('type')).lower()}",
                    "type": record.get("type"),
                    "textcolor": record.get("textcolor") or "#111827",
                    "bgcolor": record.get("bgcolor") or "#e5e7eb",
                }
                for record in result
            ])
            return relationships
    except Exception as e:
        return relationships


def _success_response(session_id, entity, frontend_session_id=None):
    summary = _analysis_summary(session_id, entity)
    status = "flagged" if summary and summary.get("flagged_nodes", 0) > 0 else "clean"
    open_payload = {
        "message": "success",
        "session_id": session_id,
        "wait_for_prepare": False,
        "socket_emit": [],
    }
    metadata = {
        "message": "success",
        "status": status,
    }
    if summary:
        metadata["summary"] = summary

    metadata_payload = {
        "session_id": session_id,
        "type": "metadata",
        "data": metadata,
    }
    relationships_payload = {
        "session_id": session_id,
        "type": "relationships",
        "data": _relationship_panel_payload(session_id, entity, status),
    }

    emit_str_report_link_analysis(open_payload, frontend_session_id or session_id)
    emit_status_payload(metadata_payload, frontend_session_id or session_id)
    emit_status_payload(relationships_payload, frontend_session_id or session_id)
    return open_payload


@STR_link_analysis_api.route("/STR_link_analysis", methods=["POST"])
@validate_json_payload(COMMON_SCHEMAS["str_link_analysis"], error_message="failed!", include_detail=False)
def STR_link_analysis():
    public_api_key = os.getenv("LINKX_PUBLIC_API_KEY")
    if public_api_key and request.headers.get("X-API-Key") != public_api_key:
        return jsonify({'message': 'unauthorized'}), 401

    data = validated_json()

    entity = str(data.get("entity", "")).strip().lower()
    type = str(data.get("type", "")).strip().lower()
    value = str(data.get("value", "")).strip()
    session_id = str(data.get("str_id") or data.get("session_id") or f"str_report_{uuid4().hex}")

    if not entity or not type or not value:
        return jsonify({'message': 'failed!'}), 400

    if entity == "bank":
        if type == "account_number":
            if not _prepare_session(session_id):
                return jsonify({'message': 'failed!'}), 400

            from auth.decorators import current_actor_from_request
            from auth.repository import bind_analysis_session_actor
            actor = current_actor_from_request()
            if actor and actor.get("id"):
                bind_analysis_session_actor(session_id, actor)

            id = "search"
            keyword = value
            search_column = "accountno"
            strict_mood = True
            date_column = _config_value(session_id, "date_column")
            date = data.get("date")

            storage_address = _config_value(session_id, "active_storage_host") or _config_value(session_id, "active_storage_address")
            if ":" in storage_address:
                storage_address = storage_address.split(":", 1)[0]

            api_port = _config_value(session_id, "api_port")
            api_search_endpoint = str(_config_value(session_id, "search_api_endpoint_es_strict")).strip("/")
            API_URL = os.getenv("LINKX_PUBLIC_ES_API_URL") or os.getenv("LINKX_STR_ELASTIC_API_URL")

            if not API_URL:
                elastic_base_url = _config_value(session_id, "elastic_api_base_url")
                if elastic_base_url:
                    API_URL = f"{str(elastic_base_url).rstrip('/')}/{api_search_endpoint}"
                else:
                    API_URL = f"http://{storage_address}:{api_port}/{api_search_endpoint}"

            response = es_keyword_search(id, API_URL, keyword, search_column, strict_mood, date_column, date, auth_header=load_temp_config("elastic_api_authorization", session_id))
        
            result_size = 0
            if response and isinstance(response, dict):
                results = response.get("results") or []
                if len(results) > 0:
                    result_size = int(results[0].get("size", 0) or 0)

            if result_size > 0:
                dataframe_payload = {
                    "id": "create_DF",
                    "session_id": session_id,
                    "kind": "hybrid",
                    "type": "array",
                    "date": date,
                    "value": [
                        {
                            "type": "elastic",
                            "column": search_column,
                            "keyword": keyword,
                            "strict": strict_mood,
                        }
                    ],
                }

                dataframe_response = create_dataframe_response(dataframe_payload, session_id)

                dataframe_status = getattr(dataframe_response, "status_code", None)
                if isinstance(dataframe_response, tuple) and len(dataframe_response) > 1:
                    dataframe_status = dataframe_response[1]

                if dataframe_status == 200:
                    if _ingest_dataframe_to_neo4j(session_id, entity):
                        return jsonify(_success_response(session_id, entity, data.get("frontend_session_id"))), 200
                    else:
                        return jsonify({'message': 'failed!'}), 200
                else:
                    return jsonify({'message': 'failed!'}), 200
            else:
                return jsonify({'message': 'Not found!'}), 200
        else:
            return jsonify({'message': 'failed!'}), 400
    else:
        return jsonify({'message': 'failed!'}), 400
