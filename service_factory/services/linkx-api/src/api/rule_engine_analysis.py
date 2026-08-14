import os
from uuid import uuid4

from flask import Blueprint, jsonify, request

from batch_manager.analyzing.analyzer import analyzer, rule_to_node_label
from batch_manager.analyzing.LA_rules_script import TRANSACTION_RELATIONSHIPS
from batch_manager.config_defaults import get_default_session_config
from batch_manager.services.dataframe_workflow import create_dataframe_response
from batch_manager.utils.artifact_utils import ensure_artifact_dir
from batch_manager.utils.elastic_utils import es_keyword_search
from batch_manager.utils.notification_utils import emit_status_payload, emit_str_report_link_analysis
from connection_utils import tools
from globals import create_file, load_temp_config, save_temp_config
from security.payload_validation import COMMON_SCHEMAS, validate_json_payload, validated_json
from security.redaction import redact_value
from service_orchestration import enqueue_worker_job


RULE_link_analysis_api = Blueprint("RULE_link_analysis_api", __name__)


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
    except Exception:
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
    if not _run_analyzer(link_payload, "rule engine link analysis"):
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
        if not _run_analyzer(relationship_payload, "rule engine source target relationship"):
            return False

    return True


def _success_response(session_id, entity, frontend_session_id=None):
    open_payload = {
        "message": "success",
        "session_id": session_id,
        "wait_for_prepare": False,
        "socket_emit": [],
    }
    emit_str_report_link_analysis(open_payload, frontend_session_id or session_id)
    return open_payload


@RULE_link_analysis_api.route("/RULE_link_analysis", methods=["POST"])
@RULE_link_analysis_api.route("/rule_engine_analysis", methods=["POST"])
@RULE_link_analysis_api.route("/rule_engine/link_analysis", methods=["POST"])
@RULE_link_analysis_api.route("/rule_engine/suspicious_transactions", methods=["POST"])
@validate_json_payload(COMMON_SCHEMAS["rule_engine_analysis"], error_message="failed!", include_detail=False)
def rule_engine_link_analysis():
    rule_api_key = os.getenv("LINKX_RULE_ENGINE_API_KEY") or os.getenv("LINKX_PUBLIC_API_KEY")
    if rule_api_key and request.headers.get("X-API-Key") != rule_api_key:
        return jsonify({'message': 'unauthorized'}), 401

    data = validated_json()

    entity = str(data.get("entity", "")).strip().lower()
    type_value = str(data.get("type", "")).strip().lower()
    value = str(data.get("value", "")).strip()

    # Ensure session ID uses the required 'rule_engine' prefix
    raw_id = str(data.get("rule_id") or data.get("session_id") or "")
    if raw_id:
        session_id = raw_id if raw_id.startswith("rule_engine") else f"rule_engine_{raw_id}"
    else:
        session_id = f"rule_engine_{uuid4().hex}"

    if not entity or not type_value or not value:
        return jsonify({'message': 'failed!'}), 400

    if entity == "bank" and type_value == "account_number":
        if not _prepare_session(session_id):
            return jsonify({'message': 'failed!'}), 400

        if _async_worker_jobs_enabled():
            payload = {
                "entity": entity,
                "type": type_value,
                "value": value,
                "session_id": session_id,
                "rule_id": session_id,
                "frontend_session_id": data.get("frontend_session_id"),
                "date": data.get("date"),
                "suspicious_reason": data.get("suspicious_reason"),
                "rule_name": data.get("rule_name"),
            }
            job = enqueue_worker_job(
                "analysis",
                "rule_engine_analysis",
                session_id=session_id,
                payload=payload,
                priority=40,
                max_attempts=3,
            )
            return jsonify({
                "message": "accepted",
                "session_id": session_id,
                "wait_for_prepare": True,
                "results": {
                    "job_id": job["job_id"],
                    "session_id": session_id,
                    "status": "queued",
                    "queue": "analysis",
                },
            }), 202

        # Synchronous fallback path
        id_val = "search"
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
        API_URL = os.getenv("LINKX_PUBLIC_ES_API_URL") or os.getenv("LINKX_STR_ELASTIC_API_URL")

        if not API_URL:
            elastic_base_url = _config_value(session_id, "elastic_api_base_url")
            if elastic_base_url:
                API_URL = f"{str(elastic_base_url).rstrip('/')}/{api_search_endpoint}"
            else:
                API_URL = f"http://{storage_address}:{api_port}/{api_search_endpoint}"

        response = es_keyword_search(id_val, API_URL, keyword, search_column, strict_mood, date_column, date, auth_header=load_temp_config("elastic_api_authorization", session_id))

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
