import eventlet
import eventlet.wsgi
eventlet.monkey_patch()

from flask import Flask, request, jsonify, session, render_template, current_app
from flask_socketio import SocketIO, emit

import os
import ipaddress
import socket
from urllib.parse import urlparse
from werkzeug.utils import secure_filename
import time
import shutil
from flask_cors import CORS
from kafka import KafkaConsumer #Kafka consumer
import pandas as pd
from datetime import datetime, timedelta
import random
import threading
import py_compile
import uuid

from globals import create_file,save_uploaded_file,save_temp_config,load_temp_config,_session_store
from connection_utils import kafka_broker, rest_api, HDFSstorage, tools

from batch_manager.batch_data_manager import batch_data_manager
from batch_manager.config_defaults import get_default_session_config
from batch_manager.services.dataframe_workflow import create_dataframe_response
from batch_manager.processing.realtime_source_loader import load_latest_kafka_message, load_realtime_api, load_kafka_batch_messages
from batch_manager.utils.schema_utils import align_schemas
from batch_manager.utils.postgres_utils import check_postgres_connection
from batch_manager.utils.artifact_utils import ensure_artifact_dir, register_artifact, register_artifact_dir
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.processing.rules_validator import validate_rules_json
from batch_manager.processing.rules_compiler import generate_python_rule, normalize_rule_key
from batch_manager.analyzing.LA_graphs_script import fetch_graph
from batch_manager.analyzing.analyzer import analyzer
from logger import log_writer,log_stream_background
from io_sockets import register_socket_handlers
from api.STR_link_analysis import STR_link_analysis_api
from api.ai_service import ai_service_api
from session_config_store import create_session_config, duplicate_window_config, get_user_config, get_workspace_layout, save_user_config, save_workspace_layout
from service_orchestration import enqueue_cleanup_run, enqueue_worker_job, get_active_session_lock, get_any_active_actor_lock, get_worker_job, list_cleanup_audit, public_lock_state, reactivate_analysis_session, request_session_cancellation
from auth.decorators import auth_required, current_actor_from_request, permission_required
from auth.repository import actor_has_permission, bind_analysis_session_actor, can_access_analysis_session_actor, get_postgres_connection
from auth.routes import auth_api
from security.payload_validation import (
    COMMON_SCHEMAS,
    PayloadValidationError,
    validate_json_payload,
    validate_payload,
    validate_uploaded_files,
    validated_json,
)
import globals #Globally used by multible pages (functions and variables) #Contains the front end url



app = Flask(__name__)
allowed_origins = os.getenv("LINKX_CORS_ORIGINS", "")
cors_origins = [origin.strip() for origin in allowed_origins.split(",") if origin.strip()]
if not cors_origins:
    if os.getenv("LINKX_ALLOW_WILDCARD_CORS", "").lower() in {"1", "true", "yes", "on"}:
        cors_origins = "*"
    else:
        raise RuntimeError("LINKX_CORS_ORIGINS must be configured; set LINKX_ALLOW_WILDCARD_CORS=true only for local development")
CORS(app, origins=cors_origins)  # Allow configured clients
app.secret_key = os.getenv("LINKX_FLASK_SECRET_KEY")
if not app.secret_key or app.secret_key == "dev-only-change-me":
    if os.getenv("LINKX_ALLOW_INSECURE_DEV_SECRET", "").lower() not in {"1", "true", "yes", "on"}:
        raise RuntimeError("LINKX_FLASK_SECRET_KEY must be set to a strong production secret")
    app.secret_key = "dev-only-change-me"
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("LINKX_MAX_UPLOAD_BYTES", "104857600"))
socketio = SocketIO(app, cors_allowed_origins=cors_origins, async_mode="eventlet") #Socket listners are found inside 'logger.py' page
# Register socket
register_socket_handlers(socketio)
# Register auth API blueprint
app.register_blueprint(auth_api, url_prefix="/auth")
# Register external API blueprint
app.register_blueprint(STR_link_analysis_api, url_prefix="/api")
app.register_blueprint(ai_service_api, url_prefix="/ai")


@app.after_request
def apply_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", os.getenv("LINKX_FRAME_OPTIONS", "DENY"))
    response.headers.setdefault("Referrer-Policy", os.getenv("LINKX_REFERRER_POLICY", "no-referrer"))
    response.headers.setdefault("Permissions-Policy", os.getenv("LINKX_PERMISSIONS_POLICY", "camera=(), microphone=(), geolocation=()"))
    csp = os.getenv("LINKX_CONTENT_SECURITY_POLICY")
    if csp:
        response.headers.setdefault("Content-Security-Policy", csp)
    if os.getenv("LINKX_ENABLE_HSTS", "").lower() in {"1", "true", "yes", "on"}:
        response.headers.setdefault("Strict-Transport-Security", os.getenv("LINKX_HSTS_VALUE", "max-age=31536000; includeSubDomains"))
    if request.path.startswith("/auth/"):
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("Pragma", "no-cache")
    return response


@app.errorhandler(413)
def request_entity_too_large(_exc):
    return jsonify({"message": "payload_too_large"}), 413


@app.errorhandler(500)
def internal_server_error(exc):
    current_app.logger.exception("unhandled API error")
    return jsonify({"message": "internal_server_error"}), 500


def _permission_denied(permission):
    return jsonify({"message": "forbidden", "permission": permission}), 403


def _require_permission(permission):
    actor = current_actor_from_request()
    if not actor:
        return jsonify({"message": "unauthorized"}), 401
    if not actor_has_permission(actor, permission):
        return _permission_denied(permission)
    return None



def _async_worker_jobs_enabled():
    return str(os.getenv("LINKX_ASYNC_WORKER_JOBS", "true")).lower() not in {"0", "false", "no"}


def _async_search_jobs_enabled():
    return _async_worker_jobs_enabled() and str(os.getenv("LINKX_ASYNC_SEARCH_JOBS", "false")).lower() in {"1", "true", "yes"}


def _new_session_log_file(session_id):
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"logfile_{session_id}_[{current_time}].log"


def _validation_error_response(exc):
    body = {"message": "validation_error", "detail": exc.message}
    if exc.field:
        body["field"] = exc.field
    return jsonify(body), 400


_LOCK_EXEMPT_PATHS = {
    "/auth/lock",
    "/auth/unlock",
    "/auth/idle-timeout",
    "/auth/logout",
    "/auth/session-policy",
    "/auth/login",
    "/auth/me",
    "/auth/verify",
    "/db/health",
}

_LOCK_PROTECTED_PREFIXES = (
    "/api/",
    "/auth/admin/",
)

_LOCK_PROTECTED_PATHS = {
    "/account/configuration",
    "/configuration",
    "/init",
    "/init_source",
    "/connect_to_source",
    "/disconnect_source",
    "/connect_to_tool",
    "/disconnect_tool",
    "/close_source_window",
    "/live_batch_files",
    "/upload_batch_files",
    "/graph_link",
    "/get_graph",
    "/admin/audit/cleanup",
    "/auth/preferences",
    "/workspace/layout",
}


def _is_lock_protected_request(path):
    if path in _LOCK_PROTECTED_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in _LOCK_PROTECTED_PREFIXES)


def _first_session_like_value(payload):
    if not isinstance(payload, dict):
        return None
    for key in ("session_id", "source_id", "run_id"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    nested = payload.get("value")
    if isinstance(nested, dict):
        for key in ("session_id", "source_id", "run_id", "window_id"):
            value = nested.get(key)
            if value not in (None, ""):
                return str(value)
    return None


def _request_session_id_for_lock():
    data = request.get_json(silent=True) if request.is_json else None
    session_id = _first_session_like_value(data)
    if session_id:
        return session_id
    for key in ("session_id", "source_id", "run_id", "window_id"):
        value = request.values.get(key)
        if value not in (None, ""):
            return str(value)
    return None


@app.before_request
def enforce_locked_session():
    if request.method == "OPTIONS":
        return None

    path = request.path.rstrip("/") or "/"
    if path in _LOCK_EXEMPT_PATHS or not _is_lock_protected_request(path):
        return None

    actor = current_actor_from_request()
    if not actor:
        return None

    try:
        session_id = _request_session_id_for_lock()
        lock = get_active_session_lock(session_id, actor=actor) if session_id else None
        actor_lock = get_any_active_actor_lock(actor=actor)
        if not lock:
            lock = actor_lock
    except Exception as exc:
        current_app.logger.exception("session lock check failed")
        return jsonify({
            "message": "lock_state_unavailable",
            "error": "Unable to verify session lock state.",
            "detail": "lock_state_check_failed",
        }), 503

    if lock:
        return jsonify({
            "message": "session_locked",
            "error": "Session is locked. Unlock required.",
            "lock": public_lock_state(lock),
        }), 423

    return None


def _network_host(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"//{raw}")
    return (parsed.hostname or raw.split(":", 1)[0]).strip().lower()


def _network_target_allowed(value):
    host = _network_host(value)
    if not host:
        return False, "missing_host"
    allowed_hosts = {h.strip().lower() for h in os.getenv("LINKX_ALLOWED_CONNECT_HOSTS", "").split(",") if h.strip()}
    if allowed_hosts and host not in allowed_hosts:
        return False, "host_not_allowed"
    try:
        addresses = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False, "host_resolution_failed"
    for info in addresses:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_unspecified:
            return False, "unsafe_host_address"
    return True, None


def _reject_unsafe_network_target(value):
    allowed, reason = _network_target_allowed(value)
    if allowed:
        return None
    return jsonify({"status": "error", "message": "Connection rejected by security policy.", "detail": reason}), 400


def _normalize_neo4j_url(url):
    value = str(url or "").strip()
    if value.startswith("neo4j://"):
        value = "bolt://" + value[len("neo4j://"):]
    if not value.startswith("bolt://"):
        return value
    remainder = value[len("bolt://"):]
    authority, separator, path = remainder.partition("/")
    if ":" not in authority:
        authority = f"{authority}:7687"
    return f"bolt://{authority}{separator}{path}"


def _parent_session_id(session_id):
    raw = str(session_id or "")
    if "_" not in raw:
        return None
    _, parent = raw.split("_", 1)
    return parent or None

def _is_spark_df(df):
    return "pyspark.sql.dataframe.DataFrame" in str(type(df))


def _dataframe_info_from_df(df, session_id):
    if df is None:
        return None

    path_to_save = ensure_artifact_dir("dfparts")
    if isinstance(df, pd.DataFrame):
        num_rows = len(df)
        columns = list(df.columns)
        merge_pandas_and_save([df], path_to_save, session_id)
    elif _is_spark_df(df):
        num_rows = df.count()
        columns = df.columns
        merge_spark_and_save([df], path_to_save, session_id)
    else:
        return None

    return {
        "columns": columns,
        "num_columns": len(columns),
        "num_rows": num_rows,
        "storage_url": load_temp_config("active_storage_address", session_id),
        "broker_url": load_temp_config("active_kafka_adress", session_id),
        "api_url": load_temp_config("active_REST_API", session_id),
        "topic": load_temp_config("active_kafka_topic", session_id),
        "tool": load_temp_config("active_tool", session_id),
        "actions": ["Store data", "Source / Target Relationship", "Link Analysis"],
        "rules": load_temp_config("rule_names", session_id),
    }


def _source_connected_response(df, session_id, message="Connection established!"):
    info = _dataframe_info_from_df(df, session_id)
    if info is None:
        return jsonify({'status': 'warning', 'message': 'Connection established, but no latest message was found.'}), 200
    return jsonify({'status': 'success', 'message': message, 'results': info}), 200


def _is_kafka_batch_topic(topic):
    topic_name = str(topic or "")
    return topic_name.endswith(".batches") or topic_name.endswith("batches")


def _parse_json_object(value):
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return value
    try:
        import json
        parsed = json.loads(stripped)
        return parsed
    except (TypeError, ValueError):
        return value


def _normalize_configuration(config):
    raw = config or {}
    if isinstance(raw, dict) and isinstance(raw.get("data"), dict):
        raw = raw.get("data") or {}
    normalized = {}
    for key, value in dict(raw).items():
        normalized[key] = _parse_json_object(value)

    active_tool_protocol = normalized.get("active_tool_protocol") or normalized.get("active_tool_url") or ""
    normalized.setdefault("active_tool_url", active_tool_protocol)
    normalized.setdefault("active_tool_protocol", active_tool_protocol)
    normalized.setdefault("tools", [])
    normalized.setdefault("tool_databases", [])
    normalized.setdefault("active_tool_tables", [])
    normalized.setdefault("active_tool_database", "")
    normalized.setdefault("custom_tool_database", "")
    normalized.setdefault("tool_protocol_port", "")
    normalized.setdefault("tool_web_port", "")
    normalized.setdefault("active_tool_username", "")
    normalized.setdefault("active_tool_password", "")
    normalized.setdefault("large_search_backend", "hive")
    normalized.setdefault("elastic_scroll_enabled", False)
    normalized.setdefault("elastic_scroll_limit", normalized.get("dataframes_limit", 1000000))
    normalized.setdefault("elastic_scroll_batch_size", 10000)

    if normalized.get("active_tool") and normalized["active_tool"] not in normalized["tools"]:
        normalized["tools"] = [normalized["active_tool"], *[tool for tool in normalized["tools"] if tool != normalized["active_tool"]]]
    active_db = normalized.get("active_tool_database") or normalized.get("custom_tool_database")
    if active_db and active_db not in normalized["tool_databases"]:
        normalized["tool_databases"] = [active_db, *[db for db in normalized["tool_databases"] if db != active_db]]
    return normalized


def _configuration_success(config, extra=None):
    normalized = _normalize_configuration(config)
    results = {"configuration": normalized}
    if extra:
        results.update(extra)
    return jsonify({
        "message": "success",
        "results": results,
        "configurations": normalized,
    }), 200

def _configuration_payload(data):
    if not isinstance(data, dict):
        return {}
    nested = data.get("configuration") or data.get("config") or data.get("data")
    if isinstance(nested, dict):
        payload = dict(nested)
        for key, value in data.items():
            if key in {"id", "session_id", "rule_name", "configuration", "config", "data"}:
                continue
            payload[key] = value
        return payload
    return {
        key: value
        for key, value in data.items()
        if key not in {"id", "session_id", "rule_name"}
    }


def _clean_id(value):
    raw = str(value or "").strip()
    return "" if raw.lower() in {"", "none", "null", "undefined"} else raw


def _source_id_from_graph_payload(data):
    source_id = _clean_id(data.get("source_id"))
    if source_id:
        return source_id
    session_id = _clean_id(data.get("session_id"))
    window_id = _clean_id(data.get("window_id"))
    if session_id and window_id:
        if session_id.startswith(f"{window_id}_"):
            return session_id
        return f"{window_id}_{session_id}"
    return session_id


def _analysis_session_exists(session_id, parent_session_id=None):
    if not session_id:
        return False
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            if parent_session_id is None:
                cur.execute(
                    "SELECT 1 FROM analysis_sessions WHERE session_id = %s",
                    (str(session_id),),
                )
            else:
                cur.execute(
                    "SELECT 1 FROM analysis_sessions WHERE session_id = %s AND parent_session_id = %s",
                    (str(session_id), str(parent_session_id)),
                )
            return cur.fetchone() is not None


def _graph_accessible_source(source_id, actor):
    if not source_id or not actor:
        return False

    parent_id = _parent_session_id(source_id)
    if parent_id:
        if not _analysis_session_exists(source_id, parent_session_id=parent_id):
            return False
        return can_access_analysis_session_actor(source_id, actor) or can_access_analysis_session_actor(parent_id, actor)

    return _analysis_session_exists(source_id) and can_access_analysis_session_actor(source_id, actor)



@app.route('/jobs/<job_id>', methods=['GET'])
@auth_required
@permission_required("session:read")
def worker_job_status(job_id):
    actor = current_actor_from_request()
    job = get_worker_job(job_id)
    if not job:
        return jsonify({"message": "not_found"}), 404
    session_id = job.get("session_id")
    if session_id and not can_access_analysis_session_actor(session_id, actor):
        return jsonify({"message": "forbidden"}), 403
    return jsonify({"message": "success", "results": job}), 200


@app.route('/admin/audit/cleanup', methods=['GET'])
@permission_required("users:manage")
def admin_cleanup_audit():
    filters = {
        "session_id": request.args.get("session_id"),
        "cleanup_type": request.args.get("cleanup_type"),
        "status": request.args.get("status"),
        "limit": request.args.get("limit"),
        "offset": request.args.get("offset"),
    }
    try:
        result = list_cleanup_audit(filters)
        return jsonify({"message": "success", "results": result}), 200
    except Exception as exc:
        current_app.logger.warning("cleanup audit query failed: %s", exc)
        return jsonify({"message": "audit_query_failed"}), 500


@app.route('/admin/cleanup/session', methods=['POST'])
@permission_required("users:manage")
@validate_json_payload(COMMON_SCHEMAS["admin_cleanup_session"])
def admin_cleanup_session():
    data = validated_json()
    session_id = str(data.get("session_id") or "").strip()
    run_id = data.get("run_id")
    reason = data.get("reason") or "admin_manual_cleanup"
    dry_run = bool(data.get("dry_run", False))
    cleanup_type = data.get("cleanup_type")

    if not cleanup_type:
        if run_id:
            cleanup_type = "run"
        elif "_" in session_id:
            cleanup_type = "window"
        else:
            cleanup_type = "session_tree"

    payload = {
        "event": "admin_manual_cleanup",
        "cleanup_targets": ["neo4j", "artifacts"],
        "preserve_session_config": bool(data.get("preserve_session_config", cleanup_type in {"window", "run"})),
    }

    try:
        cleanup_id = enqueue_cleanup_run(
            cleanup_type,
            session_id=session_id,
            run_id=run_id,
            reason=reason,
            neo4j_credentials=load_temp_config("tool_credentials", session_id),
            payload=payload,
            dry_run=dry_run,
        )
    except Exception as exc:
        current_app.logger.warning("admin cleanup enqueue failed: %s", exc)
        return jsonify({"message": "cleanup_enqueue_failed"}), 500

    return jsonify({
        "message": "success",
        "results": {
            "cleanup_id": cleanup_id,
            "cleanup_type": cleanup_type,
            "session_id": session_id,
            "run_id": str(run_id) if run_id is not None else None,
            "status": "queued",
            "dry_run": dry_run,
            "reason": reason,
        },
    }), 202


@app.route('/db/health', methods=['GET'])
def db_health():
    try:
        check_postgres_connection()
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        current_app.logger.warning("PostgreSQL health check failed: %s", e)
        return jsonify({'status': 'error'}), 500

@app.route('/workspace/layout', methods=['GET'])
@auth_required
def workspace_layout_load():
    actor = current_actor_from_request()
    session_id = str(request.args.get("session_id") or "").strip()
    if not session_id:
        return jsonify({"message": "validation_error", "detail": "session_id_required"}), 400
    if not can_access_analysis_session_actor(session_id, actor):
        return jsonify({"message": "forbidden"}), 403
    layout = get_workspace_layout(actor.get("id"), session_id)
    return jsonify({"message": "success", "results": {"session_id": session_id, "layout": layout}}), 200


@app.route('/workspace/layout', methods=['PUT'])
@auth_required
def workspace_layout_save():
    actor = current_actor_from_request()
    data = request.get_json(silent=True)
    if data is None or not isinstance(data, dict):
        return jsonify({"message": "validation_error", "detail": "json_object_required"}), 400
    session_id = str(data.get("session_id") or "").strip()
    layout = data.get("layout")
    if not session_id:
        return jsonify({"message": "validation_error", "detail": "session_id_required"}), 400
    if not isinstance(layout, dict):
        return jsonify({"message": "validation_error", "detail": "layout_object_required"}), 400
    if not can_access_analysis_session_actor(session_id, actor):
        return jsonify({"message": "forbidden"}), 403
    saved = save_workspace_layout(actor.get("id"), session_id, layout)
    return jsonify({"message": "success", "results": {"session_id": session_id, "layout": saved}}), 200


@app.route('/init', methods=['POST'])
@auth_required
@validate_json_payload(COMMON_SCHEMAS["init"])
def init():
    print("Initializing ....")
    data = validated_json()
    current_actor = current_actor_from_request()
    old_session = data.get('existing_session') or data.get('session_id')
    if old_session:
        old_session = str(old_session).strip()
        if old_session and bind_analysis_session_actor(old_session, current_actor):
            configs = load_temp_config("data", old_session)
            if configs is not None:
                normalized = _normalize_configuration(configs)
                return jsonify({
                    'message': 'success',
                    'results': {'session_id': old_session, 'configuration': normalized, 'reused_existing_session': True},
                    'configurations': normalized,
                }), 200
        current_app.logger.info("init existing_session unavailable; creating fresh session old_session=%s", old_session)

    try:
        max_value = 1000000
        min_value = 0
        session_id = random.randint(min_value, max_value - 1)
        configs = get_default_session_config(session_id)
        if not bind_analysis_session_actor(session_id, current_actor):
            return jsonify({'message': 'failed!', 'results': 'Could not bind session to user.'}), 500
        stored_new_configs = create_session_config(session_id, current_actor, default_config=configs)
        normalized = _normalize_configuration(stored_new_configs)
        return jsonify({'message': 'success', 'results': {'session_id': session_id, 'configuration': normalized, 'reused_existing_session': False}, 'configurations': normalized}), 200
    except Exception as e:
        print(e)
        return jsonify({'results': str(e), 'message': 'failed!'}), 200

@app.route('/account/configuration', methods=['GET'])
@auth_required
def account_configuration_load():
    actor = current_actor_from_request()
    if not actor or actor.get("actor_type") != "user":
        return jsonify({"message": "user_required"}), 403
    defaults = get_default_session_config(actor.get("id") or "default")
    config = get_user_config(actor.get("id"), default_config=defaults)
    return _configuration_success(config)


@app.route('/account/configuration', methods=['POST'])
@auth_required
def account_configuration_save():
    actor = current_actor_from_request()
    if not actor or actor.get("actor_type") != "user":
        return jsonify({"message": "user_required"}), 403
    raw_data = request.get_json(silent=True)
    if raw_data is None or not isinstance(raw_data, dict):
        return jsonify({'message': 'validation_error', 'detail': 'json_object_required'}), 400
    config = raw_data.get("configuration") or raw_data.get("config") or raw_data.get("data") or raw_data
    if not isinstance(config, dict):
        return jsonify({'message': 'validation_error', 'detail': 'config_object_required'}), 400
    normalized_config = _normalize_configuration(config)
    save_user_config(actor.get("id"), normalized_config)
    return _configuration_success(normalized_config)


@app.route('/configuration', methods=['POST'])
@auth_required
def configuration():
    data = {}
    files = {}
    if request.is_json:
        raw_data = request.get_json(silent=True)
        if raw_data is None or not isinstance(raw_data, dict):
            return jsonify({'message': 'validation_error', 'detail': 'json_object_required'}), 400
        try:
            data = validate_payload(raw_data, COMMON_SCHEMAS["configuration"])
        except PayloadValidationError as exc:
            return _validation_error_response(exc)
    else:
        data = request.form.to_dict() #Passed datas
        files = request.files.to_dict()  #Uploaded files -> FileStorage object
        files = {key: file for key, file in files.items() if file and file.filename}
        # If any fields are JSON-encoded strings, try parsing
        for key, value in list(data.items()):
            try:
                import json
                data[key] = json.loads(value)
            except (ValueError, TypeError):
                pass
        try:
            data = validate_payload(data, COMMON_SCHEMAS["configuration"])
        except PayloadValidationError as exc:
            return _validation_error_response(exc)

    session_id = data.get("session_id") or data.get("source_id")
    session_id = str(session_id or "").strip()
    if session_id.lower() in {"none", "null", "undefined"}:
        session_id = ""
    action = str(data.get("id") or "").strip().lower()
    if "load" in action:
        action = "load"
    elif "save" in action or "update" in action:
        action = "save"
    elif "remove" in action and "rule" in action:
        action = "remove_rule"

    required_permission = "config:read" if action == "load" else "config:write"
    denied = _require_permission(required_permission)
    if denied:
        return denied

    if action == "load":
        try:
            if session_id:
                config_data = load_temp_config("all", session_id)
            else:
                actor = current_actor_from_request()
                defaults = get_default_session_config(actor.get("id") if actor else "default")
                config_data = get_user_config(actor.get("id"), default_config=defaults) if actor and actor.get("actor_type") == "user" else defaults
            return _configuration_success(config_data)
        except Exception as e:
            return jsonify({'results': str(e), 'message': 'failed!'}), 200
    elif action == "save":
        print("Form fields:", data)
        #uploaded file
        if files:
            try:
                safe_files = validate_uploaded_files(list(files.values()), allowed_extensions={"json"}, max_files=5)
            except PayloadValidationError as exc:
                return _validation_error_response(exc)
            for file, filename, _ext in safe_files:
                print(f"Uploaded file: {filename}")
                #Check uploading folder exists
                upload_dir = ensure_artifact_dir("uploads", session_id)
                os.makedirs(upload_dir, exist_ok=True)
                #save upload into Temp folder
                file_path = os.path.join(upload_dir, f"{session_id}_{filename}")
                file.save(file_path)
                register_artifact(file_path, "rule", session_id=session_id, filename=filename, metadata={"source": "uploaded_rule_json"})
                #Validate rule (the uploaded rule)
                try:
                    rule_json = validate_rules_json(file_path)
                    if rule_json:
                        print("The rule is valid:", filename)
                        uploaded_rule_name = rule_json.get("rule_name") or filename.rsplit(".", 1)[0]
                        rule_name = data.get("rule_name", "").strip() or uploaded_rule_name
                        rule_key = normalize_rule_key(rule_name)
                        rule_file_name = f"{rule_key}_rules"

                        # Save Python version of rule
                        rules_dir = ensure_artifact_dir("rules", session_id)
                        os.makedirs(rules_dir, exist_ok=True)
                        output_py = os.path.join(rules_dir, f"{rule_file_name}.py")
                        generate_python_rule(rule_json, output_py)
                        py_compile.compile(output_py, doraise=True)
                        register_artifact(output_py, "rule", session_id=session_id, filename=os.path.basename(output_py), metadata={"source": "compiled_rule"})

                        # Register rule into configuration
                        print("Rule uploaded", session_id)
                        config = load_temp_config("all", session_id)
                        config_dict = config.get("data", {}) or {}

                        # Ensure lists exist
                        config_dict.setdefault("rule_names", [])
                        config_dict.setdefault("rule_file_names", [])

                        # Avoid duplicates
                        if rule_name not in config_dict["rule_names"]:
                            config_dict["rule_names"].append(rule_name)
                        if rule_file_name not in config_dict["rule_file_names"]:
                            config_dict["rule_file_names"].append(rule_file_name)

                        # Activate the new rule
                        config_dict["active_rule"] = [rule_name]

                        # Merge back into configuration
                        save_temp_config("all", config_dict, session_id)

                        return _configuration_success(config_dict)
                    else:
                        print("The rule is invalid")
                        return jsonify({'results': "Invalid rule file.", 'message': 'failed!'}), 200
                except Exception as e:
                    print(f"Failed to upload rule: {e}")
                    return jsonify({'results': str(e), 'message': 'failed!'}), 200
        if session_id:
            config = load_temp_config("all", session_id)
            config_dict = config.get("data", {}) if config else {}
        else:
            actor = current_actor_from_request()
            defaults = get_default_session_config(actor.get("id") if actor else "default")
            config_dict = get_user_config(actor.get("id"), default_config=defaults) if actor and actor.get("actor_type") == "user" else defaults
        incoming_config = _configuration_payload(data)
        if incoming_config:
            for key, value in incoming_config.items():
                if key == "active_rule":
                    config_dict[key] = value if isinstance(value, list) else [value]
                else:
                    config_dict[key] = value
            config_dict = _normalize_configuration(config_dict)
            if session_id:
                save_temp_config("all", config_dict, session_id)
            else:
                actor = current_actor_from_request()
                if actor and actor.get("actor_type") == "user":
                    save_user_config(actor.get("id"), config_dict)
        return _configuration_success(config_dict)
    elif action == "remove_rule":
        rule_name = str(data.get("rule_name") or "").strip()
        if not rule_name:
            return jsonify({'results': "No rule selected.", 'message': 'failed!'}), 400

        config = load_temp_config("all", session_id)
        config_dict = config.get("data", {}) if config else {}
        rule_names = list(config_dict.get("rule_names") or [])
        rule_file_names = list(config_dict.get("rule_file_names") or [])

        if rule_name not in rule_names:
            return jsonify({'results': f"Rule '{rule_name}' not found.", 'message': 'failed!'}), 404

        index = rule_names.index(rule_name)
        removed_file_name = rule_file_names[index] if index < len(rule_file_names) else f"{normalize_rule_key(rule_name)}_rules"
        config_dict["rule_names"] = [name for name in rule_names if name != rule_name]
        config_dict["rule_file_names"] = [
            name for idx, name in enumerate(rule_file_names)
            if idx != index and name != removed_file_name
        ]

        active_rule = config_dict.get("active_rule") or []
        if isinstance(active_rule, str):
            active_rule = [active_rule]
        if rule_name in active_rule:
            config_dict["active_rule"] = [config_dict["rule_names"][0]] if config_dict["rule_names"] else []

        session_rules_dir = ensure_artifact_dir("rules", session_id)
        removed_paths = []
        candidate = os.path.join(session_rules_dir, f"{removed_file_name}.py")
        if os.path.isfile(candidate):
            os.remove(candidate)
            removed_paths.append(candidate)

        pycache_dir = os.path.join(session_rules_dir, "__pycache__")
        if os.path.isdir(pycache_dir):
            pyc_prefix = f"{removed_file_name}."
            for filename in os.listdir(pycache_dir):
                if filename.startswith(pyc_prefix) and filename.endswith(".pyc"):
                    pyc_path = os.path.join(pycache_dir, filename)
                    os.remove(pyc_path)
                    removed_paths.append(pyc_path)

        save_temp_config("all", config_dict, session_id)
        return _configuration_success(config_dict, extra={'removed_rule': rule_name, 'removed_files': removed_paths})
    else:
        print("Unknown action:", data)
        return jsonify({'results': "unknown action", 'message': 'failed!'}), 400

@app.route('/init_source', methods=['POST'])
@auth_required
@permission_required("source:create")
@validate_json_payload(COMMON_SCHEMAS["init_source"])
def init_source():
    print("Initializing source window....")
    data = validated_json()
    active_session = data.get('session_id')
    window_id = data.get('window_id')
    current_actor = current_actor_from_request()
    if not current_actor:
        return jsonify({'message': 'unauthorized'}), 401
    child_session_id = f"{window_id}_{active_session}"
    try:
        if not bind_analysis_session_actor(child_session_id, current_actor, parent_session_id=str(active_session)):
            return jsonify({'results': "Could not bind source window session to user.", 'message': 'failed!'}), 500

        copied_config = duplicate_window_config(active_session, window_id)
        if copied_config is not None:
            return jsonify({'message': 'success'}), 200

        config_folder = "public/temp_config"
        file_path = f'{config_folder}/{window_id}_{active_session}_temp_config.json'
        if os.path.isfile(file_path):
            return jsonify({'message': 'success'}), 200
        file_path = f'{config_folder}/{active_session}_temp_config.json'
        if os.path.isfile(file_path):
            duplicated_file = os.path.join(config_folder, f"{window_id}_{active_session}_temp_config.json")
            shutil.copyfile(file_path, duplicated_file)
            return jsonify({'message': 'success'}), 200
        return jsonify({'results': "Base session config not found", 'message': 'failed!'}), 404
    except Exception as e:
        print(e)
        return jsonify({'results': str(e), 'message': 'failed!'}), 200

@app.route('/connect_to_source', methods=['POST'])
@auth_required
@permission_required("source:connect")
@validate_json_payload(COMMON_SCHEMAS["connect_to_source"])
def connect_to_source():
    data = validated_json() or {}
    address_type = data.get('addressType') or data.get('type')
    address = data.get('address') or data.get('broker') or data.get('broker_url') or data.get('api') or data.get('url')
    storage = data.get('storage') or data.get('hdfs') #passed hdfs_ip:port
    topic = data.get('topic') or data.get('kafka_topic')
    session_id = data.get('session_id') or data.get('source_id')
    source_mode = data.get('source_mode') or data.get('mode')

    target_to_check = storage or address
    if target_to_check:
        blocked = _reject_unsafe_network_target(target_to_check)
        if blocked:
            return blocked

    if topic and address_type == "api" and not str(address or "").startswith(("http://", "https://")):
        address_type = "broker"

    if not address_type:
        if topic:
            address_type = 'broker'
        elif data.get('api') or str(address or '').startswith(('http://', 'https://')):
            address_type = 'api'
        elif data.get('broker') or address:
            address_type = 'broker'

    if address_type == "broker":
        if not address:
            return jsonify({'status': 'error', 'message': 'Connection failed! Missing broker address.'}), 400
        if kafka_broker("check", address, session_id, topic=topic) is True:
            print("broker verified")
            save_temp_config("active_source_type", "broker", session_id)
            save_temp_config("active_source_mode", "batch" if _is_kafka_batch_topic(topic) else "realtime", session_id)
            save_temp_config("dataframe_ready", False, session_id)
            if topic:
                try:
                    if _is_kafka_batch_topic(topic):
                        df = load_kafka_batch_messages(address, topic, session_id, max_messages=200, max_rows=1000)
                    else:
                        df = load_latest_kafka_message(address, topic, session_id)
                    return _source_connected_response(df, session_id)
                except Exception as e:
                    print(f"[Kafka latest message error] {e}")
                    return jsonify({'status': 'warning', 'message': 'Broker connected, but latest message could not be loaded.'}), 200
            return jsonify({'status': 'success', 'message': 'Connection established!'}), 200
        return jsonify({'status': 'error', 'message': 'Connection failed!'}), 200

    elif address_type == "api":
        if not address:
            return jsonify({'status': 'error', 'message': 'Connection failed! Missing API address.'}), 400
        if rest_api("check", address, session_id) is True:
            print("api verified")
            save_temp_config("active_source_type", "api", session_id)
            save_temp_config("active_source_mode", "batch" if source_mode == "batch" else "realtime", session_id)
            save_temp_config("dataframe_ready", False, session_id)
            try:
                df = load_realtime_api(address, session_id)
                return _source_connected_response(df, session_id)
            except Exception as e:
                print(f"[API latest message error] {e}")
                return jsonify({'status': 'warning', 'message': 'API connected, but latest message could not be loaded.'}), 200
        return jsonify({'status': 'error', 'message': 'Connection failed!'}), 200

    elif storage:
        webhdfs_port = str(load_temp_config("storage_webhdfs_port", session_id) or load_temp_config("hadoop_web_port", session_id) or "9870")
        if ":" in storage:
            source_port = storage.split(":", 1)[1]
            if source_port != webhdfs_port:
                return jsonify({'status': 'Warning', 'message': f'Connection failed! WebHDFS must use port {webhdfs_port}.'}), 200
        else:
            storage = f"{storage}:{webhdfs_port}"

        if HDFSstorage("check", storage, session_id) is True:
            return jsonify({'status': 'success', 'message': 'Connection established!'}), 200
        return jsonify({'status': 'Warning', 'message': 'Connection failed! No storage found.'}), 200

    else:
        return jsonify({'status': 'error', 'message': 'Connection failed!'}), 400

@app.route('/disconnect_source', methods=['POST'])
@auth_required
@permission_required("source:disconnect")
@validate_json_payload(COMMON_SCHEMAS["disconnect_source"])
def disconnect_source():
    data = validated_json() or {}
    session_id = data.get('session_id') or data.get('source_id') or data.get('window_id')
    if session_id is not None:
        session_id = str(session_id)
    if not session_id:
        return jsonify({'status': 'error', 'message': 'Disconnecting failed!', 'detail': 'session_id_required'}), 400

    source_type = str(data.get('addressType') or data.get('type') or data.get('source_type') or '').lower()
    address = data.get('address') or data.get('value') or data.get('api') or data.get('url')
    broker = data.get('broker') or data.get('broker_url')
    hdfs = data.get('hdfs') or data.get('storage')

    if source_type == 'broker' and not broker:
        broker = address
    if source_type in {'hdfs', 'storage'} and not hdfs:
        hdfs = address

    try:
        disconnected = False
        if broker:
            disconnected = kafka_broker("disconnect", broker, session_id) is True or disconnected
        if hdfs:
            disconnected = HDFSstorage("disconnect", hdfs, session_id) is True or disconnected
        if source_type == 'api' or data.get('api') or data.get('url'):
            disconnected = rest_api("disconnect", address or '', session_id) is True or disconnected

        # Disconnect is intentionally idempotent: clear active source state even if
        # the client no longer knows which concrete source type was connected.
        save_temp_config("active_source_type", "", session_id)
        save_temp_config("active_source_mode", "", session_id)
        save_temp_config("active_REST_API", "", session_id)
        save_temp_config("active_kafka_adress", "", session_id)
        save_temp_config("active_kafka_topic", "", session_id)
        save_temp_config("active_storage_address", "", session_id)
        save_temp_config("dataframe_ready", False, session_id)

        cleanup_id = None
        tool_credentials = load_temp_config("tool_credentials", session_id)
        try:
            cleanup_id = enqueue_cleanup_run(
                "window",
                session_id=session_id,
                reason="source_disconnected",
                neo4j_credentials=tool_credentials if isinstance(tool_credentials, dict) else None,
                payload={"cleanup_targets": ["neo4j", "artifacts"], "event": "disconnect_source"},
            )
        except Exception as exc:
            print(f"[cleanup] failed to enqueue window cleanup for {session_id}: {exc}")

        response = {'status': 'success', 'message': 'Disconnected!'}
        if cleanup_id:
            response['cleanup_id'] = cleanup_id
        return jsonify(response), 200
    except Exception as e:
        print(e)
        return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 500

@app.route('/connect_to_tool', methods=['POST'])
@auth_required
@permission_required("graph:create")
@validate_json_payload(COMMON_SCHEMAS["connect_to_tool"])
def connect_to_tool():
    data = validated_json()
    tool_name = data.get('tool_name')
    url = data.get('url')
    username = data.get('username')
    password = data.get('password')
    session_id = data.get('source_id')
    database = data.get('database') or load_temp_config("active_tool_database", session_id)
    if tool_name == "neo4j":
        url = _normalize_neo4j_url(url)
    if url:
        blocked = _reject_unsafe_network_target(url)
        if blocked:
            return blocked
    payload = {"url": url, "username": username, "password": password, "session_id": session_id}
    if database:
        payload["database"] = database
    if url and username and password:
        if tools(tool_name, "connect", payload) is True:
            parent_session_id = _parent_session_id(session_id)
            if parent_session_id:
                parent_payload = {**payload, "session_id": parent_session_id}
                save_temp_config("tool", tool_name, parent_session_id)
                save_temp_config("tool_credentials", parent_payload, parent_session_id)
                if database:
                    save_temp_config("active_tool_database", database, parent_session_id)
            return jsonify({'status': 'success', 'message': 'Connected!', 'url': url}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Not connected!'}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Not connected!'}), 400

@app.route('/disconnect_tool', methods=['POST'])
@auth_required
@permission_required("graph:create")
@validate_json_payload(COMMON_SCHEMAS["disconnect_tool"])
def disconnect_tool():
    data = validated_json()
    session_id = data.get('source_id')
    tool_name = data.get('tool_name')
    payload={"session_id":session_id}
    if session_id:
        tool_credentials = load_temp_config("tool_credentials", session_id)
        if tools(tool_name,"disconnect",payload) is True:
            cleanup_id = None
            try:
                cleanup_id = enqueue_cleanup_run(
                    "window",
                    session_id=session_id,
                    reason="tool_disconnected",
                    neo4j_credentials=tool_credentials if isinstance(tool_credentials, dict) else None,
                    payload={"cleanup_targets": ["neo4j"], "event": "disconnect_tool", "tool_name": tool_name},
                )
            except Exception as exc:
                print(f"[cleanup] failed to enqueue tool cleanup for {session_id}: {exc}")
            response = {'status': 'success', 'message': 'Disconnected!'}
            if cleanup_id:
                response['cleanup_id'] = cleanup_id
            return jsonify(response), 200
        else:
            return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 400

def _source_window_session_id(data):
    source_id = data.get('source_id')
    if source_id:
        return str(source_id)
    session_id = data.get('session_id')
    window_id = data.get('window_id')
    if session_id is None:
        return None
    session_id = str(session_id)
    if "_" in session_id:
        return session_id
    if window_id is None or str(window_id) == "":
        return session_id
    return f"{window_id}_{session_id}"


@app.route('/close_source_window', methods=['POST'])
@auth_required
@permission_required("source:disconnect")
@validate_json_payload(COMMON_SCHEMAS["close_source_window"])
def close_source_window():
    data = validated_json() or {}
    session_id = _source_window_session_id(data)
    if not session_id:
        return jsonify({'status': 'error', 'message': 'missing_session_id'}), 400

    tool_credentials = load_temp_config("tool_credentials", session_id)
    cleanup_id = None
    try:
        cleanup_id = enqueue_cleanup_run(
            "window",
            session_id=session_id,
            run_id=data.get("run_id"),
            reason=data.get("reason") or "source_window_closed",
            neo4j_credentials=tool_credentials if isinstance(tool_credentials, dict) else None,
            payload={
                "cleanup_targets": ["neo4j", "artifacts"],
                "event": "close_source_window",
                "base_session_id": str(data.get('session_id')) if data.get('session_id') is not None else None,
                "window_id": str(data.get('window_id')) if data.get('window_id') is not None else None,
            },
        )
    except Exception as exc:
        print(f"[cleanup] failed to enqueue close_source_window cleanup for {session_id}: {exc}")
        return jsonify({'status': 'error', 'message': 'cleanup_enqueue_failed'}), 500

    return jsonify({
        'status': 'success',
        'message': 'cleanup_queued',
        'session_id': session_id,
        'cleanup_id': cleanup_id,
    }), 202


@app.route('/upload_batch_files', methods=['POST'])
@auth_required
@permission_required("batch:upload")
def upload_batch_files():
    if 'file' not in request.files:
        return jsonify({"message": "No file part in the request"}), 400
    try:
        form_data = validate_payload({"session_id": request.form.get("session_id")}, COMMON_SCHEMAS["upload_batch_files"])
        safe_files = validate_uploaded_files(
            request.files.getlist('file'),
            allowed_extensions={"csv", "json", "parquet", "xlsx"},
            max_files=25,
        )
    except PayloadValidationError as exc:
        return _validation_error_response(exc)

    session_id = form_data["session_id"]
    upload_folder = ensure_artifact_dir("uploads", session_id)

    # Create info file
    create_file(upload_folder, "info", "txt", "This directory is used for temporary uploads.")
    # Save session path in config (assuming you have this function)
    save_temp_config("files_storage_path", upload_folder, session_id)

    saved_files = []
    for file, filename, _ext in safe_files:
        saved_path = save_uploaded_file(file, upload_folder, filename_prefix=session_id, session_id=session_id)
        if not saved_path:
            return jsonify({"message": "Failed to save file"}), 500
        saved_name = os.path.basename(saved_path)
        register_artifact(saved_path, "upload", session_id=session_id, filename=saved_name)
        saved_files.append({
            "original_name": filename,
            "saved_name": saved_name,
            "path": saved_path,
        })

    return jsonify({"message": "success", "results": {"session_id": session_id, "files": saved_files}}), 200

@app.route('/live_batch_files', methods=['POST'])
@auth_required
@validate_json_payload(COMMON_SCHEMAS["live_batch_files"])
def live_batch_files():
    data = validated_json()
    print("1:",data)
    action_id = data.get('id')
    session_id = data.get('session_id')
    if not action_id or not session_id:
        return jsonify({'results': None, 'message': 'Missing action_id or session_id'}), 400

    permission_by_action = {
        "search": "batch:query",
        "create_DF": "batch:upload",
        "stream": "analysis:run",
        "end_session": "analysis:run",
    }
    required_permission = permission_by_action.get(action_id)
    if required_permission:
        denied = _require_permission(required_permission)
        if denied:
            return denied

    # -----------------------------
    # SEARCH HDFS / HYBRID
    # -----------------------------
    if action_id == "search":
        value = data.get("value", {})
        storage_ip = load_temp_config("active_storage_address",session_id)
        payload = {
            "id": "search",
            "keyword": value.get("keyword", ""),
            "date": value.get("date") or None,
            "offset": value.get("offset", 0),
            "limit": value.get("limit", 50),
            "search_column": value.get("search_column", "transactionid"), #falback to 'transaction id'
            "hybrid": value.get("hybrid", False),
            "strict": value.get("strict_mood", False),
            "storage": storage_ip,
            "session_id": data.get("session_id"),
        }
        if _async_search_jobs_enabled():
            job = enqueue_worker_job(
                "search",
                "search",
                session_id=session_id,
                payload=payload,
                priority=70,
                max_attempts=1,
            )
            return jsonify({
                "message": "success",
                "results": {
                    "accepted": True,
                    "status": "queued",
                    "job_id": job["job_id"],
                    "job": job,
                    "session_id": session_id,
                    "queue": "search",
                    "poll_url": f"/jobs/{job['job_id']}",
                },
            }), 202

        # delegate working logic to batch_data_manager
        result = batch_data_manager(payload)
        if result is None:
            return jsonify({
                "results": 0,
                "has_more": False,
                "offset": 0,
                "limit": 0,
                "message": "No results!"
            }), 200            
        # main.py handles returning the JSON
        return jsonify({
            "results": result.get("results") or [],
            "has_more": result.get("has_more") or False,
            "offset": result.get("offset") or 0,
            "limit": result.get("limit") or 0,
            "message": result.get("message", "")
        }), 200


    # -----------------------------
    # CREATE DATAFRAME / LOAD FILES
    # -----------------------------
    if action_id == "create_DF":
        print("data", data)
        print("kindkindkind:", data.get("kind", ""))
        if _async_worker_jobs_enabled():
            reactivation = reactivate_analysis_session(session_id)
            if reactivation.get("reactivated"):
                print("create_DF session reactivated:", reactivation)
            payload = dict(data)
            payload.setdefault("id", "create_DF")
            payload["session_id"] = session_id
            job = enqueue_worker_job(
                "dataframe",
                "create_DF",
                session_id=session_id,
                payload=payload,
                priority=60,
                max_attempts=1,
            )
            return jsonify({
                "message": "success",
                "results": {
                    "accepted": True,
                    "status": "queued",
                    "job_id": job["job_id"],
                    "job": job,
                    "session_id": session_id,
                    "queue": "dataframe",
                    "poll_url": f"/jobs/{job['job_id']}",
                },
            }), 202
        return create_dataframe_response(data, session_id)
    # -----------------------------
    # START SESSION
    # -----------------------------
    elif action_id == "stream":
        values = data.get("value") or {}
        if not isinstance(values, dict):
            return jsonify({'results': None, 'message': 'Invalid stream value'}), 400
        values.setdefault("session_id", session_id)
        values.setdefault("source_mode", data.get("source_mode") or data.get("mode"))
        values.setdefault("mode", data.get("mode") or data.get("source_mode"))
        if data.get("listen_realtime") is True:
            values.setdefault("listen_realtime", True)
        if data.get("use_dataframe") is True:
            values.setdefault("use_dataframe", True)

        if _async_worker_jobs_enabled():
            reactivation = reactivate_analysis_session(session_id)
            if reactivation.get("reactivated"):
                print("stream session reactivated:", reactivation)
            run_id = uuid.uuid4().hex
            log_file = _new_session_log_file(session_id)
            payload = dict(values)
            payload.update({
                "id": "start_session",
                "session_id": session_id,
                "run_id": run_id,
                "log_file": log_file,
                "run_inline": True,
            })
            job = enqueue_worker_job(
                "analysis",
                "start_session",
                session_id=session_id,
                run_id=run_id,
                payload=payload,
                priority=50,
                max_attempts=1,
            )
            print("stream queued:", job)
            return jsonify({
                'results': log_file,
                'message': 'success',
                'job': job,
                'job_id': job['job_id'],
                'status': 'queued',
                'queued': True,
            }), 202

        # Legacy fallback: run inside API memory when explicitly disabled.
        payload = {"id": "create_session", "session_id": session_id}
        session = batch_data_manager(payload)
        if session is True:
            values["id"] = "start_session"
            stream = batch_data_manager(values)
            if stream is not None:
                print("stream:", stream)
                return jsonify({'results': stream, 'message': 'success'}), 200
            return jsonify({'results': stream, 'message': 'failed!'}), 400
        return jsonify({'results': session, 'message': 'failed!'}), 400

    # -----------------------------
    # END SESSION
    # -----------------------------
    elif action_id == "end_session":
        if _async_worker_jobs_enabled():
            result = request_session_cancellation(
                session_id,
                reason="client_end_session",
                requested_by="api:live_batch_files",
                neo4j_credentials=load_temp_config("tool_credentials", session_id),
                cancel_session=False,
            )
            return jsonify({
                "message": "success",
                "results": {
                    "status": "stopping" if result.get("jobs") else "not_running",
                    "session_id": session_id,
                    "cancellation_requested": bool(result.get("jobs")),
                    "preserve_session": True,
                    "orchestration": result,
                },
            }), 202
        payload = {"id": "end_session", "session_id": session_id}
        result = batch_data_manager(payload)
        return jsonify(result), 200

    # -----------------------------
    # INVALID ACTION
    # -----------------------------
    else:
        return jsonify({'results': None, 'error': f'Invalid action: {action_id}'}), 400

@app.route('/graph_link', methods=['POST'])
@auth_required
@permission_required("graph:link")
@validate_json_payload(COMMON_SCHEMAS["graph_link"])
def graph_link():
    data = validated_json()
    action = data.get('id')
    actor = current_actor_from_request()
    source_id = _source_id_from_graph_payload(data)
    graph_window_id = _clean_id(data.get("graph_window_id")) or None

    if not source_id:
        return jsonify({"message": "validation_error", "detail": "source_id_or_session_window_required"}), 400
    if not _graph_accessible_source(source_id, actor):
        return jsonify({"message": "forbidden"}), 403

    session_info = _session_store.get(source_id)
    str_report_status = globals.str_report_status_registry.get(str(source_id), {})
    linked = action == "link"

    return jsonify({
        "message": "success",
        "results": {
            "linked": linked,
            "source_id": source_id,
            "graph_session_id": source_id,
            "graph_window_id": graph_window_id,
            "status_available": bool(session_info or str_report_status),
        },
    }), 200


@app.route('/get_graph', methods=['POST'])
@auth_required
@permission_required("graph:read")
@validate_json_payload(COMMON_SCHEMAS["get_graph"])
def get_graph():
    data = validated_json()
    action = data.get('id')
    actor = current_actor_from_request()
    source_id = _source_id_from_graph_payload(data)

    if not source_id:
        return jsonify({"message": "validation_error", "detail": "source_id_or_session_window_required"}), 400
    if not _graph_accessible_source(source_id, actor):
        return jsonify({"message": "forbidden"}), 403

    if action == "relationship":
        try:
            if _async_worker_jobs_enabled():
                payload = {
                    "id": action,
                    "source_id": source_id,
                    "relationship": data["relationship"],
                    "session_id": source_id,
                }
                job = enqueue_worker_job(
                    "graph",
                    "graph_fetch",
                    session_id=source_id,
                    payload=payload,
                    priority=45,
                    max_attempts=1,
                )
                return jsonify({
                    "message": "success",
                    "results": {
                        "accepted": True,
                        "status": "queued",
                        "job_id": job["job_id"],
                        "job": job,
                        "source_id": source_id,
                        "graph_session_id": source_id,
                        "relationship": data["relationship"],
                        "queue": "graph",
                        "poll_url": f"/jobs/{job['job_id']}",
                    },
                }), 202

            graph = fetch_graph(action, "generate", source_id, data["relationship"], "html")
            if isinstance(graph, tuple):
                return graph
            graph["file"] = "graphs_template"
            return jsonify({
                "message": "success",
                "results": {
                    **graph,
                    "source_id": source_id,
                    "graph_session_id": source_id,
                    "relationship": data["relationship"],
                    "graph": graph,
                },
            }), 200
        except Exception as e:
            current_app.logger.exception('graph fetch failed')
            return jsonify({'message': 'failed!', 'error': 'graph_fetch_failed'}), 500

    return jsonify({'results': "", 'message': 'failed!'}), 200


if __name__ == "__main__":
    #socketio.run(app, host="0.0.0.0", port=8000, debug=True)
    port = int(os.getenv("PORT", "8100"))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
