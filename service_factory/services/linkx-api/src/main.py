import eventlet
import eventlet.wsgi
eventlet.monkey_patch()

from flask import Flask, request, jsonify, session, render_template, current_app, g
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
from batch_manager.utils.neo4j_utils import Neo4jCredentialConfigError, neo4j_database_name, redacted_neo4j_credentials, resolve_neo4j_credentials
from batch_manager.utils.Classified_entities import TrustedEntitiesValidationError, normalize_risk_entities, normalize_trusted_entities
from logger import log_writer,log_stream_background
from io_sockets import register_socket_handlers
from api.STR_link_analysis import STR_link_analysis_api
from api.risk_scoring_api import risk_scoring_api
from api.risk_scoring_sync_api import risk_scoring_sync_api
from api.unified_risk_scoring_api import unified_risk_scoring_api
from api.ML_link_analysis import ML_link_analysis_api
from api.rule_engine_analysis import RULE_link_analysis_api
from api.ai_service import ai_service_api
from session_config_store import create_session_config, duplicate_window_config, get_user_config, get_workspace_layout, load_session_config, reset_user_config, save_session_config, save_user_config, save_workspace_layout
from service_orchestration import enqueue_cleanup_run, enqueue_worker_job, get_active_session_lock, get_actor_main_session_info, get_any_active_actor_lock, get_worker_job, list_cleanup_audit, public_lock_state, reactivate_analysis_session, request_session_cancellation
from auth.decorators import auth_required, current_actor_from_request, permission_required
from auth.repository import actor_has_permission, bind_analysis_session_actor, can_access_analysis_session_actor, get_postgres_connection, record_security_event
from auth.routes import auth_api, exchange_parent_oauth_code
from security.redaction import redact_value
from security.payload_validation import (
    COMMON_SCHEMAS,
    PayloadValidationError,
    validate_json_payload,
    validate_payload,
    validate_uploaded_files,
    validated_json,
)
from observability.metrics import (
    metrics_enabled,
    metrics_response,
    metrics_token,
    normalize_route,
    observe_request,
    request_in_progress_dec,
    request_in_progress_inc,
    request_started,
    should_track_request,
)
import globals #Globally used by multible pages (functions and variables) #Contains the front end url



app = Flask(__name__)

try:
    from observability.metrics import start_otel_metrics_server
    start_otel_metrics_server(8889)
except Exception as exc:
    print(f"API OTel metrics notice: {exc}", flush=True)

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
app.config["LINKX_MAX_JSON_BYTES"] = int(os.getenv("LINKX_MAX_JSON_BYTES", "2097152"))
socketio = SocketIO(app, cors_allowed_origins=cors_origins, async_mode="eventlet") #Socket listners are found inside 'logger.py' page
# Register socket
register_socket_handlers(socketio)
# Register auth API blueprint
app.register_blueprint(auth_api, url_prefix="/auth")
app.add_url_rule("/api/auth/exchange", view_func=exchange_parent_oauth_code, methods=["POST"])
# Register external API blueprint
app.register_blueprint(STR_link_analysis_api, url_prefix="/api")
app.register_blueprint(risk_scoring_api, url_prefix="/api/risk_scoring")
app.register_blueprint(risk_scoring_sync_api, url_prefix="/api/risk_scoring")
app.register_blueprint(unified_risk_scoring_api, url_prefix="/api/v1/risk-score")
app.register_blueprint(ML_link_analysis_api, url_prefix="/api")
app.register_blueprint(RULE_link_analysis_api, url_prefix="/api")
app.register_blueprint(ai_service_api, url_prefix="/ai")

from api.reports_api import reports_api
app.register_blueprint(reports_api)


@app.after_request
def apply_security_headers(response):
    if getattr(g, "_metrics_track_request", False):
        observe_request(g._metrics_method, g._metrics_route, response.status_code, g._metrics_started_at)
        request_in_progress_dec(g._metrics_method, g._metrics_route)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    frame_options = os.getenv("LINKX_FRAME_OPTIONS")
    if frame_options:
        response.headers.setdefault("X-Frame-Options", frame_options)
    response.headers.setdefault("Referrer-Policy", os.getenv("LINKX_REFERRER_POLICY", "no-referrer"))
    response.headers.setdefault("Permissions-Policy", os.getenv("LINKX_PERMISSIONS_POLICY", "camera=(), microphone=(), geolocation=()"))
    
    # CSP with support for Parent project iframe embedding
    csp = os.getenv("LINKX_CONTENT_SECURITY_POLICY")
    if not csp:
        # Build default CSP with Parent project support if origin is configured
        parent_origin = os.getenv("LINKX_PARENT_FRAME_ORIGIN", "")
        frame_ancestors = "'self'"
        if parent_origin:
            frame_ancestors = f"'self' {parent_origin}"
        csp = f"frame-ancestors {frame_ancestors}; default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline';"
    
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


@app.before_request
def enforce_request_body_size():
    if request.method == "OPTIONS":
        return None
    content_length = request.content_length
    if not content_length or content_length < 0:
        return None
    if request.is_json:
        max_json_bytes = int(os.getenv("LINKX_MAX_JSON_BYTES", app.config.get("LINKX_MAX_JSON_BYTES", 2097152)))
        if max_json_bytes > 0 and content_length > max_json_bytes:
            return jsonify({
                "message": "payload_too_large",
                "limit": "json",
                "max_bytes": max_json_bytes,
            }), 413
    return None


@app.errorhandler(500)
def internal_server_error(exc):
    current_app.logger.exception("unhandled API error")
    return jsonify({"message": "internal_server_error"}), 500


@app.before_request
def begin_request_metrics():
    if metrics_enabled():
        start_otel_metrics_server(8889)
    g._metrics_track_request = False
    if not should_track_request(request):
        return None
    g._metrics_track_request = True
    g._metrics_method = request.method
    g._metrics_route = normalize_route(request)
    g._metrics_started_at = request_started()
    request_in_progress_inc(g._metrics_method, g._metrics_route)
    return None


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


def _queued_worker_response(job, action_id, queue_name):
    return jsonify({
        "message": "success",
        "results": {
            "accepted": True,
            "status": "queued",
            "action": action_id,
            "job_id": job["job_id"],
            "job": job,
            "queue": queue_name,
            "poll_url": f"/jobs/{job['job_id']}",
        },
        "job": job,
        "job_id": job["job_id"],
        "status": "queued",
        "queued": True,
    }), 202

def _search_diagnostic_logs_enabled():
    return str(os.getenv("LINKX_SEARCH_DIAGNOSTIC_LOGS", "false")).lower() in {"1", "true", "yes", "on"}


def _new_session_log_file(session_id):
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"logfile_{session_id}_[{current_time}].log"


def _validation_error_response(exc):
    body = {"message": "validation_error", "detail": exc.message}
    if exc.field:
        body["field"] = exc.field
    return jsonify(body), 400


def _audit_context():
    return {
        "ip_address": request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip() or None,
        "user_agent": (request.headers.get("User-Agent") or "")[:512] or None,
    }


def _record_security_event_safe(event_type, **kwargs):
    try:
        ctx = _audit_context()
        kwargs.setdefault("ip_address", ctx["ip_address"])
        kwargs.setdefault("user_agent", ctx["user_agent"])
        return record_security_event(event_type, **kwargs)
    except Exception:
        current_app.logger.warning("security audit event failed event_type=%s", event_type, exc_info=True)
        return None


_LOCK_EXEMPT_PATHS = {
    "/auth/lock",
    "/auth/unlock",
    "/auth/idle-timeout",
    "/auth/logout",
    "/auth/login",
    "/auth/me",
    "/auth/verify",
    "/db/health",
    "/api/risk_scoring/analysis_request",
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
    "/graph_link",
    "/get_graph",
    "/admin/audit/cleanup",
    "/auth/preferences",
    "/auth/session-policy",
    "/workspace/layout",
}


def _is_lock_exempt_request(path, method):
    if path == "/auth/session-policy":
        return method == "GET"
    if path == "/api/risk_scoring/analysis_request" or path == "/api/risk_scoring/sync_analysis" or path.endswith("/risk_scoring/analysis_request") or path.endswith("/risk_scoring/sync_analysis") or path.startswith("/api/v1/risk-score"):
        return True
    return path in _LOCK_EXEMPT_PATHS


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
    if _is_lock_exempt_request(path, request.method) or not _is_lock_protected_request(path):
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
    normalized.setdefault("large_search_backend", "elastic_scroll")
    normalized.setdefault("elastic_scroll_enabled", False)
    normalized.setdefault("elastic_scroll_limit", normalized.get("dataframes_limit", 1000000))
    normalized.setdefault("elastic_scroll_batch_size", 10000)
    trusted_entities_value = normalized.get("trusted_entities")
    if trusted_entities_value in (None, "", []):
        trusted_entities_value = normalized.get("trusted_catalog")
    normalized["trusted_entities"] = normalize_trusted_entities(trusted_entities_value)
    normalized.pop("trusted_catalog", None)
    normalized["risk_entities"] = normalize_risk_entities(normalized.get("risk_entities"))

    if normalized.get("active_tool") and normalized["active_tool"] not in normalized["tools"]:
        normalized["tools"] = [normalized["active_tool"], *[tool for tool in normalized["tools"] if tool != normalized["active_tool"]]]
    active_db = normalized.get("active_tool_database") or normalized.get("custom_tool_database")
    if active_db and active_db not in normalized["tool_databases"]:
        normalized["tool_databases"] = [active_db, *[db for db in normalized["tool_databases"] if db != active_db]]
    active_storage = str(normalized.get("active_storage_address") or "").strip()
    storage_list = normalized.get("storage_addresses") or []
    if active_storage and active_storage not in storage_list:
        normalized["storage_addresses"] = [active_storage, *[a for a in storage_list if a != active_storage]]
    active_kafka = str(normalized.get("active_kafka_adress") or "").strip()
    kafka_list = normalized.get("kafka_addresses") or []
    if active_kafka and active_kafka not in kafka_list:
        normalized["kafka_addresses"] = [active_kafka, *[a for a in kafka_list if a != active_kafka]]

    active_topic = str(normalized.get("active_kafka_topic") or "").strip()
    topic_list = normalized.get("kafka_topics") or []
    if active_topic and active_topic not in topic_list:
        normalized["kafka_topics"] = [active_topic, *[t for t in topic_list if t != active_topic]]
    else:
        normalized.setdefault("kafka_topics", topic_list)
        
    for topic_key in [
        "kafka_risk_scoring_input_topic", 
        "kafka_risk_scoring_mapped_topic", 
        "kafka_risk_scoring_flagged_topic"
    ]:
        topic_val = str(normalized.get(topic_key) or "").strip()
        if topic_val and topic_val not in normalized["kafka_topics"]:
            normalized["kafka_topics"].append(topic_val)
    return normalized


SENSITIVE_CONFIG_KEY_PARTS = ("password", "secret", "token", "credential", "authorization", "x-api-key", "client_secret")


def _sensitive_config_paths(value, prefix=""):
    paths = []
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key or "")
            path = f"{prefix}.{key_text}" if prefix else key_text
            lowered = key_text.lower()
            if any(part in lowered for part in SENSITIVE_CONFIG_KEY_PARTS):
                paths.append(path)
            paths.extend(_sensitive_config_paths(item, path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            paths.extend(_sensitive_config_paths(item, f"{prefix}[{idx}]" if prefix else f"[{idx}]"))
    return paths


def _configuration_for_response(config):
    return redact_value(_normalize_configuration(config))


def _configuration_success(config, extra=None):
    normalized = _configuration_for_response(config)
    results = {"configuration": normalized}
    if extra:
        results.update(redact_value(extra))
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


def _preserve_runtime_connection_fields(existing_config, incoming_config):
    if not isinstance(incoming_config, dict):
        return incoming_config
    current = existing_config if isinstance(existing_config, dict) else {}
    sanitized = dict(incoming_config)

    existing_tool_credentials = current.get("tool_credentials")
    incoming_tool_credentials = sanitized.get("tool_credentials")
    if incoming_tool_credentials in (None, "", "***"):
        sanitized.pop("tool_credentials", None)
    elif isinstance(incoming_tool_credentials, dict):
        merged = dict(existing_tool_credentials) if isinstance(existing_tool_credentials, dict) else {}
        merged.update(incoming_tool_credentials)
        if merged.get("password") == "***":
            if isinstance(existing_tool_credentials, dict):
                if not merged.get("password_ref") and existing_tool_credentials.get("password_ref"):
                    merged["password_ref"] = existing_tool_credentials.get("password_ref")
                if not merged.get("url") and existing_tool_credentials.get("url"):
                    merged["url"] = existing_tool_credentials.get("url")
                if not merged.get("username") and existing_tool_credentials.get("username"):
                    merged["username"] = existing_tool_credentials.get("username")
                if not merged.get("database") and existing_tool_credentials.get("database"):
                    merged["database"] = existing_tool_credentials.get("database")
        sanitized["tool_credentials"] = merged
    else:
        sanitized.pop("tool_credentials", None)

    for key in ("active_tool_password", "active_tool_password_ref"):
        if sanitized.get(key) in (None, "", "***") and current.get(key) not in (None, ""):
            sanitized.pop(key, None)

    return sanitized


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


def _config_session_accessible(session_id, actor):
    if not session_id or not actor:
        return False
    parent_id = _parent_session_id(session_id)
    if parent_id:
        return can_access_analysis_session_actor(session_id, actor) or can_access_analysis_session_actor(parent_id, actor)
    return can_access_analysis_session_actor(session_id, actor)



@app.route('/jobs/<job_id>', methods=['GET'])
@auth_required
@permission_required("session:read")
def worker_job_status(job_id):
    actor = current_actor_from_request()
    include_chunks = str(request.args.get("include_chunks") or "").lower() in {"1", "true", "yes", "on"}
    job = get_worker_job(job_id, include_chunks=include_chunks, after_event_id=request.args.get("after_event_id"))
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
        _record_security_event_safe(
            "admin.cleanup.request",
            actor=current_actor_from_request(),
            target_type="cleanup_run",
            target_id=cleanup_id,
            session_id=session_id or None,
            success=True,
            metadata={"cleanup_type": cleanup_type, "dry_run": dry_run, "has_run_id": bool(run_id)},
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

@app.route('/metrics', methods=['GET'])
def prometheus_metrics():
    if not metrics_enabled():
        return jsonify({"message": "not_found"}), 404
    expected_token = metrics_token()
    if expected_token:
        presented_token = request.headers.get("X-Linkx-Metrics-Token", "").strip()
        if presented_token != expected_token:
            return jsonify({"message": "forbidden"}), 403
    return metrics_response()


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


def _session_rotation_seconds():
    try:
        return max(0, int(os.getenv("LINKX_SESSION_ROTATION_SECONDS", "43200")))
    except (TypeError, ValueError):
        return 43200


def _session_age_seconds(session_info):
    if not isinstance(session_info, dict):
        return None
    created_at = session_info.get("created_at")
    if not created_at:
        return None
    now = datetime.now(created_at.tzinfo) if getattr(created_at, "tzinfo", None) else datetime.utcnow()
    age = now - created_at
    return max(0, int(age.total_seconds()))


def _new_parent_session_id(current_actor, attempts=32):
    for _ in range(max(1, attempts)):
        session_id = random.randint(0, 999999)
        if bind_analysis_session_actor(session_id, current_actor):
            return session_id
    raise RuntimeError("session_id_allocation_failed")


def _load_reusable_parent_session(current_actor, requested_session=None):
    candidate_session = str(requested_session or "").strip()
    if candidate_session:
        if bind_analysis_session_actor(candidate_session, current_actor):
            configs = load_temp_config("data", candidate_session)
            if configs is not None:
                return {
                    "session_id": candidate_session,
                    "configuration": _normalize_configuration(configs),
                    "reused_existing_session": True,
                    "session_rotated": False,
                    "rotated_from_session": None,
                }
        current_app.logger.info("init existing_session unavailable; creating fresh session old_session=%s", candidate_session)

    session_info = get_actor_main_session_info(current_actor)
    if not session_info:
        return None

    active_session_id = str(session_info.get("session_id") or "").strip()
    if not active_session_id:
        return None

    age_seconds = _session_age_seconds(session_info)
    rotation_seconds = _session_rotation_seconds()
    if rotation_seconds > 0 and age_seconds is not None and age_seconds >= rotation_seconds:
        return {
            "session_id": _new_parent_session_id(current_actor),
            "configuration": None,
            "reused_existing_session": False,
            "session_rotated": True,
            "rotated_from_session": active_session_id,
        }

    if bind_analysis_session_actor(active_session_id, current_actor):
        configs = load_temp_config("data", active_session_id)
        if configs is not None:
            return {
                "session_id": active_session_id,
                "configuration": _normalize_configuration(configs),
                "reused_existing_session": True,
                "session_rotated": False,
                "rotated_from_session": None,
            }
    return None


@app.route('/init', methods=['POST'])
@auth_required
@validate_json_payload(COMMON_SCHEMAS["init"])
def init():
    data = validated_json()
    current_actor = current_actor_from_request()
    old_session = data.get('existing_session') or data.get('session_id')

    try:
        reusable = _load_reusable_parent_session(current_actor, old_session)
        if reusable and reusable.get("configuration") is not None:
            safe_config = redact_value(reusable['configuration'])
            response_results = {
                'session_id': reusable['session_id'],
                'configuration': safe_config,
                'reused_existing_session': reusable.get('reused_existing_session', False),
                'session_rotated': reusable.get('session_rotated', False),
            }
            if reusable.get('rotated_from_session'):
                response_results['rotated_from_session'] = reusable.get('rotated_from_session')
            return jsonify({
                'message': 'success',
                'results': response_results,
                'configurations': safe_config,
            }), 200

        session_id = reusable.get('session_id') if reusable else _new_parent_session_id(current_actor)
        session_id = str(session_id)
        rotation_source = reusable.get('rotated_from_session') if reusable else None
        configs = get_default_session_config(session_id)
        stored_new_configs = create_session_config(
            session_id,
            current_actor,
            default_config=configs,
            existing_session_id=rotation_source,
        )
        normalized = redact_value(_normalize_configuration(stored_new_configs))
        response_results = {
            'session_id': session_id,
            'configuration': normalized,
            'reused_existing_session': False,
            'session_rotated': bool(rotation_source),
        }
        if rotation_source:
            response_results['rotated_from_session'] = rotation_source
        return jsonify({'message': 'success', 'results': response_results, 'configurations': normalized}), 200
    except Exception as e:
        current_app.logger.warning("init failed: %s", e)
        return jsonify({'message': 'failed!', 'error': 'init_failed'}), 500

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
    try:
        normalized_config = _normalize_configuration(config)
    except TrustedEntitiesValidationError as exc:
        field_name = 'risk_entities' if 'risk_entities' in str(exc) else 'trusted_entities'
        return jsonify({'message': 'validation_error', 'detail': str(exc), 'field': field_name}), 400
    save_user_config(actor.get("id"), normalized_config)
    _record_security_event_safe(
        "config.user.save",
        actor=actor,
        target_type="user_config",
        target_id=actor.get("id"),
        success=True,
        metadata={"sensitive_paths": _sensitive_config_paths(normalized_config)},
    )
    return _configuration_success(normalized_config)


@app.route('/account/configuration/reset', methods=['POST'])
@auth_required
def account_configuration_reset():
    actor = current_actor_from_request()
    if not actor or actor.get("actor_type") != "user":
        return jsonify({"message": "user_required"}), 403
    defaults = get_default_session_config(actor.get("id") or "default")
    config = reset_user_config(actor.get("id"), default_config=defaults)
    _record_security_event_safe(
        "config.user.reset",
        actor=actor,
        target_type="user_config",
        target_id=str(actor.get("id")),
        success=True,
    )
    return _configuration_success(config)



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
    raw_action = str(data.get("id") or "").strip().lower()
    if raw_action in {"load_default", "load-default", "reset", "reset_default", "reset_defaults", "default"}:
        action = "load_default"
    elif "load" in raw_action:
        action = "load"
    elif "save" in raw_action or "update" in raw_action:
        action = "save"
    elif "remove" in raw_action and "rule" in raw_action:
        action = "remove_rule"

    required_permission = "config:read" if action in {"load", "load_default"} else "config:write"
    denied = _require_permission(required_permission)
    if denied:
        return denied

    if session_id:
        actor = current_actor_from_request()
        if not _config_session_accessible(session_id, actor):
            return jsonify({"message": "forbidden"}), 403

    if action == "load_default":
        try:
            actor = current_actor_from_request()
            defaults = get_default_session_config(session_id or (actor.get("id") if actor else "default"))
            if session_id:
                save_temp_config("data", defaults, session_id)
                defaults = load_temp_config("data", session_id) or defaults
            if actor and actor.get("actor_type") == "user":
                reset_user_config(actor.get("id"), default_config=defaults)
                if not session_id:
                    defaults = get_user_config(actor.get("id"), default_config=defaults)
            return _configuration_success(defaults)
        except Exception as e:
            current_app.logger.warning("configuration reset failed: %s", e)
            return jsonify({'message': 'failed!', 'error': 'configuration_reset_failed'}), 500
    elif action == "load":
        try:
            if session_id:
                config_data = load_temp_config("all", session_id)
                defaults = get_default_session_config(session_id)
                if not config_data or not isinstance(config_data, dict) or not config_data.get("data"):
                    save_temp_config("data", defaults, session_id)
                    config_data = {"data": load_temp_config("data", session_id) or defaults}
                else:
                    data_obj = dict(config_data.get("data") or {})
                    updated = False
                    for k in (
                        "storage_addresses",
                        "active_storage_address",
                        "active_storage_host",
                        "elastic_api_base_url",
                        "kafka_addresses",
                        "active_kafka_adress",
                        "kafka_bootstrap_servers",
                        "active_tool_protocol",
                    ):
                        if not data_obj.get(k) and defaults.get(k):
                            data_obj[k] = defaults[k]
                            updated = True
                    if updated:
                        config_data["data"] = data_obj
                        save_temp_config("data", data_obj, session_id)
            else:
                actor = current_actor_from_request()
                defaults = get_default_session_config(actor.get("id") if actor else "default")
                config_data = get_user_config(actor.get("id"), default_config=defaults) if actor and actor.get("actor_type") == "user" else defaults
            return _configuration_success(config_data)
        except Exception as e:
            current_app.logger.warning("configuration load failed: %s", e)
            return jsonify({'message': 'failed!', 'error': 'configuration_load_failed'}), 500
    elif action == "save":
        current_app.logger.info("configuration save requested session_id=%s fields=%s", session_id, redact_value(data))
        #uploaded file
        if files:
            try:
                safe_files = validate_uploaded_files(list(files.values()), allowed_extensions={"json"}, max_files=5)
            except PayloadValidationError as exc:
                return _validation_error_response(exc)
            for file, filename, _ext in safe_files:
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
                        return jsonify({'results': "Invalid rule file.", 'message': 'failed!'}), 200
                except Exception as e:
                    current_app.logger.warning("rule upload failed session_id=%s: %s", session_id, e)
                    return jsonify({'message': 'failed!', 'error': 'rule_upload_failed'}), 400
        if session_id:
            config = load_temp_config("all", session_id)
            config_dict = config.get("data", {}) if config else {}
        else:
            actor = current_actor_from_request()
            defaults = get_default_session_config(actor.get("id") if actor else "default")
            config_dict = get_user_config(actor.get("id"), default_config=defaults) if actor and actor.get("actor_type") == "user" else defaults
        incoming_config = _configuration_payload(data)
        if isinstance(incoming_config, dict) and "trusted_catalog" in incoming_config and "trusted_entities" not in incoming_config:
            incoming_config["trusted_entities"] = incoming_config.pop("trusted_catalog")
        incoming_config = _preserve_runtime_connection_fields(config_dict, incoming_config)
        if incoming_config:
            sensitive_paths = _sensitive_config_paths(incoming_config)
            if sensitive_paths:
                actor = current_actor_from_request()
                if not actor or not actor_has_permission(actor, "users:manage"):
                    current_app.logger.warning(
                        "sensitive configuration write denied session_id=%s actor=%s paths=%s",
                        session_id,
                        redact_value(actor),
                        sensitive_paths,
                    )
                    return _permission_denied("users:manage")
                current_app.logger.info(
                    "sensitive configuration updated session_id=%s actor=%s paths=%s",
                    session_id,
                    redact_value(actor),
                    sensitive_paths,
                )
                _record_security_event_safe(
                    "config.session.sensitive_update",
                    actor=actor,
                    target_type="session_config",
                    target_id=session_id,
                    session_id=session_id,
                    success=True,
                    metadata={"sensitive_paths": sensitive_paths},
                )
            for key, value in incoming_config.items():
                if key == "active_rule":
                    config_dict[key] = value if isinstance(value, list) else [value]
                else:
                    config_dict[key] = value
            try:
                config_dict = _normalize_configuration(config_dict)
            except TrustedEntitiesValidationError as exc:
                field_name = 'risk_entities' if 'risk_entities' in str(exc) else 'trusted_entities'
                return jsonify({'message': 'validation_error', 'detail': str(exc), 'field': field_name}), 400
            if session_id:
                save_temp_config("all", config_dict, session_id)
                config_dict = load_temp_config("data", session_id) or config_dict
                _record_security_event_safe(
                    "config.session.save",
                    actor=current_actor_from_request(),
                    target_type="session_config",
                    target_id=session_id,
                    session_id=session_id,
                    success=True,
                    metadata={"sensitive_paths": _sensitive_config_paths(incoming_config)},
                )
            else:
                actor = current_actor_from_request()
                if actor and actor.get("actor_type") == "user":
                    save_user_config(actor.get("id"), config_dict)
                    config_dict = get_user_config(actor.get("id"), default_config=config_dict)
                    _record_security_event_safe(
                        "config.user.save",
                        actor=actor,
                        target_type="user_config",
                        target_id=actor.get("id"),
                        success=True,
                        metadata={"sensitive_paths": _sensitive_config_paths(incoming_config)},
                    )
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
        current_app.logger.info("unknown configuration action fields=%s", redact_value(data))
        return jsonify({'results': "unknown action", 'message': 'failed!'}), 400

@app.route('/init_source', methods=['POST'])
@auth_required
@permission_required("source:create")
@validate_json_payload(COMMON_SCHEMAS["init_source"])
def init_source():
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
        current_app.logger.warning("init failed: %s", e)
        return jsonify({'message': 'failed!', 'error': 'init_failed'}), 500

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
                    return jsonify({'status': 'warning', 'message': 'Broker connected, but latest message could not be loaded.'}), 200
            return jsonify({'status': 'success', 'message': 'Connection established!'}), 200
        return jsonify({'status': 'error', 'message': 'Connection failed!'}), 200

    elif address_type == "api":
        if not address:
            return jsonify({'status': 'error', 'message': 'Connection failed! Missing API address.'}), 400
        if rest_api("check", address, session_id) is True:
            save_temp_config("active_source_type", "api", session_id)
            save_temp_config("active_source_mode", "batch" if source_mode == "batch" else "realtime", session_id)
            save_temp_config("dataframe_ready", False, session_id)
            try:
                df = load_realtime_api(address, session_id)
                return _source_connected_response(df, session_id)
            except Exception as e:
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
        except Exception:
            pass

        response = {'status': 'success', 'message': 'Disconnected!'}
        if cleanup_id:
            response['cleanup_id'] = cleanup_id
        return jsonify(response), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 500

def _log_connect_to_tool_validation_failure(detail, field=None, *, payload=None, session_id=None, source_id=None):
    payload = payload if isinstance(payload, dict) else {}
    current_app.logger.warning(
        "connect_to_tool validation failed endpoint=%s session_id=%s source_id=%s field=%s detail=%s database_empty=%s",
        request.path,
        redact_value(session_id or payload.get("session_id")),
        redact_value(source_id or payload.get("source_id")),
        field,
        detail,
        payload.get("database") == "",
    )


@app.route('/connect_to_tool', methods=['POST'])
@auth_required
@permission_required("graph:create")
def connect_to_tool():
    raw_data = request.get_json(silent=True)
    if raw_data is None:
        _log_connect_to_tool_validation_failure("json_body_required")
        return jsonify({'message': 'validation_error', 'detail': 'json_body_required', 'endpoint': request.path}), 400
    if not isinstance(raw_data, dict):
        _log_connect_to_tool_validation_failure("json_object_required")
        return jsonify({'message': 'validation_error', 'detail': 'json_object_required', 'endpoint': request.path}), 400
    try:
        data = validate_payload(raw_data, COMMON_SCHEMAS["connect_to_tool"])
    except PayloadValidationError as exc:
        _log_connect_to_tool_validation_failure(exc.message, field=exc.field, payload=raw_data)
        body = {'message': 'validation_error', 'detail': exc.message, 'endpoint': request.path}
        if exc.field:
            body['field'] = exc.field
            body['invalid_fields'] = [exc.field]
        return jsonify(body), 400

    tool_name = data.get('tool_name')
    source_id = str(data.get('source_id') or '').strip()
    session_id = str(data.get('session_id') or source_id).strip()
    current_app.logger.info(
        "connect_to_tool request tool=%s session_id=%s source_id=%s has_password=%s has_password_ref=%s database_empty=%s",
        redact_value(tool_name),
        redact_value(session_id),
        redact_value(source_id),
        bool(data.get('password')),
        bool(data.get('password_ref')),
        data.get('database') in (None, ''),
    )
    if session_id and source_id and session_id != source_id:
        detail = 'session_id_and_source_id_must_match_for_connect_to_tool'
        _log_connect_to_tool_validation_failure(detail, field='session_id', payload=data, session_id=session_id, source_id=source_id)
        return jsonify({
            'message': 'validation_error',
            'detail': detail,
            'field': 'session_id',
            'invalid_fields': ['session_id', 'source_id'],
            'endpoint': request.path,
        }), 400

    url = data.get('url')
    username = data.get('username')
    password = data.get('password')
    password_ref = data.get('password_ref')
    database_input = data.get('database')
    database = str(database_input).strip() if database_input is not None else ''
    stored_tool_credentials = load_temp_config("tool_credentials", source_id)
    if not isinstance(stored_tool_credentials, dict):
        stored_tool_credentials = {}
    if not database:
        database = neo4j_database_name(stored_tool_credentials) or ''
    if tool_name == "neo4j":
        url = _normalize_neo4j_url(url)
    if url:
        blocked = _reject_unsafe_network_target(url)
        if blocked:
            return blocked

    payload = {"url": url, "username": username, "password": password, "session_id": source_id}
    if database:
        payload["database"] = database
    if password_ref:
        payload["password_ref"] = password_ref

    if tool_name == "neo4j":
        stored_credentials = stored_tool_credentials
        if not isinstance(stored_credentials, dict):
            stored_credentials = {}
        validation_payload = dict(stored_credentials)
        validation_payload.update({"url": url, "username": username, "session_id": source_id})
        if database:
            validation_payload["database"] = database
        if password == "***":
            validation_payload["password"] = "***"
            validation_payload["password_ref"] = password_ref or stored_credentials.get("password_ref")
        else:
            validation_payload["password"] = password
            if password_ref:
                validation_payload["password_ref"] = password_ref
        try:
            resolved = resolve_neo4j_credentials(validation_payload)
        except Neo4jCredentialConfigError as exc:
            _log_connect_to_tool_validation_failure(str(exc), field='password', payload=data, session_id=session_id, source_id=source_id)
            return jsonify({
                'message': 'validation_error',
                'detail': str(exc),
                'field': 'password',
                'invalid_fields': ['password'],
                'endpoint': request.path,
            }), 400
        current_app.logger.info(
            "connect_to_tool credential source session_id=%s source_id=%s creds=%s",
            redact_value(session_id),
            redact_value(source_id),
            redacted_neo4j_credentials(resolved),
        )
        if password == "***" and validation_payload.get("password_ref"):
            payload["password_ref"] = validation_payload.get("password_ref")

    if url and username and password:
        current_app.logger.info(
            "connect_to_tool attempting connection tool=%s session_id=%s source_id=%s creds=%s",
            redact_value(tool_name),
            redact_value(session_id),
            redact_value(source_id),
            redacted_neo4j_credentials(payload) if tool_name == "neo4j" else None,
        )
        connected = tools(tool_name, "connect", payload) is True
        current_app.logger.info(
            "connect_to_tool connection result tool=%s session_id=%s source_id=%s connected=%s",
            redact_value(tool_name),
            redact_value(session_id),
            redact_value(source_id),
            connected,
        )
        if connected:
            if tool_name == "neo4j":
                persisted = save_session_config(
                    source_id,
                    {
                        "tool": tool_name,
                        "active_tool": tool_name,
                        "active_tool_database": database or "",
                        "tool_credentials": dict(payload),
                    },
                    merge=True,
                )
                persisted_config = load_session_config(source_id) if persisted else None
                persisted_credentials = (persisted_config or {}).get("tool_credentials") if isinstance(persisted_config, dict) else None
                current_app.logger.info(
                    "connect_to_tool persistence result session_id=%s source_id=%s persisted=%s has_config=%s creds=%s",
                    redact_value(session_id),
                    redact_value(source_id),
                    bool(persisted),
                    isinstance(persisted_config, dict),
                    redacted_neo4j_credentials(persisted_credentials) if isinstance(persisted_credentials, dict) else None,
                )
                if not persisted or not isinstance(persisted_credentials, dict) or not persisted_credentials.get("password_ref"):
                    current_app.logger.error(
                        "connect_to_tool canonical credential persistence failed session_id=%s source_id=%s persisted=%s creds=%s",
                        redact_value(session_id),
                        redact_value(source_id),
                        bool(persisted),
                        redacted_neo4j_credentials(persisted_credentials) if isinstance(persisted_credentials, dict) else None,
                    )
                    return jsonify({
                        'status': 'error',
                        'message': 'Not connected!',
                        'detail': 'neo4j_credential_persistence_failed',
                        'session_id': source_id,
                    }), 500
            else:
                save_temp_config("tool", tool_name, source_id)
                save_temp_config("active_tool_database", database or "", source_id)
            return jsonify({'status': 'success', 'message': 'Connected!', 'url': url, 'session_id': source_id}), 200
        return jsonify({'status': 'error', 'message': 'Not connected!', 'detail': 'neo4j_connection_failed'}), 200

    detail = 'missing_required_connection_fields'
    _log_connect_to_tool_validation_failure(detail, payload=data, session_id=session_id, source_id=source_id)
    return jsonify({'message': 'validation_error', 'detail': detail, 'endpoint': request.path}), 400

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
            except Exception:
                pass
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
    except Exception:
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
    current_app.logger.info("live_batch_files action=%s session_id=%s", data.get("id"), data.get("session_id"))
    action_id = data.get('id')
    session_id = data.get('session_id')
    if not action_id or not session_id:
        return jsonify({'results': None, 'message': 'Missing action_id or session_id'}), 400

    bind_analysis_session_actor(session_id, current_actor_from_request())

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
        storage_ip = load_temp_config("active_storage_address", session_id)
        payload = {
            "id": "search",
            "keyword": value.get("keyword", ""),
            "date": value.get("date") or None,
            "offset": value.get("offset", 0),
            "limit": value.get("limit", 50),
            "search_column": value.get("search_column", "transactionid"),
            "hybrid": value.get("hybrid", False),
            "strict": value.get("strict_mood", False),
            "storage": storage_ip,
            "session_id": data.get("session_id"),
        }
        if _async_worker_jobs_enabled():
            job = enqueue_worker_job(
                "search",
                "search",
                session_id=session_id,
                payload=payload,
                priority=60,
                max_attempts=1,
            )
            current_app.logger.info("search queued session_id=%s job_id=%s", session_id, job.get("job_id"))
            return _queued_worker_response(job, "search", "search")

        result = batch_data_manager(payload)
        if result is None:
            return jsonify({
                "results": 0,
                "has_more": False,
                "offset": 0,
                "limit": 0,
                "message": "No results!"
            }), 200
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
        current_app.logger.info("create_DF requested session_id=%s kind=%s type=%s", session_id, data.get("kind", ""), data.get("type", ""))
        if _async_worker_jobs_enabled():
            payload = dict(data)
            payload.setdefault("session_id", session_id)
            job = enqueue_worker_job(
                "dataframe",
                "create_DF",
                session_id=session_id,
                payload=payload,
                priority=55,
                max_attempts=1,
            )
            current_app.logger.info("create_DF queued session_id=%s job_id=%s", session_id, job.get("job_id"))
            return _queued_worker_response(job, "create_DF", "dataframe")
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

        stream_tool = values.get("tool") or load_temp_config("active_tool", session_id) or load_temp_config("tool", session_id)
        if str(stream_tool or "").lower() == "neo4j":
            try:
                stream_credentials = load_temp_config("tool_credentials", session_id)
                if not isinstance(stream_credentials, dict):
                    raise Neo4jCredentialConfigError("Neo4j credentials are invalid or missing for this session")
                resolve_neo4j_credentials(stream_credentials)
            except Neo4jCredentialConfigError as exc:
                current_app.logger.warning(
                    "stream rejected missing/invalid neo4j credentials session_id=%s detail=%s",
                    redact_value(session_id),
                    str(exc),
                )
                return jsonify({
                    "message": "neo4j_not_connected_for_session",
                    "detail": str(exc),
                    "session_id": session_id,
                }), 400

        if _async_worker_jobs_enabled():
            reactivation = reactivate_analysis_session(session_id)
            if reactivation.get("reactivated"):
                pass
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
            current_app.logger.info("stream queued session_id=%s job_id=%s run_id=%s", session_id, job.get("job_id"), run_id)
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

    if action == "evidence":
        try:
            from batch_manager.utils.postgres_utils import get_postgres_connection
            import json
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT response_payload FROM link_analysis_evidence WHERE trace_id = %s", (source_id,))
                    row = cur.fetchone()
                    if row and row[0]:
                        payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                        graph_data = payload.get("data", {}).get("graph", {"nodes": [], "edges": []})
                        return jsonify({
                            "message": "success",
                            "results": {
                                "nodes": graph_data.get("nodes", []),
                                "edges": graph_data.get("edges", []),
                                "source_id": source_id,
                                "graph_session_id": source_id,
                                "relationship": "*",
                                "graph": graph_data
                            }
                        }), 200
            return jsonify({"message": "not_found", "detail": "evidence_not_found"}), 404
        except Exception as e:
            current_app.logger.exception('evidence graph fetch failed')
            return jsonify({'message': 'failed!', 'error': str(e)}), 500

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
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app, log_output=False)
