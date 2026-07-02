import json
import os
from datetime import date, datetime

from flask import Blueprint, jsonify, request

from auth.decorators import current_actor_from_request, permission_required
from auth.repository import can_access_analysis_session_actor, record_security_event
from globals import load_temp_config
from service_orchestration import connect
from batch_manager.utils.neo4j_utils import create_neo4j_driver


ai_service_api = Blueprint("ai_service_api", __name__)


def _json_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _rows(cursor, columns):
    return [
        {column: _json_value(value) for column, value in zip(columns, row)}
        for row in cursor.fetchall()
    ]


def _int_arg(name, default, minimum=0, maximum=500):
    try:
        value = int(request.args.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


def _env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _csv_env_set(name):
    return {item.strip() for item in (os.getenv(name) or "").split(",") if item.strip()}


def _ai_global_read_enabled():
    return _env_bool("LINKX_AI_ALLOW_GLOBAL_READ", False)


def _ai_allowed_session_ids():
    return _csv_env_set("LINKX_AI_ALLOWED_SESSION_IDS")


def _audit_context():
    return {
        "ip_address": request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip() or None,
        "user_agent": (request.headers.get("User-Agent") or "")[:512] or None,
    }


def _record_ai_event(event_type, *, session_id=None, target_id=None, success=None, metadata=None):
    try:
        ctx = _audit_context()
        return record_security_event(
            event_type,
            actor=current_actor_from_request(),
            target_type="ai_api",
            target_id=target_id,
            session_id=session_id,
            success=success,
            metadata=metadata or {},
            ip_address=ctx["ip_address"],
            user_agent=ctx["user_agent"],
        )
    except Exception:
        return None


def _session_info(session_id):
    with connect(application_name="linkx-ai-session-info") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT session_id, parent_session_id, owner_user_id, owner_service_id
                FROM analysis_sessions
                WHERE session_id = %s
                """,
                (str(session_id),),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "session_id": row[0],
        "parent_session_id": row[1],
        "owner_user_id": row[2],
        "owner_service_id": row[3],
    }


def _actor_can_read_ai_session(session_id, info=None):
    actor = current_actor_from_request()
    if not actor:
        return False
    if _ai_global_read_enabled():
        return True
    if can_access_analysis_session_actor(session_id, actor):
        return True
    info = info or _session_info(session_id)
    if not info:
        return False
    allowed = _ai_allowed_session_ids()
    return str(info.get("session_id")) in allowed or str(info.get("parent_session_id") or "") in allowed


def _require_ai_session_access(session_id, event_type):
    info = _session_info(session_id)
    if not info:
        _record_ai_event(event_type, session_id=session_id, success=False, metadata={"error": "not_found"})
        return None, (jsonify({"message": "not_found"}), 404)
    if not _actor_can_read_ai_session(session_id, info):
        _record_ai_event(event_type, session_id=session_id, success=False, metadata={"error": "session_not_allowed"})
        return None, (jsonify({"message": "forbidden", "detail": "ai_session_not_allowed"}), 403)
    _record_ai_event(event_type, session_id=session_id, success=True)
    return info, None


def _ai_session_filter(where, params):
    if _ai_global_read_enabled():
        return where, params
    actor = current_actor_from_request() or {}
    allowed = list(_ai_allowed_session_ids())
    if actor.get("actor_type") == "service":
        where.append("(owner_service_id = %s OR session_id = ANY(%s::text[]) OR parent_session_id = ANY(%s::text[]))")
    else:
        where.append("(owner_user_id = %s OR session_id = ANY(%s::text[]) OR parent_session_id = ANY(%s::text[]))")
    params.extend([actor.get("id"), allowed, allowed])
    return where, params


def _session_exists(session_id):
    return _session_info(session_id) is not None


def _graph_session_ids(session_id):
    raw = str(session_id)
    ids = [raw]
    if "_" in raw:
        _window, parent = raw.split("_", 1)
        if parent:
            ids.append(parent)
    else:
        with connect(application_name="linkx-ai-child-sessions") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_id FROM analysis_sessions WHERE parent_session_id = %s ORDER BY created_at DESC LIMIT 100",
                    (raw,),
                )
                ids.extend(row[0] for row in cur.fetchall())
    return list(dict.fromkeys(str(item) for item in ids if item))


@ai_service_api.route("/health", methods=["GET"])
@permission_required("ai:read")
def ai_health():
    return jsonify({"message": "success", "results": {"status": "ok", "service": "linkx-ai-service"}}), 200


@ai_service_api.route("/sessions", methods=["GET"])
@permission_required("ai:session:read")
def list_sessions():
    limit = _int_arg("limit", 50, 1, 200)
    offset = _int_arg("offset", 0, 0, 100000)
    status = request.args.get("status")
    where = []
    params = []
    if status:
        where.append("status = %s")
        params.append(status)
    where, params = _ai_session_filter(where, params)
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    with connect(application_name="linkx-ai-list-sessions") as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT session_id, parent_session_id, owner_user_id, owner_service_id,
                       created_by_type, created_by_id, status, created_at, last_seen_at,
                       ended_at, cancellation_requested_at, cancellation_reason
                FROM analysis_sessions
                {where_sql}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (*params, limit, offset),
            )
            sessions = _rows(cur, [
                "session_id", "parent_session_id", "owner_user_id", "owner_service_id",
                "created_by_type", "created_by_id", "status", "created_at", "last_seen_at",
                "ended_at", "cancellation_requested_at", "cancellation_reason",
            ])
    _record_ai_event("ai.sessions.list", success=True, metadata={"count": len(sessions), "global_read": _ai_global_read_enabled()})
    return jsonify({"message": "success", "results": {"sessions": sessions, "limit": limit, "offset": offset}}), 200


@ai_service_api.route("/sessions/<session_id>", methods=["GET"])
@permission_required("ai:session:read")
def get_session(session_id):
    _info, error = _require_ai_session_access(session_id, "ai.session.read")
    if error:
        return error
    with connect(application_name="linkx-ai-get-session") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT session_id, parent_session_id, owner_user_id, owner_service_id,
                       created_by_type, created_by_id, status, created_at, last_seen_at,
                       ended_at, cancellation_requested_at, cancellation_reason, cancel_requested_by
                FROM analysis_sessions
                WHERE session_id = %s
                """,
                (str(session_id),),
            )
            rows = _rows(cur, [
                "session_id", "parent_session_id", "owner_user_id", "owner_service_id",
                "created_by_type", "created_by_id", "status", "created_at", "last_seen_at",
                "ended_at", "cancellation_requested_at", "cancellation_reason", "cancel_requested_by",
            ])
    if not rows:
        return jsonify({"message": "not_found"}), 404
    return jsonify({"message": "success", "results": {"session": rows[0]}}), 200


@ai_service_api.route("/sessions/<session_id>/artifacts", methods=["GET"])
@permission_required("ai:artifact:read")
def get_session_artifacts(session_id):
    _info, error = _require_ai_session_access(session_id, "ai.artifacts.read")
    if error:
        return error
    limit = _int_arg("limit", 100, 1, 500)
    offset = _int_arg("offset", 0, 0, 100000)
    with connect(application_name="linkx-ai-session-artifacts") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text, session_id, artifact_type, storage_uri, filename,
                       delete_status, created_at, expires_at, metadata::text
                FROM artifacts
                WHERE session_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (str(session_id), limit, offset),
            )
            artifacts = _rows(cur, [
                "id", "session_id", "artifact_type", "storage_uri", "filename",
                "delete_status", "created_at", "expires_at", "metadata",
            ])
    for artifact in artifacts:
        if artifact.get("metadata"):
            try:
                artifact["metadata"] = json.loads(artifact["metadata"])
            except Exception:
                pass
    _record_ai_event("ai.artifacts.list", session_id=session_id, success=True, metadata={"count": len(artifacts)})
    return jsonify({"message": "success", "results": {"artifacts": artifacts, "limit": limit, "offset": offset}}), 200


@ai_service_api.route("/cleanup-runs", methods=["GET"])
@permission_required("ai:cleanup:read")
def list_cleanup_runs():
    limit = _int_arg("limit", 50, 1, 200)
    offset = _int_arg("offset", 0, 0, 100000)
    session_id = request.args.get("session_id")
    where = []
    params = []
    from_sql = "cleanup_runs cr"
    if session_id:
        _info, error = _require_ai_session_access(session_id, "ai.cleanup.read")
        if error:
            return error
        where.append("cr.session_id = %s")
        params.append(str(session_id))
    elif not _ai_global_read_enabled():
        from_sql = "cleanup_runs cr JOIN analysis_sessions s ON s.session_id = cr.session_id"
        actor = current_actor_from_request() or {}
        allowed = list(_ai_allowed_session_ids())
        if actor.get("actor_type") == "service":
            where.append("(s.owner_service_id = %s OR s.session_id = ANY(%s::text[]) OR s.parent_session_id = ANY(%s::text[]))")
        else:
            where.append("(s.owner_user_id = %s OR s.session_id = ANY(%s::text[]) OR s.parent_session_id = ANY(%s::text[]))")
        params.extend([actor.get("id"), allowed, allowed])
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    with connect(application_name="linkx-ai-cleanup-runs") as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT cr.id::text, cr.cleanup_type, cr.status, cr.session_id, cr.dry_run,
                       cr.started_at, cr.finished_at, cr.created_at, cr.summary::text
                FROM {from_sql}
                {where_sql}
                ORDER BY cr.created_at DESC
                LIMIT %s OFFSET %s
                """,
                (*params, limit, offset),
            )
            runs = _rows(cur, [
                "id", "cleanup_type", "status", "session_id", "dry_run",
                "started_at", "finished_at", "created_at", "summary",
            ])
    for run in runs:
        if run.get("summary"):
            try:
                run["summary"] = json.loads(run["summary"])
            except Exception:
                pass
    _record_ai_event("ai.cleanup.list", session_id=session_id, success=True, metadata={"count": len(runs), "global_read": _ai_global_read_enabled()})
    return jsonify({"message": "success", "results": {"cleanup_runs": runs, "limit": limit, "offset": offset}}), 200


def _neo4j_driver_for_session(session_id):
    credentials = load_temp_config("tool_credentials", session_id)
    if not isinstance(credentials, dict):
        credentials = load_temp_config("tool_credentials", str(session_id).split("_", 1)[-1])
    if not isinstance(credentials, dict):
        return None, "neo4j_credentials_not_found"
    return create_neo4j_driver(credentials), None


@ai_service_api.route("/sessions/<session_id>/graph/metadata", methods=["GET"])
@permission_required("ai:graph:metadata:read")
def graph_metadata(session_id):
    _info, error = _require_ai_session_access(session_id, "ai.graph.metadata.read")
    if error:
        return error
    driver, error = _neo4j_driver_for_session(session_id)
    if error:
        return jsonify({"message": error}), 404
    session_ids = _graph_session_ids(session_id)
    try:
        with driver.session() as session:
            counts = session.run(
                """
                MATCH (n)
                WHERE n.session_id IN $session_ids OR n.parent_session_id IN $session_ids OR n.batch_id IN $session_ids
                WITH count(n) AS nodes
                MATCH ()-[r]->()
                WHERE r.session_id IN $session_ids OR r.parent_session_id IN $session_ids OR r.batch_id IN $session_ids
                RETURN nodes, count(r) AS relationships, collect(DISTINCT type(r))[0..100] AS relationship_types
                """,
                session_ids=session_ids,
            ).single()
    finally:
        driver.close()
    node_count = counts["nodes"] if counts else 0
    relationship_count = counts["relationships"] if counts else 0
    relationship_types = counts["relationship_types"] if counts else []
    _record_ai_event(
        "ai.graph.metadata.result",
        session_id=session_id,
        success=True,
        metadata={"nodes": node_count, "relationships": relationship_count, "relationship_type_count": len(relationship_types)},
    )
    return jsonify({
        "message": "success",
        "results": {
            "session_id": str(session_id),
            "session_ids": session_ids,
            "nodes": node_count,
            "relationships": relationship_count,
            "relationship_types": relationship_types,
        },
    }), 200
