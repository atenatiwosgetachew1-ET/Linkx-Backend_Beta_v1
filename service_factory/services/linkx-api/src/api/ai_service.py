import json
from datetime import date, datetime

from flask import Blueprint, jsonify, request

from auth.decorators import permission_required
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


def _session_exists(session_id):
    with connect(application_name="linkx-ai-session-exists") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM analysis_sessions WHERE session_id = %s", (str(session_id),))
            return cur.fetchone() is not None


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
@permission_required("ai:read")
def list_sessions():
    limit = _int_arg("limit", 50, 1, 200)
    offset = _int_arg("offset", 0, 0, 100000)
    status = request.args.get("status")
    where = []
    params = []
    if status:
        where.append("status = %s")
        params.append(status)
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
    return jsonify({"message": "success", "results": {"sessions": sessions, "limit": limit, "offset": offset}}), 200


@ai_service_api.route("/sessions/<session_id>", methods=["GET"])
@permission_required("ai:read")
def get_session(session_id):
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
@permission_required("ai:read")
def get_session_artifacts(session_id):
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
    return jsonify({"message": "success", "results": {"artifacts": artifacts, "limit": limit, "offset": offset}}), 200


@ai_service_api.route("/cleanup-runs", methods=["GET"])
@permission_required("ai:read")
def list_cleanup_runs():
    limit = _int_arg("limit", 50, 1, 200)
    offset = _int_arg("offset", 0, 0, 100000)
    session_id = request.args.get("session_id")
    where = []
    params = []
    if session_id:
        where.append("session_id = %s")
        params.append(str(session_id))
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    with connect(application_name="linkx-ai-cleanup-runs") as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id::text, cleanup_type, status, session_id, dry_run,
                       started_at, finished_at, created_at, summary::text
                FROM cleanup_runs
                {where_sql}
                ORDER BY created_at DESC
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
    return jsonify({"message": "success", "results": {"cleanup_runs": runs, "limit": limit, "offset": offset}}), 200


def _neo4j_driver_for_session(session_id):
    credentials = load_temp_config("tool_credentials", session_id)
    if not isinstance(credentials, dict):
        credentials = load_temp_config("tool_credentials", str(session_id).split("_", 1)[-1])
    if not isinstance(credentials, dict):
        return None, "neo4j_credentials_not_found"
    return create_neo4j_driver(credentials), None


@ai_service_api.route("/sessions/<session_id>/graph/metadata", methods=["GET"])
@permission_required("ai:read")
def graph_metadata(session_id):
    if not _session_exists(session_id):
        return jsonify({"message": "not_found"}), 404
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
    return jsonify({
        "message": "success",
        "results": {
            "session_id": str(session_id),
            "session_ids": session_ids,
            "nodes": counts["nodes"] if counts else 0,
            "relationships": counts["relationships"] if counts else 0,
            "relationship_types": counts["relationship_types"] if counts else [],
        },
    }), 200
