import json
import os

from batch_manager.utils.neo4j_utils import credentials_for_cleanup


def get_database_url():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def connect(application_name="linkx-api-orchestration"):
    import psycopg

    dsn = get_database_url()
    if not dsn:
        raise RuntimeError("DATABASE_URL or LINKX_POSTGRES_DSN is required")
    return psycopg.connect(dsn, application_name=application_name)


def enqueue_cleanup_run(cleanup_type, session_id=None, run_id=None, reason="event_requested", neo4j_credentials=None, payload=None, dry_run=False):
    cleanup_payload = dict(payload or {})
    if session_id is not None:
        cleanup_payload.setdefault("session_id", str(session_id))
    if run_id is not None:
        cleanup_payload.setdefault("run_id", str(run_id))
    cleanup_payload.setdefault("reason", reason)
    cleanup_payload.update(credentials_for_cleanup(neo4j_credentials))

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cleanup_runs (cleanup_type, status, session_id, dry_run, summary)
                VALUES (%s, 'created', %s, %s, %s::jsonb)
                RETURNING id::text
                """,
                (cleanup_type, str(session_id) if session_id is not None else None, bool(dry_run), json.dumps(cleanup_payload)),
            )
            cleanup_id = cur.fetchone()[0]
        conn.commit()
    return cleanup_id


def request_session_cancellation(session_id, reason="client_requested", requested_by=None, neo4j_credentials=None):
    if not session_id:
        return {"cancel_requested": False, "message": "missing session_id"}

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analysis_sessions
                SET status = CASE
                        WHEN status IN ('cleaned', 'cancelled') THEN status
                        ELSE 'cancel_requested'
                    END,
                    cancellation_requested_at = COALESCE(cancellation_requested_at, NOW()),
                    cancellation_reason = COALESCE(cancellation_reason, %s),
                    cancel_requested_by = COALESCE(cancel_requested_by, %s),
                    ended_at = COALESCE(ended_at, NOW()),
                    last_seen_at = NOW()
                WHERE session_id = %s
                RETURNING session_id
                """,
                (reason, requested_by, str(session_id)),
            )
            session_row = cur.fetchone()
            cur.execute(
                """
                UPDATE jobs
                SET status = CASE
                        WHEN status IN ('created', 'queued', 'retry') THEN 'cancelled'
                        WHEN status = 'running' THEN 'cancel_requested'
                        ELSE status
                    END,
                    cancellation_requested_at = COALESCE(cancellation_requested_at, NOW()),
                    cancellation_reason = COALESCE(cancellation_reason, %s),
                    finished_at = CASE
                        WHEN status IN ('created', 'queued', 'retry') THEN COALESCE(finished_at, NOW())
                        ELSE finished_at
                    END
                WHERE session_id = %s
                  AND status NOT IN ('succeeded', 'failed', 'cancelled')
                RETURNING id::text, status
                """,
                (reason, str(session_id)),
            )
            jobs = [{"id": row[0], "status": row[1]} for row in cur.fetchall()]
            cleanup_id = None
            if session_row:
                cleanup_summary = {"session_id": str(session_id), "reason": reason}
                cleanup_summary.update(credentials_for_cleanup(neo4j_credentials))
                cleanup_type = "window" if "_" in str(session_id) else "session_tree"
                cur.execute(
                    """
                    INSERT INTO cleanup_runs (cleanup_type, status, session_id, dry_run, summary)
                    VALUES (%s, 'created', %s, false, %s::jsonb)
                    RETURNING id::text
                    """,
                    (cleanup_type, str(session_id), json.dumps(cleanup_summary)),
                )
                cleanup_id = cur.fetchone()[0]
        conn.commit()

    return {
        "cancel_requested": bool(session_row),
        "session_id": str(session_id),
        "jobs": jobs,
        "cleanup_id": cleanup_id,
    }


def _clamp_limit(value, default=50, maximum=200):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


def _clamp_offset(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 0
    return max(0, parsed)


def list_cleanup_audit(filters=None):
    filters = filters or {}
    limit = _clamp_limit(filters.get("limit"))
    offset = _clamp_offset(filters.get("offset"))
    where = []
    params = []

    if filters.get("session_id"):
        where.append("c.session_id = %s")
        params.append(str(filters["session_id"]))
    if filters.get("cleanup_type"):
        where.append("c.cleanup_type = %s")
        params.append(str(filters["cleanup_type"]))
    if filters.get("status"):
        where.append("c.status = %s")
        params.append(str(filters["status"]))

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    with connect(application_name="linkx-cleanup-audit") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM cleanup_runs c {where_sql}", params)
            total = cur.fetchone()[0]
            cur.execute(
                f"""
                SELECT
                    c.id::text,
                    c.cleanup_type,
                    c.status,
                    c.session_id,
                    c.job_id::text,
                    c.dry_run,
                    c.started_at,
                    c.finished_at,
                    c.created_at,
                    c.error_message,
                    c.summary,
                    s.parent_session_id,
                    s.owner_user_id,
                    s.status AS session_status,
                    COALESCE(a.artifact_count, 0) AS artifact_count,
                    COALESCE(a.deleted_artifact_count, 0) AS deleted_artifact_count
                FROM cleanup_runs c
                LEFT JOIN analysis_sessions s ON s.session_id = c.session_id
                LEFT JOIN LATERAL (
                    SELECT
                        count(*) AS artifact_count,
                        count(*) FILTER (WHERE delete_status = 'deleted') AS deleted_artifact_count
                    FROM artifacts a
                    WHERE a.session_id = c.session_id
                ) a ON TRUE
                {where_sql}
                ORDER BY c.created_at DESC
                LIMIT %s OFFSET %s
                """,
                [*params, limit, offset],
            )
            rows = cur.fetchall()

    items = []
    for row in rows:
        items.append({
            "id": row[0],
            "cleanup_type": row[1],
            "status": row[2],
            "session_id": row[3],
            "job_id": row[4],
            "dry_run": row[5],
            "started_at": row[6].isoformat() if row[6] else None,
            "finished_at": row[7].isoformat() if row[7] else None,
            "created_at": row[8].isoformat() if row[8] else None,
            "error_message": row[9],
            "summary": row[10] or {},
            "session": {
                "parent_session_id": row[11],
                "owner_user_id": row[12],
                "status": row[13],
            },
            "artifacts": {
                "count": row[14],
                "deleted_count": row[15],
            },
        })

    return {"items": items, "total": total, "limit": limit, "offset": offset}
