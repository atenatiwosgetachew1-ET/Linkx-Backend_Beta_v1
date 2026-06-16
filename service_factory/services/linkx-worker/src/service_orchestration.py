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
