import json
import os
from datetime import datetime


GRAPH_METADATA_CHANGED_EVENT = "graph_metadata_changed"


def _database_url():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def _jsonable(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return str(value)


def record_graph_metadata_changed(
    session_id,
    *,
    job_id=None,
    run_id=None,
    batch_id=None,
    phase=None,
    nodes_inserted=None,
    relationships_inserted=None,
    analysis_updated=None,
    extra=None,
):
    if not session_id:
        return None
    dsn = _database_url()
    if not dsn:
        return None

    payload = {
        "session_id": str(session_id),
        "run_id": str(run_id) if run_id else None,
        "batch_id": str(batch_id) if batch_id else None,
        "phase": str(phase) if phase else None,
        "nodes_inserted": nodes_inserted,
        "relationships_inserted": relationships_inserted,
        "analysis_updated": analysis_updated,
    }
    if extra:
        payload["extra"] = _jsonable(extra)
    payload = {key: value for key, value in payload.items() if value is not None}

    try:
        import psycopg

        with psycopg.connect(dsn, application_name="linkx-graph-status-event") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_events (job_id, session_id, event_type, message, payload)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    RETURNING id
                    """,
                    (
                        str(job_id) if job_id else None,
                        str(session_id),
                        GRAPH_METADATA_CHANGED_EVENT,
                        f"Graph metadata changed: {phase or 'updated'}",
                        json.dumps(_jsonable(payload)),
                    ),
                )
                event_id = cur.fetchone()[0]
            conn.commit()
        return event_id
    except Exception as exc:
        print(f"[graph_status_event] record failed session_id={session_id} phase={phase}: {exc}", flush=True)
        return None


def latest_graph_metadata_event(session_id, after_event_id=0):
    if not session_id:
        return None
    dsn = _database_url()
    if not dsn:
        return None
    try:
        after_id = int(after_event_id or 0)
    except (TypeError, ValueError):
        after_id = 0

    try:
        import psycopg

        with psycopg.connect(dsn, application_name="linkx-graph-status-event-read") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, payload, created_at
                    FROM job_events
                    WHERE session_id = %s
                      AND event_type = %s
                      AND id > %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (str(session_id), GRAPH_METADATA_CHANGED_EVENT, after_id),
                )
                row = cur.fetchone()
        if not row:
            return None
        payload = row[1] or {}
        return {
            "event_id": row[0],
            "payload": payload if isinstance(payload, dict) else {},
            "created_at": row[2].isoformat() if row[2] else None,
        }
    except Exception as exc:
        print(f"[graph_status_event] read failed session_id={session_id}: {exc}", flush=True)
        return None


def has_active_graph_session_job(session_id):
    if not session_id:
        return False
    dsn = _database_url()
    if not dsn:
        return False
    try:
        import psycopg

        with psycopg.connect(dsn, application_name="linkx-graph-status-active-job") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM jobs
                    WHERE session_id = %s
                      AND (
                          status IN ('created', 'queued', 'running', 'retry', 'cancel_requested')
                          OR (finished_at IS NULL AND status NOT IN ('succeeded', 'failed', 'cancelled'))
                      )
                    LIMIT 1
                    """,
                    (str(session_id),),
                )
                return cur.fetchone() is not None
    except Exception as exc:
        print(f"[graph_status_event] active job check failed session_id={session_id}: {exc}", flush=True)
        return False
