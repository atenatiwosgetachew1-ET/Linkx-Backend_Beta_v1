import json
import os
import time
from datetime import datetime


GRAPH_METADATA_CHANGED_EVENT = "graph_metadata_changed"
_EVENT_CACHE = {}
_ACTIVE_JOB_CACHE = {}
_LOOKUP_INDEXES_CHECKED = False


def _verbose_logging():
    return str(os.getenv("LINKX_GRAPH_STATUS_VERBOSE", "0")).lower() in {"1", "true", "yes", "on"}


def _database_url():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def _env_float(name, default):
    try:
        return max(0.1, float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return float(default)


def _cache_get(cache, key):
    entry = cache.get(key)
    if not entry:
        return None
    expires_at, value = entry
    if time.monotonic() >= expires_at:
        cache.pop(key, None)
        return None
    return value


def _cache_set(cache, key, value, ttl):
    cache[key] = (time.monotonic() + ttl, value)
    return value


def _ensure_lookup_indexes(dsn):
    global _LOOKUP_INDEXES_CHECKED
    if _LOOKUP_INDEXES_CHECKED:
        return
    _LOOKUP_INDEXES_CHECKED = True
    try:
        import psycopg

        with psycopg.connect(dsn, application_name="linkx-graph-status-indexes") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_job_events_session_type_id_desc
                    ON job_events(session_id, event_type, id DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_jobs_session_status_finished
                    ON jobs(session_id, status, finished_at)
                    """
                )
            conn.commit()
    except Exception:
        if _verbose_logging():
            print("[graph_status_event] lookup index check failed", flush=True)


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
    except Exception:
        if _verbose_logging():
            print(f"[graph_status_event] record failed session_id={session_id} phase={phase}", flush=True)
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

    session_key = str(session_id)
    _ensure_lookup_indexes(dsn)
    ttl = _env_float("LINKX_GRAPH_STATUS_EVENT_CACHE_SECONDS", 1)
    cached_latest = _cache_get(_EVENT_CACHE, session_key)
    if cached_latest is not None:
        event_id = cached_latest.get("event_id") if cached_latest else None
        if event_id and int(event_id) > after_id:
            return cached_latest
        if event_id is None or int(event_id) <= after_id:
            return None

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
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (session_key, GRAPH_METADATA_CHANGED_EVENT),
                )
                row = cur.fetchone()
        if not row:
            _cache_set(_EVENT_CACHE, session_key, {}, ttl)
            return None
        payload = row[1] or {}
        latest = {
            "event_id": row[0],
            "payload": payload if isinstance(payload, dict) else {},
            "created_at": row[2].isoformat() if row[2] else None,
        }
        _cache_set(_EVENT_CACHE, session_key, latest, ttl)
        return latest if int(latest["event_id"]) > after_id else None
    except Exception:
        if _verbose_logging():
            print(f"[graph_status_event] read failed session_id={session_id}", flush=True)
        return None


def has_active_graph_session_job(session_id):
    if not session_id:
        return False
    dsn = _database_url()
    if not dsn:
        return False
    session_key = str(session_id)
    _ensure_lookup_indexes(dsn)
    ttl = _env_float("LINKX_GRAPH_STATUS_ACTIVE_JOB_CACHE_SECONDS", 2)
    cached = _cache_get(_ACTIVE_JOB_CACHE, session_key)
    if cached is not None:
        return bool(cached)

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
                    (session_key,),
                )
                active = cur.fetchone() is not None
                return _cache_set(_ACTIVE_JOB_CACHE, session_key, active, ttl)
    except Exception:
        if _verbose_logging():
            print(f"[graph_status_event] active job check failed session_id={session_id}", flush=True)
        return False
