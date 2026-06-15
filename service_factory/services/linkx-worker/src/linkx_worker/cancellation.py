import json
import os
import time


CANCEL_REQUESTED_STATUSES = {"cancel_requested", "cancelling", "cancelled"}


def get_database_url():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def connect(application_name="linkx-worker-cancellation"):
    import psycopg

    dsn = get_database_url()
    if not dsn:
        raise RuntimeError("DATABASE_URL or LINKX_POSTGRES_DSN is required")
    return psycopg.connect(dsn, application_name=application_name)


def is_cancel_requested(conn, session_id=None, job_id=None):
    with conn.cursor() as cur:
        if job_id:
            cur.execute(
                """
                SELECT 1
                FROM jobs j
                LEFT JOIN analysis_sessions s ON s.session_id = j.session_id
                WHERE j.id = %s
                  AND (
                    j.status IN ('cancel_requested', 'cancelling', 'cancelled')
                    OR j.cancellation_requested_at IS NOT NULL
                    OR s.status IN ('cancel_requested', 'cancelling', 'cancelled')
                    OR s.cancellation_requested_at IS NOT NULL
                  )
                LIMIT 1
                """,
                (job_id,),
            )
            if cur.fetchone():
                return True

        if session_id:
            cur.execute(
                """
                SELECT 1
                FROM analysis_sessions
                WHERE session_id = %s
                  AND (
                    status IN ('cancel_requested', 'cancelling', 'cancelled')
                    OR cancellation_requested_at IS NOT NULL
                  )
                LIMIT 1
                """,
                (str(session_id),),
            )
            return cur.fetchone() is not None
    return False


class DatabaseCancellationEvent:
    """Small adapter with the same shape as threading.Event for analyzer loops."""

    def __init__(self, session_id=None, job_id=None, check_interval=2.0):
        self.session_id = str(session_id) if session_id else None
        self.job_id = str(job_id) if job_id else None
        self.check_interval = float(check_interval)
        self._local_set = False
        self._last_checked = 0.0
        self._last_result = False

    def set(self):
        self._local_set = True

    def is_set(self):
        if self._local_set:
            return True

        now = time.monotonic()
        if now - self._last_checked < self.check_interval:
            return self._last_result

        self._last_checked = now
        try:
            with connect() as conn:
                self._last_result = is_cancel_requested(
                    conn,
                    session_id=self.session_id,
                    job_id=self.job_id,
                )
                return self._last_result
        except Exception:
            return self._last_result


def mark_job_cancelled(conn, job, message="Job cancelled"):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'cancelled',
                finished_at = NOW(),
                error_message = %s,
                cancellation_requested_at = COALESCE(cancellation_requested_at, NOW())
            WHERE id = %s
            """,
            (message, job["id"]),
        )
        cur.execute(
            """
            INSERT INTO job_events (job_id, session_id, event_type, message, payload)
            VALUES (%s, %s, 'job_cancelled', %s, '{}'::jsonb)
            """,
            (job["id"], job.get("session_id"), message),
        )
    conn.commit()


def enqueue_session_cleanup(conn, session_id, job_id=None, payload=None, dry_run=False):
    if not session_id:
        return None
    cleanup_payload = dict(payload or {})
    cleanup_payload.setdefault("session_id", str(session_id))
    if job_id:
        cleanup_payload.setdefault("job_id", str(job_id))

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO cleanup_runs (cleanup_type, status, session_id, job_id, dry_run, summary)
            VALUES ('session', 'created', %s, %s, %s, %s::jsonb)
            RETURNING id::text
            """,
            (str(session_id), job_id, bool(dry_run), json.dumps(cleanup_payload)),
        )
        cleanup_id = cur.fetchone()[0]
    conn.commit()
    return cleanup_id
