import argparse
import json
import os
import socket
import time
from datetime import datetime, timezone

from linkx_worker.cancellation import (
    enqueue_session_cleanup,
    is_cancel_requested,
    mark_job_cancelled,
)
from linkx_worker.handlers import run_job_safely


def utcnow():
    return datetime.now(timezone.utc)


def get_database_url():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def connect():
    import psycopg

    dsn = get_database_url()
    if not dsn:
        raise RuntimeError("DATABASE_URL or LINKX_POSTGRES_DSN is required")
    return psycopg.connect(dsn, application_name=os.getenv("WORKER_NAME", "linkx-worker"))


def emit_event(cur, job, event_type, message=None, payload=None):
    cur.execute(
        """
        INSERT INTO job_events (job_id, session_id, event_type, message, payload)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        """,
        (job["id"], job.get("session_id"), event_type, message, json.dumps(payload or {})),
    )


def claim_job(conn, queues, worker_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH candidate AS (
                SELECT j.id
                FROM jobs j
                LEFT JOIN analysis_sessions s ON s.session_id = j.session_id
                WHERE j.status IN ('created', 'queued', 'retry')
                  AND j.queue_name = ANY(%s)
                  AND j.scheduled_at <= NOW()
                  AND COALESCE(s.status, 'active') NOT IN ('cancel_requested', 'cancelling', 'cancelled')
                  AND s.cancellation_requested_at IS NULL
                ORDER BY j.priority ASC, j.scheduled_at ASC, j.created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE jobs j
            SET status = 'running',
                locked_by = %s,
                locked_at = NOW(),
                started_at = COALESCE(started_at, NOW()),
                attempts = attempts + 1
            FROM candidate
            WHERE j.id = candidate.id
            RETURNING j.id::text, j.session_id, j.run_id, j.job_type, j.queue_name, j.payload, j.attempts, j.max_attempts
            """,
            (queues, worker_name),
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        job = {
            "id": row[0],
            "session_id": row[1],
            "run_id": row[2],
            "job_type": row[3],
            "queue_name": row[4],
            "payload": row[5] or {},
            "attempts": row[6],
            "max_attempts": row[7],
        }
        job["payload"]["job_id"] = job["id"]
        job["payload"].setdefault("session_id", job.get("session_id"))
        job["payload"].setdefault("run_id", job.get("run_id"))
        emit_event(cur, job, "job_started", f"Job claimed by {worker_name}")
        conn.commit()
        return job


def finish_job(conn, job, ok, result=None, error=None):
    with conn.cursor() as cur:
        if ok:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'succeeded', finished_at = NOW(), error_message = NULL
                WHERE id = %s
                """,
                (job["id"],),
            )
            emit_event(cur, job, "job_succeeded", "Job completed", {"result": _jsonable(result)})
        else:
            next_status = "retry" if int(job.get("attempts") or 0) < int(job.get("max_attempts") or 0) else "failed"
            cur.execute(
                """
                UPDATE jobs
                SET status = %s,
                    finished_at = CASE WHEN %s = 'failed' THEN NOW() ELSE finished_at END,
                    error_message = %s,
                    scheduled_at = CASE WHEN %s = 'retry' THEN NOW() + INTERVAL '30 seconds' ELSE scheduled_at END
                WHERE id = %s
                """,
                (next_status, next_status, (error or {}).get("error"), next_status, job["id"]),
            )
            emit_event(cur, job, "job_failed" if next_status == "failed" else "job_retry", (error or {}).get("error"), error)
        conn.commit()


def _jsonable(value):
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


def run_loop(queues, poll_interval, once=False):
    worker_name = os.getenv("WORKER_NAME") or f"linkx-worker@{socket.gethostname()}:{os.getpid()}"
    print(f"[worker] starting {worker_name} queues={queues}", flush=True)
    while True:
        with connect() as conn:
            job = claim_job(conn, queues, worker_name)
            if job:
                print(f"[worker] running job_id={job['id']} type={job['job_type']} queue={job['queue_name']}", flush=True)
                if is_cancel_requested(conn, session_id=job.get("session_id"), job_id=job.get("id")):
                    mark_job_cancelled(conn, job, "Job cancelled before execution")
                    enqueue_session_cleanup(
                        conn,
                        job.get("session_id"),
                        job_id=job.get("id"),
                        payload={"reason": "cancelled_before_execution"},
                    )
                    print(f"[worker] cancelled job_id={job['id']} before execution", flush=True)
                    if once:
                        return
                    time.sleep(poll_interval)
                    continue
                ok, result, error = run_job_safely(job["job_type"], job["payload"])
                if is_cancel_requested(conn, session_id=job.get("session_id"), job_id=job.get("id")):
                    mark_job_cancelled(conn, job, "Job cancelled during execution")
                    enqueue_session_cleanup(
                        conn,
                        job.get("session_id"),
                        job_id=job.get("id"),
                        payload={"reason": "cancelled_during_execution"},
                    )
                    print(f"[worker] cancelled job_id={job['id']} during execution", flush=True)
                else:
                    finish_job(conn, job, ok, result=result, error=error)
                    print(f"[worker] finished job_id={job['id']} ok={ok}", flush=True)
            elif once:
                print("[worker] no job found", flush=True)
                return
        if once:
            return
        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="LinkX DB-backed worker runner.")
    parser.add_argument("--queues", default=os.getenv("WORKER_QUEUES", "ingestion,dataframe,analysis,graph"))
    parser.add_argument("--poll-interval", type=float, default=float(os.getenv("WORKER_POLL_INTERVAL", "2")))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    queues = [q.strip() for q in args.queues.split(",") if q.strip()]
    if not queues:
        raise SystemExit("at least one queue is required")
    run_loop(queues, args.poll_interval, once=args.once)


if __name__ == "__main__":
    main()
