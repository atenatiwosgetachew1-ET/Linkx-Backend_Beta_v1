import argparse
import json
import multiprocessing
import os
import pickle
import queue as queue_module
import socket
import tempfile
import time
from datetime import datetime, timezone

from batch_manager.utils.neo4j_utils import credentials_for_cleanup
from linkx_worker.cancellation import (
    enqueue_session_cleanup,
    is_cancel_requested,
    mark_job_cancelled,
)
from linkx_worker.handlers import run_job_safely
from security.redaction import is_sensitive_key, redact_value


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


def emit_event(cur, job, event_type, message=None, payload=None, preserve_payload=False):
    event_payload = _jsonable(payload or {}) if preserve_payload else redact_value(payload or {})
    cur.execute(
        """
        INSERT INTO job_events (job_id, session_id, event_type, message, payload)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        """,
        (job["id"], job.get("session_id"), event_type, message, json.dumps(event_payload)),
    )


def claim_job(conn, queues, worker_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH candidate AS (
                SELECT j.id
                FROM jobs j
                WHERE j.status IN ('created', 'queued', 'retry')
                  AND j.queue_name = ANY(%s)
                  AND j.scheduled_at <= NOW()
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


DEFAULT_JOB_TIMEOUT_SECONDS = {
    "search": 300,
    "graph": 300,
    "dataframe": 3600,
    "analysis": 7200,
    "ingestion": 7200,
}


def _env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _timeout_env_names(job_type=None, queue_name=None):
    names = []
    if job_type:
        normalized_job_type = str(job_type).upper().replace("-", "_")
        names.append(f"WORKER_JOB_TIMEOUT_SECONDS_{normalized_job_type}")
    if queue_name:
        normalized_queue = str(queue_name).upper().replace("-", "_")
        names.append(f"WORKER_JOB_TIMEOUT_SECONDS_{normalized_queue}")
    return names


def job_timeout_seconds(job):
    payload = job.get("payload") or {}
    for key in ("timeout_seconds", "job_timeout_seconds"):
        if payload.get(key) is not None:
            try:
                return int(payload[key])
            except (TypeError, ValueError):
                pass

    queue_name = job.get("queue_name")
    default = DEFAULT_JOB_TIMEOUT_SECONDS.get(queue_name, _env_int("WORKER_JOB_TIMEOUT_SECONDS_DEFAULT", 3600))
    if queue_name == "graph":
        default = _env_int("WORKER_GRAPH_STALE_SECONDS", default)

    for env_name in _timeout_env_names(job.get("job_type"), queue_name):
        if os.getenv(env_name) is not None:
            return _env_int(env_name, default)
    return default


def recover_stale_jobs(conn, queues, worker_name):
    recovered = 0

    for queue_name in queues:
        timeout_seconds = job_timeout_seconds({"queue_name": queue_name, "payload": {}})
        if timeout_seconds <= 0:
            continue
        error_message = f"stale {queue_name} job recovered by worker after {timeout_seconds}s"

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    finished_at = NOW(),
                    locked_by = NULL,
                    locked_at = NULL,
                    error_message = %s
                WHERE queue_name = %s
                  AND status = 'running'
                  AND started_at < NOW() - (%s * INTERVAL '1 second')
                RETURNING id::text, session_id, job_type
                """,
                (error_message, queue_name, timeout_seconds),
            )
            rows = cur.fetchall()
            for job_id, session_id, job_type in rows:
                emit_event(
                    cur,
                    {"id": job_id, "session_id": session_id},
                    "job_failed",
                    error_message,
                    {
                        "worker": worker_name,
                        "queue": queue_name,
                        "job_type": job_type,
                        "timeout_seconds": timeout_seconds,
                    },
                )
            conn.commit()

        if rows:
            recovered += len(rows)
            print(f"[worker] recovered stale {queue_name} jobs count={len(rows)} timeout={timeout_seconds}s", flush=True)

    return recovered


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
            emit_event(cur, job, "job_succeeded", "Job completed", {"result": result}, preserve_payload=True)
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
            emit_event(cur, job, "job_failed" if next_status == "failed" else "job_retry", (error or {}).get("error"), redact_value(error or {}))
        conn.commit()


def _jsonable(value, key=None):
    if is_sensitive_key(key):
        return "***"
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item, key) for item in value]
    if hasattr(value, "iso_format"):
        return value.iso_format()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _result_dir():
    path = os.getenv("WORKER_RESULT_DIR", "/tmp/linkx-worker-results")
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


def _write_child_result(result):
    fd, path = tempfile.mkstemp(prefix="job-result-", suffix=".pickle", dir=_result_dir())
    try:
        with os.fdopen(fd, "wb") as fh:
            pickle.dump(result, fh, protocol=pickle.HIGHEST_PROTOCOL)
        return path
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise


def _read_result_file(path):
    try:
        with open(path, "rb") as fh:
            return pickle.load(fh)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _job_child_main(job_type, payload, result_queue):
    try:
        result_path = _write_child_result(run_job_safely(job_type, payload))
        result_queue.put({"result_path": result_path})
    except BaseException as exc:
        try:
            result_path = _write_child_result((False, None, {"error": "worker_child_failed"}))
            result_queue.put({"result_path": result_path})
        except BaseException:
            result_queue.put({"inline_result": (False, None, {"error": "worker_child_failed"})})


def _terminate_child(process, grace_seconds=5.0):
    if not process.is_alive():
        return
    process.terminate()
    process.join(grace_seconds)
    if process.is_alive():
        process.kill()
        process.join(2.0)


def _process_start_method():
    method = os.getenv("WORKER_PROCESS_START_METHOD", "fork")
    try:
        return multiprocessing.get_context(method)
    except ValueError:
        return multiprocessing.get_context("fork")


def start_job_process(job):
    ctx = _process_start_method()
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(
        target=_job_child_main,
        args=(job["job_type"], job["payload"], result_queue),
    )
    process.start()
    return {
        "job": job,
        "process": process,
        "result_queue": result_queue,
        "started_monotonic": time.monotonic(),
    }


def _read_child_result(active_job):
    process = active_job["process"]
    result_queue = active_job["result_queue"]
    process.join()
    try:
        message = result_queue.get_nowait()
    except queue_module.Empty:
        return False, None, {"error": f"worker_child_exited:{process.exitcode}"}
    if isinstance(message, dict) and message.get("result_path"):
        return _read_result_file(message["result_path"])
    if isinstance(message, dict) and "inline_result" in message:
        return message["inline_result"]
    return message


def _mark_running(conn, job):
    with conn.cursor() as cur:
        emit_event(cur, job, "job_progress", f"Running {job['job_type']} on worker", {"queue": job.get("queue_name")})
        conn.commit()


def _cancel_job(conn, job, active_job=None, message="Job cancelled during execution"):
    if active_job:
        _terminate_child(active_job["process"])
    mark_job_cancelled(conn, job, message)
    enqueue_session_cleanup(
        conn,
        job.get("session_id"),
        job_id=job.get("id"),
        payload={"reason": "cancelled_during_execution", "run_id": job.get("run_id"), **credentials_for_cleanup(job["payload"].get("tool_credentials"))},
    )
    print(f"[worker] cancelled job_id={job['id']} during execution", flush=True)


def _finish_active_job(conn, active_job):
    job = active_job["job"]
    ok, result, error = _read_child_result(active_job)
    with conn.cursor() as cur:
        emit_event(cur, job, "job_progress", f"Finished execution ok={ok}", {"ok": ok})
        conn.commit()
    if is_cancel_requested(conn, session_id=job.get("session_id"), job_id=job.get("id")):
        _cancel_job(conn, job, message="Job cancelled during execution")
    else:
        finish_job(conn, job, ok, result=result, error=error)
        print(f"[worker] finished job_id={job['id']} ok={ok}", flush=True)


def run_loop(queues, poll_interval, once=False, concurrency=1):
    worker_name = os.getenv("WORKER_NAME") or f"linkx-worker@{socket.gethostname()}:{os.getpid()}"
    concurrency = max(1, int(concurrency or 1))
    active_jobs = []
    print(f"[worker] starting {worker_name} queues={queues} concurrency={concurrency}", flush=True)
    last_stale_recovery = 0.0
    while True:
        with connect() as conn:
            now_monotonic = time.monotonic()
            if now_monotonic - last_stale_recovery >= 30:
                recover_stale_jobs(conn, queues, worker_name)
                last_stale_recovery = now_monotonic

            # First, free slots by handling completed or cancelled child jobs.
            for active_job in list(active_jobs):
                job = active_job["job"]
                process = active_job["process"]
                if process.is_alive():
                    timeout_seconds = job_timeout_seconds(job)
                    elapsed = time.monotonic() - float(active_job.get("started_monotonic") or 0)
                    if timeout_seconds > 0 and elapsed > timeout_seconds:
                        _terminate_child(process)
                        finish_job(
                            conn,
                            job,
                            False,
                            error={
                                "error": "job timed out",
                                "queue": job.get("queue_name"),
                                "job_type": job.get("job_type"),
                                "timeout_seconds": timeout_seconds,
                            },
                        )
                        active_jobs.remove(active_job)
                        print(
                            f"[worker] timed out job_id={job['id']} type={job.get('job_type')} "
                            f"queue={job.get('queue_name')} timeout={timeout_seconds}s",
                            flush=True,
                        )
                    elif is_cancel_requested(conn, session_id=job.get("session_id"), job_id=job.get("id")):
                        _cancel_job(conn, job, active_job=active_job)
                        active_jobs.remove(active_job)
                    else:
                        conn.commit()
                    continue
                _finish_active_job(conn, active_job)
                active_jobs.remove(active_job)

            # Claim enough work to fill available slots.
            claimed_any = False
            while len(active_jobs) < concurrency:
                job = claim_job(conn, queues, worker_name)
                if not job:
                    break
                claimed_any = True
                print(f"[worker] running job_id={job['id']} type={job['job_type']} queue={job['queue_name']}", flush=True)
                if is_cancel_requested(conn, session_id=job.get("session_id"), job_id=job.get("id")):
                    mark_job_cancelled(conn, job, "Job cancelled before execution")
                    enqueue_session_cleanup(
                        conn,
                        job.get("session_id"),
                        job_id=job.get("id"),
                        payload={"reason": "cancelled_before_execution", "run_id": job.get("run_id"), **credentials_for_cleanup(job["payload"].get("tool_credentials"))},
                    )
                    print(f"[worker] cancelled job_id={job['id']} before execution", flush=True)
                    continue
                _mark_running(conn, job)
                active_jobs.append(start_job_process(job))

            if once and not active_jobs:
                if not claimed_any:
                    print("[worker] no job found", flush=True)
                return
        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="LinkX DB-backed worker runner.")
    parser.add_argument("--queues", default=os.getenv("WORKER_QUEUES", "ingestion,search,dataframe,analysis,graph"))
    parser.add_argument("--poll-interval", type=float, default=float(os.getenv("WORKER_POLL_INTERVAL", "2")))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("WORKER_CONCURRENCY", "1")))
    args = parser.parse_args()
    queues = [q.strip() for q in args.queues.split(",") if q.strip()]
    if not queues:
        raise SystemExit("at least one queue is required")

    try:
        import sys
        src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if src_dir not in sys.path:
            sys.path.insert(0, src_dir)
        from observability.metrics import start_otel_metrics_server
        start_otel_metrics_server(8889)
    except Exception as exc:
        print(f"[worker] metrics server notice: {exc}", flush=True)

    run_loop(queues, args.poll_interval, once=args.once, concurrency=args.concurrency)


if __name__ == "__main__":
    main()
