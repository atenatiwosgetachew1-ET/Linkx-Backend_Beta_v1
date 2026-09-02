import argparse
import json
import os
import socket
import time

from linkx_xcleanup.db import connect
from linkx_xcleanup.tasks import run_cleanup


def claim_cleanup(conn, worker_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH candidate AS (
                SELECT id
                FROM cleanup_runs
                WHERE status IN ('created', 'queued', 'retry')
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE cleanup_runs c
            SET status = 'running', started_at = COALESCE(started_at, NOW())
            FROM candidate
            WHERE c.id = candidate.id
            RETURNING c.id::text, c.cleanup_type, c.session_id, c.job_id::text, c.dry_run, c.summary
            """,
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        cleanup = {
            "id": row[0],
            "cleanup_type": row[1],
            "session_id": row[2],
            "job_id": row[3],
            "dry_run": row[4],
            "summary": row[5] or {},
        }
        conn.commit()
        return cleanup


def finish_cleanup(conn, cleanup, ok, result=None, error=None):
    with conn.cursor() as cur:
        if ok:
            cur.execute(
                """
                UPDATE cleanup_runs
                SET status = 'succeeded', finished_at = NOW(), summary = %s::jsonb, error_message = NULL
                WHERE id = %s
                """,
                (json.dumps(result or {}), cleanup["id"]),
            )
        else:
            cur.execute(
                """
                UPDATE cleanup_runs
                SET status = 'failed', finished_at = NOW(), error_message = %s, summary = %s::jsonb
                WHERE id = %s
                """,
                (str(error), json.dumps({"error": str(error)}), cleanup["id"]),
            )
        conn.commit()


def run_loop(poll_interval=5, once=False):
    worker_name = os.getenv("CLEANUP_WORKER_NAME") or f"linkx-xcleanup@{socket.gethostname()}:{os.getpid()}"
    print(f"[cleanup] starting {worker_name}", flush=True)
    while True:
        with connect(application_name=worker_name) as conn:
            cleanup = claim_cleanup(conn, worker_name)
            if cleanup:
                payload = dict(cleanup.get("summary") or {})
                payload.setdefault("session_id", cleanup.get("session_id"))
                payload.setdefault("job_id", cleanup.get("job_id"))
                print(f"[cleanup] running id={cleanup['id']} type={cleanup['cleanup_type']} dry_run={cleanup['dry_run']}", flush=True)
                try:
                    result = run_cleanup(cleanup["cleanup_type"], payload=payload, dry_run=cleanup["dry_run"])
                    finish_cleanup(conn, cleanup, True, result=result)
                    print(f"[cleanup] finished id={cleanup['id']}", flush=True)
                except Exception as exc:
                    finish_cleanup(conn, cleanup, False, error=exc)
                    print(f"[cleanup] failed id={cleanup['id']} error={exc}", flush=True)
            elif once:
                print("[cleanup] no cleanup run found", flush=True)
                return
        if once:
            return
        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="LinkX cleanup worker.")
    parser.add_argument("--poll-interval", type=float, default=float(os.getenv("CLEANUP_POLL_INTERVAL", "5")))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    try:
        import sys
        src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if src_dir not in sys.path:
            sys.path.insert(0, src_dir)
        from observability.metrics import start_otel_metrics_server
        start_otel_metrics_server(8889)
    except Exception as exc:
        print(f"[cleanup] metrics server notice: {exc}", flush=True)

    run_loop(args.poll_interval, once=args.once)


if __name__ == "__main__":
    main()
