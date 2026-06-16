import argparse
import json
import os
import time

from linkx_cleanup.db import connect


def enqueue_cleanup(cleanup_type, payload=None, dry_run=False):
    payload = payload or {}
    with connect(application_name="linkx-cleanup-scheduler") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cleanup_runs (cleanup_type, status, dry_run, session_id, summary)
                VALUES (%s, 'queued', %s, %s, %s::jsonb)
                RETURNING id::text
                """,
                (cleanup_type, dry_run, payload.get("session_id"), json.dumps(payload)),
            )
            cleanup_id = cur.fetchone()[0]
        conn.commit()
    return cleanup_id


def schedule_once(dry_run=False):
    ids = []
    ids.append(enqueue_cleanup("artifacts_expired", {"limit": int(os.getenv("CLEANUP_ARTIFACT_LIMIT", "500"))}, dry_run=dry_run))
    ids.append(enqueue_cleanup("metadata_prune", {"retention_days": int(os.getenv("LINKX_METADATA_RETENTION_DAYS", "30"))}, dry_run=dry_run))
    ids.append(enqueue_cleanup(
        "abandoned_sessions",
        {
            "retention_minutes": int(os.getenv("LINKX_ABANDONED_SESSION_MINUTES", "360")),
            "limit": int(os.getenv("CLEANUP_ABANDONED_SESSION_LIMIT", "100")),
            "reason": "scheduled_abandoned_session_scan",
        },
        dry_run=dry_run,
    ))
    return ids


def main():
    parser = argparse.ArgumentParser(description="Schedule LinkX retention cleanup runs.")
    parser.add_argument("--interval-seconds", type=float, default=float(os.getenv("CLEANUP_SCHEDULE_INTERVAL_SECONDS", "3600")))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true", default=os.getenv("CLEANUP_DRY_RUN", "false").lower() == "true")
    args = parser.parse_args()
    while True:
        ids = schedule_once(dry_run=args.dry_run)
        print(f"[cleanup-scheduler] queued cleanup_runs={ids} dry_run={args.dry_run}", flush=True)
        if args.once:
            return
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
