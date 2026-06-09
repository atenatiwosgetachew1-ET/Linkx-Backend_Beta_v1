import argparse
import json
import os


def main():
    parser = argparse.ArgumentParser(description="Enqueue a LinkX worker job into PostgreSQL.")
    parser.add_argument("--queue", required=True)
    parser.add_argument("--type", required=True, dest="job_type")
    parser.add_argument("--session-id")
    parser.add_argument("--run-id")
    parser.add_argument("--payload", default="{}", help="JSON payload")
    args = parser.parse_args()

    import psycopg

    dsn = os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("DATABASE_URL or LINKX_POSTGRES_DSN is required")
    payload = json.loads(args.payload)
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO jobs (session_id, run_id, job_type, queue_name, status, payload)
                VALUES (%s, %s, %s, %s, 'queued', %s::jsonb)
                RETURNING id::text
                """,
                (args.session_id, args.run_id, args.job_type, args.queue, json.dumps(payload)),
            )
            print(cur.fetchone()[0])
        conn.commit()


if __name__ == "__main__":
    main()
