from batch_manager.utils.postgres_utils import get_postgres_connection
with get_postgres_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, job_type, status, created_at FROM worker_jobs WHERE job_type='risk_scoring_webhook'")
        rows = cur.fetchall()
        for r in rows:
            print(r)
