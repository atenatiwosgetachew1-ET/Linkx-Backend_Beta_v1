from batch_manager.utils.postgres_utils import get_postgres_connection
with get_postgres_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'analysis_sessions'")
        for r in cur.fetchall():
            print(r)
