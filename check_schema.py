import os
import psycopg

dsn = os.getenv("LINKX_POSTGRES_DSN", "postgresql://linkx:linkx@172.27.23.106:5432/linkx")
try:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'analysis_sessions';")
            print("analysis_sessions:", cur.fetchall())
            cur.execute("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users';")
            print("users:", cur.fetchall())
except Exception as e:
    print(e)
