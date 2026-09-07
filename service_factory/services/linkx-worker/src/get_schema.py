import os
import psycopg
dsn = None
with open("/opt/linkx-worker/.env") as f:
    for line in f:
        if line.startswith("LINKX_POSTGRES_DSN="):
            dsn = line.strip().split("=", 1)[1].strip().strip("\"'")
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'service_accounts';")
        print("service_accounts columns:", [row[0] for row in cur.fetchall()])
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'analysis_sessions';")
        print("analysis_sessions columns:", [row[0] for row in cur.fetchall()])
