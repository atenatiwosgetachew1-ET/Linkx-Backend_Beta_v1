import os
import psycopg

dsn = None
with open("/opt/linkx-worker/.env") as f:
    for line in f:
        if line.startswith("LINKX_POSTGRES_DSN="):
            dsn = line.strip().split("=", 1)[1].strip().strip("\"'")

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO service_accounts (id, client_id, display_name) VALUES (999, 'xvigilance', 'xVigilance Daemon') ON CONFLICT DO NOTHING")
        cur.execute("INSERT INTO analysis_sessions (session_id, owner_service_id, created_by_type, created_by_id, status) VALUES ('XVIGILANCE_FINDINGS', 999, 'service_account', 999, 'active') ON CONFLICT DO NOTHING")
        conn.commit()

print("Successfully registered XVIGILANCE_FINDINGS in PostgreSQL!")
