import os
import psycopg

def get_dsn():
    with open("/opt/linkx-worker/.env") as f:
        for line in f:
            if line.startswith("LINKX_POSTGRES_DSN="):
                return line.strip().split("=", 1)[1].strip().strip('"').strip("'")
    return None

dsn = get_dsn()
if not dsn:
    print("Could not find DSN")
    exit(1)

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        # 1. Insert service account
        cur.execute("""
            INSERT INTO service_accounts (id, service_name, created_at)
            VALUES (999, 'xvigilance_daemon', NOW())
            ON CONFLICT DO NOTHING
            RETURNING id;
        """)
        
        # 2. Insert analysis session
        cur.execute("""
            INSERT INTO analysis_sessions (session_id, owner_service_id, created_by_type, created_by_id, status)
            VALUES ('XVIGILANCE_FINDINGS', 999, 'service_account', 999, 'active')
            ON CONFLICT (session_id) DO UPDATE SET status = 'active';
        """)
        
        conn.commit()
        print("Successfully registered XVIGILANCE_FINDINGS in the database!")
