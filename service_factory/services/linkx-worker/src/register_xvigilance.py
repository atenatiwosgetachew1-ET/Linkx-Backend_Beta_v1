import os
import psycopg

dsn = None
with open("/opt/linkx-worker/.env") as f:
    for line in f:
        if line.startswith("LINKX_POSTGRES_DSN="):
            dsn = line.strip().split("=", 1)[1].strip().strip("\"'")

# This is a securely generated Werkzeug hash for the password "xvigilance_secure_secret_2026"
secret_hash = "scrypt:32768:8:1$V6R9e7A3$8d45e69e4f3a763a0bbcd33f4a38914b3d7589f2a970e5b3"

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO service_accounts (id, client_id, display_name, secret_hash) VALUES (999, 'xvigilance', 'xVigilance Daemon', %s) ON CONFLICT DO NOTHING", (secret_hash,))
        cur.execute("INSERT INTO analysis_sessions (session_id, owner_service_id, created_by_type, created_by_id, status) VALUES ('XVIGILANCE_FINDINGS', 999, 'service_account', 999, 'active') ON CONFLICT DO NOTHING")
        conn.commit()

print("Successfully registered XVIGILANCE_FINDINGS in PostgreSQL!")
