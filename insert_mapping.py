import os
import json
import psycopg

dsn = None
with open("/opt/linkx-worker/.env", "r") as f:
    for line in f:
        if line.startswith("LINKX_POSTGRES_DSN="):
            dsn = line.strip().split("=", 1)[1].strip().strip("\"'")

mapping = {
    "SENDERACCOUNTID": "ACCOUNTNO",
    "RECEIVERACCOUNTID": "BENACCOUNTNO",
    "CREATEDDATE": "TRANSACTIONDATE",
    "TRANSFERAMOUNT": "AMOUNTINBIRR"
}

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        # Ensure session exists first!
        cur.execute("SELECT session_id FROM analysis_sessions WHERE session_id = 'xvigilance_system'")
        if not cur.fetchone():
            try:
                cur.execute("INSERT INTO analysis_sessions (session_id) VALUES ('xvigilance_system')")
            except Exception as e:
                # If there are NOT NULL constraints, we can inspect them
                conn.rollback()
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'analysis_sessions' AND is_nullable = 'NO' AND column_default IS NULL")
                cols = [r[0] for r in cur.fetchall()]
                # Construct dynamic insert with dummy values for required columns
                cols.remove('session_id') if 'session_id' in cols else None
                keys = ['session_id'] + cols
                vals = ["'xvigilance_system'"] + ["'dummy'" for _ in cols]
                cur.execute(f"INSERT INTO analysis_sessions ({','.join(keys)}) VALUES ({','.join(vals)})")
            
        cur.execute("SELECT config FROM session_configs WHERE session_id = 'xvigilance_system' AND window_id = ''")
        row = cur.fetchone()
        
        if row and row[0]:
            config = row[0]
            config["column_mapping"] = mapping
            cur.execute(
                "UPDATE session_configs SET config = %s WHERE session_id = 'xvigilance_system' AND window_id = ''",
                (json.dumps(config),)
            )
        else:
            config = {"column_mapping": mapping}
            cur.execute(
                "INSERT INTO session_configs (session_id, window_id, config) VALUES ('xvigilance_system', '', %s)",
                (json.dumps(config),)
            )
        conn.commit()
print("SUCCESS: Dynamic JSON Column Mapping injected into Postgres!")
