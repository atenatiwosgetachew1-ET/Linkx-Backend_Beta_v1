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
            cur.execute("INSERT INTO analysis_sessions (session_id, status, type) VALUES ('xvigilance_system', 'completed', 'system')")
            
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
