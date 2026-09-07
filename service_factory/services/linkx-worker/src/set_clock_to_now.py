import os
import psycopg
import datetime

dsn = None
with open("/opt/linkx-worker/.env") as f:
    for line in f:
        if line.startswith("LINKX_POSTGRES_DSN="):
            dsn = line.strip().split("=", 1)[1].strip().strip("\"'")

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        now_utc_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:00:00")
        cur.execute("UPDATE xvigilance_checkpoints SET last_window_end = %s WHERE feed_name = %s", (now_utc_str, "hourly_transaction_detective"))
        conn.commit()

print("xVigilance clock successfully reset to exactly right now!")
