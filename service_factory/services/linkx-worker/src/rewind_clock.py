import os
import psycopg

dsn = None
with open("/opt/linkx-worker/.env") as f:
    for line in f:
        if line.startswith("LINKX_POSTGRES_DSN="):
            dsn = line.strip().split("=", 1)[1].strip().strip("\"'")

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("UPDATE xvigilance_checkpoints SET last_window_end = '2026-03-01 10:00:00' WHERE feed_name = 'hourly_transaction_detective'")
        conn.commit()

print("xVigilance clock successfully rewound to March 2026!")
