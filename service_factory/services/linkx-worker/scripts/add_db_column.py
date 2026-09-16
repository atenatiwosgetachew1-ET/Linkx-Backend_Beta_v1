import psycopg, os

dsn = os.getenv("LINKX_POSTGRES_DSN")
if not dsn:
    with open("/opt/linkx-worker/.env") as f:
        for line in f:
            if line.startswith("LINKX_POSTGRES_DSN="):
                dsn = line.strip().split("=", 1)[1].strip('"').strip("'")
                break

if dsn:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE xvigilance_checkpoints ADD COLUMN IF NOT EXISTS total_graph_analyzed BIGINT DEFAULT 0;")
            cur.execute("UPDATE xvigilance_checkpoints SET total_graph_analyzed = total_records_analyzed;")
        conn.commit()
    print("Database column created successfully!")
else:
    print("Error: Could not find DSN.")
