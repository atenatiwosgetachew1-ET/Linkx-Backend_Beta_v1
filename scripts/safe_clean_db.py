import psycopg, os

dsn = None
with open('/opt/linkx-worker/.env', 'r') as f:
    for line in f:
        if line.startswith('LINKX_POSTGRES_DSN='):
            dsn = line.strip().split('=', 1)[1].strip(' "\'')

if not dsn:
    print("Could not find LINKX_POSTGRES_DSN in .env")
    exit(1)

conn = psycopg.connect(dsn)
cur = conn.cursor()
cur.execute("DELETE FROM linkx_reports WHERE report_type = 'XVIGILANCE_FINDING';")
cur.execute("DELETE FROM link_analysis_evidence WHERE session_id = 'XVIGILANCE_FINDINGS';")
print(f"Clean Slate! Deleted {cur.rowcount} evidence graphs.")
conn.commit()
cur.close()
conn.close()
