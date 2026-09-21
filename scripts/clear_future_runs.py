import psycopg, os

dsn = None
with open('/opt/linkx-worker/.env', 'r') as f:
    for line in f:
        if line.startswith('LINKX_POSTGRES_DSN='):
            dsn = line.strip().split('=', 1)[1].strip(' "\'')

conn=psycopg.connect(dsn)
cur=conn.cursor()
cur.execute("DELETE FROM xvigilance_slice_runs WHERE window_end > '2026-08-24 00:00:00+00';")
deleted = cur.rowcount
conn.commit()
cur.close()
conn.close()
print(f"Deleted {deleted} future audit log rows!")
