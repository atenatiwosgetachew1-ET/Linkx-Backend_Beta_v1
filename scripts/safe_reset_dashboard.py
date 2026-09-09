import psycopg, os

dsn = None
with open('/opt/linkx-worker/.env', 'r') as f:
    for line in f:
        if line.startswith('LINKX_POSTGRES_DSN='):
            dsn = line.strip().split('=', 1)[1].strip(' "\'')

conn = psycopg.connect(dsn)
cur = conn.cursor()

# 1. Reset the massive counter back to 0
cur.execute("UPDATE xvigilance_checkpoints SET total_records_analyzed = 0 WHERE feed_name = 'hourly_transaction_detective';")

# 2. Wipe the old slice history from the dashboard
cur.execute("DELETE FROM xvigilance_slice_runs;")

conn.commit()
cur.close()
conn.close()
print('Dashboard metrics and history cleanly reset to zero!')
