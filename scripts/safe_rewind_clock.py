import psycopg, os

dsn = None
with open('/opt/linkx-worker/.env', 'r') as f:
    for line in f:
        if line.startswith('LINKX_POSTGRES_DSN='):
            dsn = line.strip().split('=', 1)[1].strip(' "\'')

conn=psycopg.connect(dsn)
cur=conn.cursor()
cur.execute("UPDATE xvigilance_checkpoints SET last_window_end = '2025-08-31 22:00:00+00' WHERE feed_name = 'hourly_transaction_detective';")
conn.commit()
cur.close()
conn.close()
print('Clock successfully rewound to 2025-08-31 22:00:00 UTC!')
