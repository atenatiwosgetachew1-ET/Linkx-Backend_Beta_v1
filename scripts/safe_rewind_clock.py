import psycopg, os
from dotenv import load_dotenv

load_dotenv('/opt/linkx-worker/src/.env')
conn=psycopg.connect(os.getenv('LINKX_POSTGRES_DSN'))
cur=conn.cursor()
cur.execute("UPDATE xvigilance_checkpoints SET last_window_end = '2025-08-31 22:00:00+00' WHERE feed_name = 'hourly_transaction_detective';")
conn.commit()
cur.close()
conn.close()
print('Clock successfully rewound to 2025-08-31 22:00:00 UTC!')
