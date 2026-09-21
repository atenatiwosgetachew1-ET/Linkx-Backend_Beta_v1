import psycopg, os

dsn = None
with open('/opt/linkx-worker/.env', 'r') as f:
    for line in f:
        if line.startswith('LINKX_POSTGRES_DSN='):
            dsn = line.strip().split('=', 1)[1].strip(' "\'')

conn = psycopg.connect(dsn)
cur = conn.cursor()

try:
    cur.execute("ALTER TABLE xvigilance_checkpoints ADD COLUMN is_paused BOOLEAN DEFAULT FALSE;")
    print("Added is_paused column to xvigilance_checkpoints.")
except psycopg.errors.DuplicateColumn:
    print("Column is_paused already exists.")
    conn.rollback()
except Exception as e:
    print(f"Error: {e}")

conn.commit()
cur.close()
conn.close()
