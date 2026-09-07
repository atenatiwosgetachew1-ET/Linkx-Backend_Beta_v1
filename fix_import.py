import re

filepath = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py"
with open(filepath, "r") as f:
    code = f.read()

# Replace the bad import with psycopg
code = code.replace("from db import get_pg_connection", "import psycopg")

# Replace get_pg_connection() with psycopg.connect(os.getenv("LINKX_POSTGRES_DSN"))
code = code.replace("get_pg_connection()", "psycopg.connect(os.getenv('LINKX_POSTGRES_DSN'))")

with open(filepath, "w") as f:
    f.write(code)

print("Fixed imports!")
