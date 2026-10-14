import eventlet
eventlet.monkey_patch()
from psycopg_pool import ConnectionPool
import os
dsn = "postgresql://postgres:postgres@localhost:5432/postgres" # fake
try:
    pool = ConnectionPool(dsn, min_size=1, max_size=1, timeout=2)
    with pool.connection() as conn:
        print("Got connection")
except Exception as e:
    print(f"Error: {e}")
