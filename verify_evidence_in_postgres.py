import psycopg
import os
import sys
import sys
sys.path.append('/opt/linkx-worker/src')
from batch_manager import config_defaults

# Try to load env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

dsn = os.getenv('LINKX_POSTGRES_DSN', "postgresql://postgres:postgres@172.27.23.106:5432/linkx")

try:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT response_payload 
                FROM link_analysis_evidence 
                ORDER BY analyzed_at DESC 
                LIMIT 5;
            """)
            rows = cur.fetchall()
            print('========================================')
            print('Latest 5 Anomalies in Postgres:')
            for row in rows:
                print(f' - Payload: {row[0]}')
            print('========================================')
except Exception as e:
    print(f'Error: {e}')
