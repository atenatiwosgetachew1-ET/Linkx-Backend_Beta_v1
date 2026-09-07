import os
import sys
import json
import getpass

# Add the src directory to sys.path
sys.path.append("/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src")
sys.path.append("/var/www/linkx-backend/service_factory/services/linkx-api/src")
sys.path.append("/var/www/linkx-backend/service_factory/services/linkx-worker/src")

print("--- XVigilance Database Override Test ---")
print("To test this, we need to connect to your live PostgreSQL database.")
print("The standard LinkX database user is 'linkx'.")
db_host = input("Enter the database IP (leave blank for 127.0.0.1): ").strip() or "127.0.0.1"
db_pass = getpass.getpass("Enter the password for the 'linkx' user: ")

# Set the environment variable so our config.py and db.py can use it
os.environ["LINKX_POSTGRES_DSN"] = f"postgresql://linkx:{db_pass}@{db_host}/linkx"
os.environ["LINKX_ES_DIRECT_BASE_URL"] = "http://172.27.23.141:9200"

from linkx_xvigilance.db import connect
from linkx_xvigilance.config import get_xvigilance_config

print("\n[1] Connecting to database and inserting a test override for xvigilance...")
try:
    with connect(application_name="test-script") as conn:
        with conn.cursor() as cur:
            # We insert a crazy page size (7777) into the database for the xvigilance_system session
            test_config = {"es_scroll_page_size": 7777}
            cur.execute(
                """
                INSERT INTO session_configs (session_id, window_id, config)
                VALUES ('xvigilance_system', '', %s::jsonb)
                ON CONFLICT (session_id, window_id) DO UPDATE 
                SET config = EXCLUDED.config
                """,
                (json.dumps(test_config),)
            )
        conn.commit()
    print("✅ Successfully inserted database override: es_scroll_page_size = 7777")
except Exception as e:
    print(f"❌ Failed to connect or insert into database: {e}")
    sys.exit(1)

print("\n[2] Calling get_xvigilance_config()...")
config = get_xvigilance_config()

print("\n[3] Result:")
if config.get("es_scroll_page_size") == 7777:
    print("🎉 SUCCESS! The config dynamically loaded the override from the PostgreSQL database!")
else:
    print(f"❌ FAILED. Expected 7777, but got {config.get('es_scroll_page_size')}")

print("\n[4] Cleaning up test data...")
try:
    with connect(application_name="test-script") as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM session_configs WHERE session_id = 'xvigilance_system'")
        conn.commit()
    print("✅ Cleaned up successfully.")
except Exception as e:
    print(f"Warning: Failed to clean up: {e}")
