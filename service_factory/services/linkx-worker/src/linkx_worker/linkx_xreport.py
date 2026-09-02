import os
import sys
import time
import json
import traceback

src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from batch_manager.utils.postgres_utils import get_postgres_connection

# In a real scenario, this would post to an actual parent gateway or webhook URL
# Using a dummy URL for now.
DEFAULT_PARENT_GATEWAY_URL = os.getenv("LINKX_PARENT_GATEWAY_URL", "http://parent-gateway.local/api/sync")

def process_outbox_batch():
    batch_size = 50
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            # Select pending or failed reports (max 3 attempts)
            cur.execute("""
                SELECT id, report_id, target_system, payload, attempts
                FROM report_sync_outbox
                WHERE status IN ('PENDING', 'FAILED') AND attempts < 3
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            """, (batch_size,))
            
            records = cur.fetchall()
            if not records:
                return 0
                
            for record in records:
                outbox_id, report_id, target_system, payload, attempts = record
                
                try:
                    # SIMULATED HTTP REQUEST
                    import requests
                    # response = requests.post(DEFAULT_PARENT_GATEWAY_URL, json=payload, timeout=5)
                    # response.raise_for_status()
                    
                    # Update status to SUCCESS
                    cur.execute("""
                        UPDATE report_sync_outbox
                        SET status = 'SUCCESS', attempts = attempts + 1, updated_at = NOW()
                        WHERE id = %s
                    """, (outbox_id,))
                    print(f"[OutboxRelay] Successfully synced report {report_id} to {target_system}", flush=True)
                except Exception as e:
                    error_msg = str(e)
                    # If conflict error from parent (e.g., 409 Conflict), mark as CONFLICT
                    if "409" in error_msg:
                        new_status = 'CONFLICT'
                    else:
                        new_status = 'FAILED'
                        
                    cur.execute("""
                        UPDATE report_sync_outbox
                        SET status = %s, attempts = attempts + 1, last_error = %s, updated_at = NOW()
                        WHERE id = %s
                    """, (new_status, error_msg, outbox_id))
                    print(f"[OutboxRelay] Failed to sync report {report_id}: {error_msg}", flush=True)
            conn.commit()
            return len(records)

def run_daemon():
    print("[OutboxRelay] Starting report outbox relay daemon...", flush=True)
    while True:
        try:
            processed = process_outbox_batch()
            if processed == 0:
                time.sleep(5)
            else:
                time.sleep(1)
        except Exception as e:
            print(f"[OutboxRelay] Error in relay loop: {e}", flush=True)
            traceback.print_exc()
            time.sleep(10)

if __name__ == "__main__":
    run_daemon()
