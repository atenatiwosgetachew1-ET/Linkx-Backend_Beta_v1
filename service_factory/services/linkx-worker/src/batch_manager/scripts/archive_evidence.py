import os
import json
import gzip
import argparse
from datetime import datetime, timedelta, timezone
import sys

# Ensure imports work when run as a standalone script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Explicitly load environment variables for standalone cron jobs
def _load_env_manually(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    os.environ[key.strip()] = val.strip().strip('"\'')

_load_env_manually('/opt/linkx-worker/.env')
_load_env_manually(os.path.join(os.path.dirname(__file__), '../../../../../.env'))
from batch_manager.utils.postgres_utils import get_postgres_connection

ARCHIVE_DIR = "/mnt/linkx-artifacts/evidence_archive"

def archive_old_evidence(days):
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            # Select heavy payloads older than cutoff
            cur.execute("""
                SELECT trace_id, analyzed_at, response_payload 
                FROM link_analysis_evidence 
                WHERE analyzed_at < %s AND response_payload::text != '{"archived": true}'
            """, (cutoff_date,))
            
            rows = cur.fetchall()
            archived_count = 0
            
            for row in rows:
                trace_id, analyzed_at, payload_data = row
                
                # Compress and save to artifacts directory
                file_path = os.path.join(ARCHIVE_DIR, f"{trace_id}_{analyzed_at.strftime('%Y%m%d')}.json.gz")
                with gzip.open(file_path, 'wt', encoding='utf-8') as gz:
                    gz.write(json.dumps(payload_data) if isinstance(payload_data, dict) else str(payload_data))
                
                # Drop the heavy payload from active DB, leaving a tiny tombstone so the UI knows it was archived
                cur.execute("""
                    UPDATE link_analysis_evidence 
                    SET response_payload = '{"archived": true, "message": "Graph securely archived to cold storage."}'::jsonb 
                    WHERE trace_id = %s
                """, (trace_id,))
                
                archived_count += 1
                
            conn.commit()
            print(f"[EvidenceArchiver] Successfully compressed and archived {archived_count} evidence graphs to {ARCHIVE_DIR}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=180, help='Archive records older than X days')
    args = parser.parse_args()
    
    print(f"[EvidenceArchiver] Scanning for graphs older than {args.days} days...")
    archive_old_evidence(args.days)
