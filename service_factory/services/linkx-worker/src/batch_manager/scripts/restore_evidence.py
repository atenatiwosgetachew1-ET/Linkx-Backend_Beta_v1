import os
import json
import gzip
import argparse
import sys
import glob

# Ensure imports work when run as a standalone script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Explicitly load environment variables
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

def restore_evidence(trace_id):
    # Find the matching archive file
    search_pattern = os.path.join(ARCHIVE_DIR, f"{trace_id}_*.json.gz")
    files = glob.glob(search_pattern)
    
    if not files:
        print(f"[RestoreError] No archive found for trace_id: {trace_id} in {ARCHIVE_DIR}")
        print("Note: If it was moved to the off-host disaster server, you must copy the .json.gz file back to this directory first.")
        return
        
    target_file = files[0]
    print(f"[EvidenceRestore] Found compressed archive: {target_file}")
    
    # Read the compressed JSON
    with gzip.open(target_file, 'rt', encoding='utf-8') as gz:
        payload_json = gz.read()
        
    # Inject it back into active PostgreSQL
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE link_analysis_evidence 
                SET response_payload = %s::jsonb 
                WHERE trace_id = %s
            """, (payload_json, trace_id))
            
            # Check if row actually exists
            if cur.rowcount == 0:
                print(f"[RestoreError] Trace ID {trace_id} does not exist in the database table anymore!")
                return
                
            conn.commit()
            
    print(f"[EvidenceRestore] SUCCESS! The graph for {trace_id} has been restored to the active database.")
    print("The frontend UI will now instantly render this graph again.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--trace-id', type=str, required=True, help='The trace_id of the evidence to restore')
    args = parser.parse_args()
    restore_evidence(args.trace_id)
