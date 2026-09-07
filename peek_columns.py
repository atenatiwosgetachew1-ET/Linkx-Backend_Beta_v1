import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment from the root
load_dotenv("/opt/Linkx_xmaintenance/.env")

# Must import after loading env
from linkx_xvigilance.config import get_xvigilance_config
from linkx_xvigilance.fetcher import stream_window_records

config = get_xvigilance_config()
start = datetime.strptime("2026-03-01 10:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
end = datetime.strptime("2026-03-01 11:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

for page in stream_window_records(config, start, end):
    if page:
        print("\n=== ELASTICSEARCH RAW COLUMNS ===")
        for col in page[0].keys():
            print(f"- {col}")
        
        print("\n=== SAMPLE ELASTICSEARCH RECORD ===")
        print(json.dumps(page[0], indent=2))
        break
