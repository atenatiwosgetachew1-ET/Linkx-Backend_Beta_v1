import psycopg, os
from datetime import datetime

dsn = os.getenv("LINKX_POSTGRES_DSN")
if not dsn:
    with open("/opt/linkx-worker/.env") as f:
        for line in f:
            if line.startswith("LINKX_POSTGRES_DSN="):
                dsn = line.strip().split("=", 1)[1].strip('"').strip("'")
                break

if dsn:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT feed_name, last_window_end, total_records_analyzed, total_graph_analyzed FROM xvigilance_checkpoints LIMIT 1')
            print('\n' + '='*50)
            print('        CURRENT CHECKPOINT STATUS')
            print('='*50)
            for row in cur.fetchall():
                print(f'Feed Name:               {row[0]}')
                print(f'Last Window End:         {row[1]}')
                print(f'Total Ingested (Producer): {row[2]:,}')
                print(f'Total Graph Analyzed (Consumer): {row[3]:,}')
                
                if row[3] == row[2]:
                    print("\nSTATUS: PERFECTLY SYNCED ✅")
                else:
                    print("\nSTATUS: CONSUMER CATCHING UP ⏳")
            print('='*50 + '\n')
else:
    print("Error: Could not find DSN.")
