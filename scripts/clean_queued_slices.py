#!/usr/bin/env python3
import os

try:
    import psycopg
except ImportError:
    import psycopg2 as psycopg

def main():
    dsn = os.getenv("LINKX_POSTGRES_DSN")
    if not dsn:
        for p in ["/opt/linkx-worker/.env", "/opt/Linkx_xmaintenance/.env", "/opt/linkx-backend-update/.env"]:
            if os.path.isfile(p):
                with open(p, "r") as f:
                    for line in f:
                        if line.startswith("LINKX_POSTGRES_DSN="):
                            dsn = line.strip().split("=", 1)[1].strip(" \"'")
                            break
            if dsn:
                break

    if not dsn:
        print("[-] Error: LINKX_POSTGRES_DSN not found.")
        return

    print("[+] Connecting to PostgreSQL...")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # 1. Delete all queued runs from the audit table
            cur.execute("DELETE FROM xvigilance_slice_runs WHERE status = 'queued';")
            deleted = cur.rowcount
            print(f"[+] Deleted {deleted} 'queued' slice runs from audit table.")

            # 2. Also remove any empty zombie 'succeeded' runs created in future
            cur.execute("DELETE FROM xvigilance_slice_runs WHERE window_start > NOW();")
            if cur.rowcount > 0:
                print(f"[+] Cleaned {cur.rowcount} future placeholder rows.")

            # 3. Find the latest genuinely completed window with data
            cur.execute("""
                SELECT max(window_end) 
                FROM xvigilance_slice_runs 
                WHERE status = 'succeeded' AND records_count > 0;
            """)
            last_succeeded_window = cur.fetchone()[0]
            print(f"[+] Latest completed window with transactions: {last_succeeded_window}")

            # 4. Set checkpoint last_window_end to that window
            if last_succeeded_window:
                cur.execute("""
                    UPDATE xvigilance_checkpoints 
                    SET last_window_end = %s 
                    WHERE feed_name = 'hourly_transaction_detective';
                """, (last_succeeded_window,))
                print(f"[+] Aligned producer checkpoint (last_window_end) to {last_succeeded_window}")

            conn.commit()

            # 5. Print final status summary
            cur.execute("SELECT status, count(*) FROM xvigilance_slice_runs GROUP BY status ORDER BY count(*) DESC;")
            print("\n=== Cleaned Status Counts in Database ===")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")

if __name__ == "__main__":
    main()
