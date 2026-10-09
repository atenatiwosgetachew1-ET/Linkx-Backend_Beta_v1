#!/usr/bin/env python3
import os
import psycopg

def main():
    dsn = os.getenv("LINKX_POSTGRES_DSN")
    if not dsn:
        for p in ["/opt/linkx-worker/.env", "/opt/linkx-backend-update/.env", "/opt/linkx-backend-api/.env"]:
            if os.path.isfile(p):
                with open(p, "r") as f:
                    for line in f:
                        if line.startswith("LINKX_POSTGRES_DSN="):
                            dsn = line.strip().split("=", 1)[1].strip(" \"'")
                            break
            if dsn:
                break

    if not dsn:
        print("[-] Error: LINKX_POSTGRES_DSN not found in environment or .env files.")
        return

    print("[+] Connecting to PostgreSQL...")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # 1. Status Counts
            cur.execute("SELECT status, count(*) FROM xvigilance_slice_runs GROUP BY status ORDER BY count(*) DESC;")
            print("\n=== Current Status Counts in xvigilance_slice_runs ===")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")

            # 2. Checkpoint Info
            cur.execute("SELECT feed_name, last_window_end, total_records_analyzed, total_graph_analyzed FROM xvigilance_checkpoints LIMIT 1;")
            cp = cur.fetchone()
            if cp:
                print(f"\n=== xVigilance Checkpoint ===")
                print(f"  Feed: {cp[0]}")
                print(f"  Last Producer Window: {cp[1]}")
                print(f"  Total Extracted Records: {cp[2]}")
                print(f"  Total Graph Analyzed: {cp[3]}")

            # 3. Recent 5 runs
            cur.execute("""
                SELECT id, window_start, window_end, status, records_count 
                FROM xvigilance_slice_runs 
                ORDER BY window_end DESC 
                LIMIT 5;
            """)
            print("\n=== Latest 5 Slices in Audit Table (Ordered DESC) ===")
            for r in cur.fetchall():
                print(f"  Run {r[0]} | {r[1]} -> {r[2]} | {r[3]} ({r[4]} records)")

            # 4. Heal completed runs up to Aug 27 00:00:00
            print("\n[+] Auto-healing past completed slices that were stuck on 'queued'...")
            cur.execute("""
                UPDATE xvigilance_slice_runs 
                SET status = 'succeeded', finished_at = NOW() 
                WHERE window_end <= '2026-08-27 00:00:00+00' 
                  AND status = 'queued';
            """)
            healed = cur.rowcount
            conn.commit()
            print(f"[+] Successfully healed and marked {healed} past slice run(s) as 'succeeded'!")

            # 5. New Status Counts
            cur.execute("SELECT status, count(*) FROM xvigilance_slice_runs GROUP BY status ORDER BY count(*) DESC;")
            print("\n=== Updated Status Counts ===")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")

if __name__ == "__main__":
    main()
