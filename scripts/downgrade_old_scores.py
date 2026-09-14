import os
import psycopg
from dotenv import load_dotenv

# Load credentials from Node 21 worker environment
load_dotenv("/opt/linkx-worker/.env")

dsn = os.environ.get("LINKX_POSTGRES_DSN")
if not dsn:
    print("Error: LINKX_POSTGRES_DSN not found. Make sure you are running this on Node-21.")
    exit(1)

try:
    print("Connecting to database...")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            
            # 1. Check how many records match
            cur.execute("""
                SELECT COUNT(*) FROM linkx_reports 
                WHERE report_type = 'XVIGILANCE_FINDING' 
                AND created_at < '2026-09-10 12:51:00'
                AND payload->>'fraud_score' IS NOT NULL;
            """)
            record_count = cur.fetchone()[0]
            
            if record_count == 0:
                print("No xVigilance reports found matching the criteria (before 2026-09-10 12:51:00 with a fraud_score).")
            else:
                print(f"Found {record_count} historical anomaly reports. Applying -10 fraud score penalty...")
                
                # 2. Execute the JSONB Update
                update_query = """
                    UPDATE linkx_reports
                    SET payload = jsonb_set(
                                    jsonb_set(payload::jsonb, '{fraud_score}', to_jsonb(GREATEST(0, CAST(payload->>'fraud_score' AS NUMERIC) - 10))),
                                    '{score_band}', 
                                    to_jsonb(
                                        CASE 
                                           WHEN GREATEST(0, CAST(payload->>'fraud_score' AS NUMERIC) - 10) >= 80 THEN 'Critical'
                                           WHEN GREATEST(0, CAST(payload->>'fraud_score' AS NUMERIC) - 10) >= 50 THEN 'High'
                                           WHEN GREATEST(0, CAST(payload->>'fraud_score' AS NUMERIC) - 10) >= 20 THEN 'Medium'
                                           ELSE 'Low'
                                        END
                                    )
                                )
                    WHERE report_type = 'XVIGILANCE_FINDING' 
                    AND created_at < '2026-09-10 12:51:00'
                    AND payload->>'fraud_score' IS NOT NULL;
                """
                cur.execute(update_query)
                conn.commit()
                
                print(f"Successfully downgraded the fraud scores and recalculated the banding for {cur.rowcount} records!")

except Exception as e:
    print(f"Database error: {e}")
