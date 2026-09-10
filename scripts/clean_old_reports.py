import os
import psycopg2
from dotenv import load_dotenv

# Load credentials
load_dotenv("/opt/linkx-worker/.env")

dsn = os.environ.get("LINKX_POSTGRES_DSN")
if not dsn:
    print("Error: LINKX_POSTGRES_DSN not found in environment.")
    exit(1)

try:
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    
    # Check how many we are about to delete
    cur.execute("SELECT COUNT(*) FROM link_analysis_evidence WHERE event_type = 'analysis.link.flagged';")
    evidence_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM linkx_reports WHERE report_type = 'XVIGILANCE_FINDING';")
    report_count = cur.fetchone()[0]
    
    print(f"Found {evidence_count} old evidence records and {report_count} old summary reports.")
    
    # Delete them
    cur.execute("DELETE FROM link_analysis_evidence WHERE event_type = 'analysis.link.flagged';")
    cur.execute("DELETE FROM linkx_reports WHERE report_type = 'XVIGILANCE_FINDING';")
    
    conn.commit()
    print("Successfully wiped all historical anomaly reports. The database is clean and ready for the new ruleset!")
    
    cur.close()
    conn.close()
except Exception as e:
    print(f"Database error: {e}")
