import psycopg, os
from dotenv import load_dotenv

load_dotenv('/opt/linkx-worker/src/.env')
conn = psycopg.connect(os.getenv('LINKX_POSTGRES_DSN'))
cur = conn.cursor()
cur.execute("DELETE FROM linkx_reports WHERE report_type = 'XVIGILANCE_FINDING';")
cur.execute("DELETE FROM link_analysis_evidence WHERE session_id = 'XVIGILANCE_FINDINGS';")
print(f"Clean Slate! Deleted {cur.rowcount} evidence graphs.")
conn.commit()
cur.close()
conn.close()
