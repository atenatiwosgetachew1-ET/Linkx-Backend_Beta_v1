import psycopg, os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv('/opt/linkx-worker/src/.env')

# Check reports
conn = psycopg.connect(os.getenv('LINKX_POSTGRES_DSN'))
cur = conn.cursor()
cur.execute("SELECT count(*), max(created_at) FROM linkx_reports WHERE report_type = 'XVIGILANCE_FINDING'")
count, last = cur.fetchone()
print(f'Total XVIGILANCE reports: {count}, Last created: {last}')
conn.close()

# Wipe Neo4j
driver = GraphDatabase.driver('bolt://172.27.23.85:7687', auth=('neo4j', 'Linkx@123'))
with driver.session() as s:
    s.run("MATCH (n) DETACH DELETE n")
print('Neo4j wiped clean!')
driver.close()
