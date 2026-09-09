import os
from neo4j import GraphDatabase

uri = os.getenv("LINKX_NEO4J_URI", "bolt://172.27.23.85:7687")
user = os.getenv("LINKX_NEO4J_USER", "neo4j")
password = os.getenv("LINKX_NEO4J_PASSWORD", "linkx")

# Override to local Node 22 IP just in case
uri = "bolt://172.27.23.85:7687"

print(f"Connecting to Neo4j at {uri}...")
driver = GraphDatabase.driver(uri, auth=(user, password))

queries = [
    "CREATE INDEX idx_node_id IF NOT EXISTS FOR (n:bank_transactions_xvigilance_daemon) ON (n.NodeId)",
    "CREATE INDEX idx_batch_id IF NOT EXISTS FOR (n:bank_transactions_xvigilance_daemon) ON (n.batch_id)",
    "CREATE INDEX idx_account_no IF NOT EXISTS FOR (n:bank_transactions_xvigilance_daemon) ON (n.ACCOUNTNO)",
    "CREATE INDEX idx_ben_account_no IF NOT EXISTS FOR (n:bank_transactions_xvigilance_daemon) ON (n.BENACCOUNTNO)",
    "CREATE INDEX idx_tx_date IF NOT EXISTS FOR (n:bank_transactions_xvigilance_daemon) ON (n.TRANSACTIONDATE)"
]

with driver.session() as session:
    for q in queries:
        print(f"Executing: {q}")
        session.run(q)

print("All performance indexes created successfully!")
driver.close()
