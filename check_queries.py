import os
from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
user = "neo4j"
password = os.environ.get("LINKX_NEO4J_PASSWORD", "linkx_xvigilance_secure")

driver = GraphDatabase.driver(uri, auth=(user, password))

with driver.session() as session:
    res = session.run("SHOW TRANSACTIONS YIELD transactionId, currentQuery, elapsedTime, status")
    for record in res:
        query = record["currentQuery"]
        if query and "SHOW TRANSACTIONS" not in query:
            print(f"[{record['elapsedTime']}] {record['status']} - {query[:200]}...")
driver.close()
