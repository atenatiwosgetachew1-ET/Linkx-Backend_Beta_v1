from neo4j import GraphDatabase
import os

def wipe():
    uri = "bolt://172.27.23.106:7687"
    user = "neo4j"
    password = "change-me"
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    print("Connected to Neo4j. Starting emergency wipe...")
    
    deleted_total = 0
    with driver.session() as session:
        while True:
            # Delete nodes for xvigilance-daemon with backticks
            result = session.run("MATCH (n:`bank_transactions_xvigilance-daemon`) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS deleted")
            deleted = result.single()["deleted"]
            deleted_total += deleted
            print(f"Deleted {deleted} nodes...")
            if deleted == 0:
                break
    
    print(f"Emergency wipe complete. Total nodes deleted: {deleted_total}")
    driver.close()

if __name__ == "__main__":
    wipe()
