import sys
import os

try:
    from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver, _neo4j_credentials
except ImportError as e:
    print(f"Import error: {e}. Please ensure PYTHONPATH is set to /opt/linkx-worker/src")
    sys.exit(1)

print("Fetching Neo4j credentials from environment/config...")
credentials = _neo4j_credentials(None)
if not credentials:
    print("Warning: _neo4j_credentials(None) returned empty. Attempting environment variables directly...")
    url = os.getenv("LINKX_NEO4J_URL", "bolt://localhost:7687")
    username = os.getenv("LINKX_NEO4J_USERNAME", "neo4j")
    password = os.getenv("LINKX_NEO4J_PASSWORD", "password")
    credentials = {"url": url, "username": username, "password": password}

print(f"Connecting to Neo4j at {credentials.get('url', credentials.get('uri', 'unknown'))}...")

try:
    driver = create_neo4j_driver(credentials)
except TypeError:
    driver = create_neo4j_driver()

with driver.session() as s:
    print("\n--- Top 20 Device IDs ---")
    res1 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        WHERE t.DEVICE_ID IS NOT NULL AND t.DEVICE_ID <> '' AND toLower(t.DEVICE_ID) <> 'null'
        RETURN t.DEVICE_ID AS device, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 20
    """)
    for r in res1:
        print(f"{r['cnt']:>6} uses -> '{r['device']}'")
        
    print("\n--- Top 20 IP Addresses ---")
    res2 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        WHERE t.IP_ADDRESS IS NOT NULL AND t.IP_ADDRESS <> '' AND toLower(t.IP_ADDRESS) <> 'null'
        RETURN t.IP_ADDRESS AS ip, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 20
    """)
    for r in res2:
        print(f"{r['cnt']:>6} uses -> '{r['ip']}'")

driver.close()
print("\nProfiling complete.")
