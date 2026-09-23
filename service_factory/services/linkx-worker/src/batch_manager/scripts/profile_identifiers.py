import sys

# Point to the live daemon's source directory
sys.path.append("/opt/linkx-worker/src")

from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver, _neo4j_credentials

print("Fetching Neo4j credentials using session ID 128778...")
credentials = _neo4j_credentials('128778')

if not credentials:
    print("Failed to get credentials.")
    sys.exit(1)

print(f"Connecting to Neo4j at {credentials.get('url')}...")
driver = create_neo4j_driver(credentials)

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
