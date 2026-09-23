import sys

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
    print("\n--- Top 20 BUSINESS MOBILES ---")
    res1 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        WHERE t.BUSINESSMOBILENO IS NOT NULL AND t.BUSINESSMOBILENO <> '' AND toLower(t.BUSINESSMOBILENO) <> 'null'
        RETURN t.BUSINESSMOBILENO AS phone, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 20
    """)
    for r in res1:
        print(f"{r['cnt']:>6} uses -> '{r['phone']}'")
        
    print("\n--- Top 20 BEN TEL NOs ---")
    res2 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        WHERE t.BENTELNO IS NOT NULL AND t.BENTELNO <> '' AND toLower(t.BENTELNO) <> 'null'
        RETURN t.BENTELNO AS phone, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 20
    """)
    for r in res2:
        print(f"{r['cnt']:>6} uses -> '{r['phone']}'")

driver.close()
print("\nProfiling complete.")
