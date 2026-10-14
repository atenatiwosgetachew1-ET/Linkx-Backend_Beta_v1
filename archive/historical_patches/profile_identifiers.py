import sys
import os

# We will just run Cypher via python using the native neo4j driver inside linkx-worker
# We can use the risk_scoring_kafka_service's create_neo4j_driver by setting PYTHONPATH
sys.path.append("/var/www/linkx-backend/service_factory/services/linkx-worker/src")
try:
    from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver
except Exception as e:
    print(f"Failed to import: {e}")
    sys.exit(1)

driver = create_neo4j_driver()
with driver.session() as s:
    print("--- Top 15 Device IDs ---")
    res1 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        WHERE t.DEVICE_ID IS NOT NULL AND t.DEVICE_ID <> ''
        RETURN t.DEVICE_ID AS device, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 15
    """)
    for r in res1:
        print(f"{r['cnt']} uses -> '{r['device']}'")
        
    print("\n--- Top 15 IP Addresses ---")
    res2 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        WHERE t.IP_ADDRESS IS NOT NULL AND t.IP_ADDRESS <> ''
        RETURN t.IP_ADDRESS AS ip, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 15
    """)
    for r in res2:
        print(f"{r['cnt']} uses -> '{r['ip']}'")

driver.close()
