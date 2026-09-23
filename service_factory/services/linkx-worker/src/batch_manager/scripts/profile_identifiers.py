import sys

sys.path.append("/opt/linkx-worker/src")
from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver, _neo4j_credentials

credentials = _neo4j_credentials('128778')
if not credentials:
    sys.exit(1)

driver = create_neo4j_driver(credentials)

with driver.session() as s:
    print("\n--- Node Keys (Sample 1) ---")
    res1 = s.run("MATCH (t:`bank_transactions_xvigilance-daemon`) RETURN keys(t) AS k LIMIT 1")
    for r in res1:
        print(r['k'])
        
    print("\n--- Top 10 BUSINESS MOBILES (No string functions) ---")
    res2 = s.run("""
        MATCH (t:`bank_transactions_xvigilance-daemon`)
        RETURN t.BUSINESSMOBILENO AS phone, count(t) AS cnt
        ORDER BY cnt DESC LIMIT 10
    """)
    for r in res2:
        print(f"{r['cnt']:>6} uses -> '{r['phone']}'")

driver.close()
