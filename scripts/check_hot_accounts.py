import sys
sys.path.append('/opt/linkx-worker/src')
from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver, _neo4j_credentials

creds = _neo4j_credentials('128778')
driver = create_neo4j_driver(creds)
with driver.session() as s:
    r = s.run('MATCH (n:`bank_transactions_xvigilance-daemon`) RETURN count(n) AS total, count(DISTINCT n.LOGICAL_ACCOUNTNO) AS accounts')
    row = r.single()
    print(f"Total nodes: {row['total']}")
    print(f"Distinct accounts: {row['accounts']}")
    
    r2 = s.run('MATCH (n:`bank_transactions_xvigilance-daemon`) WITH n.LOGICAL_ACCOUNTNO AS acc, count(n) AS cnt WHERE cnt > 500 RETURN acc, cnt ORDER BY cnt DESC LIMIT 10')
    print("\\nHot accounts (>500 txns):")
    for rec in r2:
        print(f"  {rec['acc']}: {rec['cnt']} txns")
driver.close()
