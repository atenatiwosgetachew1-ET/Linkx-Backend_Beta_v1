from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver
import json

q = """
EXPLAIN
MATCH (a:`bank_transactions_xvigilance-daemon`)
WHERE a.session_id = 'test'
WITH a.LOGICAL_ACCOUNTNO AS acc, a.LOGICAL_BENACCOUNTNO AS ben, coalesce(a.TRANSACTIONDATE, '') AS tdate, count(a) AS a_count
WHERE a_count < 1000
MATCH (b:`bank_transactions_xvigilance-daemon`)
WHERE b.session_id = 'test'
  AND b.LOGICAL_ACCOUNTNO = ben AND b.LOGICAL_BENACCOUNTNO = acc
  AND coalesce(b.TRANSACTIONDATE, '') = tdate
WITH acc, ben, tdate, a_count, count(b) AS b_count
WHERE b_count > 0 AND b_count < 1000
MATCH (a:`bank_transactions_xvigilance-daemon` {ACCOUNTNO: acc, BENACCOUNTNO: ben})
WHERE a.session_id = 'test' AND coalesce(a.TRANSACTIONDATE, '') = tdate
MATCH (b:`bank_transactions_xvigilance-daemon` {ACCOUNTNO: ben, BENACCOUNTNO: acc})
WHERE b.session_id = 'test' AND coalesce(b.TRANSACTIONDATE, '') = tdate
  AND elementId(a) < elementId(b)
CALL {
  WITH a, b
  RETURN a.ACCOUNTNO as val
} IN TRANSACTIONS OF 10000 ROWS
RETURN val
"""
try:
    driver = create_neo4j_driver()
    with driver.session() as s:
        res = s.run(q)
        print("Valid syntax!")
except Exception as e:
    print("Error:", e)
