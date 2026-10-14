from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(user, password))

query = """
EXPLAIN MATCH (t:`bank_transactions_xvigilance-daemon`)-[r:SMURFING|CIRCULAR_FLOW|FUND_FLOW|DORMANT_TO_ACTIVE|ABNORMAL_BALANCE_CHANGE|HUB_AND_SPOKE|SHARED_IDENTIFIER|LATE_NIGHT_TX|JUST_BELOW_THRESHOLD|RAPID_WITHDRAWAL|ACCOUNT_ACTIVITY_SPIKE|HIGH_RISK_LINK]->()
WHERE ($session_id IS NULL OR t.session_id = $session_id) 
  AND r.is_evidence = true
WITH coalesce(t.LOGICAL_ACCOUNTNO, t.ACCOUNTNO) AS account_no,
     sum(r.anomaly_score) AS total_score,
     collect(distinct type(r)) AS evidence_types,
     count(r) AS evidence_count,
     collect(distinct elementId(t)) AS involved_tx_ids
WHERE total_score >= 1.0
  AND account_no IS NOT NULL AND account_no <> ''
CALL {
  WITH account_no, total_score, evidence_types, evidence_count, involved_tx_ids
  MERGE (a:AccountAlert {account_no: account_no, session_id: $session_id})
  SET a.total_score = total_score,
      a.evidence_types = evidence_types,
      a.evidence_count = evidence_count,
      a.created_at = datetime(),
      a.alert_type = 'FRAUD_ALERT_TARGET'
  
  WITH a, involved_tx_ids
  UNWIND involved_tx_ids AS tid
  MATCH (t:`bank_transactions_xvigilance-daemon`) WHERE elementId(t) = tid
  MERGE (a)-[fa:FRAUD_ALERT_TARGET {session_id:$session_id}]->(t)
  SET fa.bgcolor = '#ff0000', fa.directed_display = true, fa.reason = 'Aggregated Fraud Evidence'
} IN TRANSACTIONS OF 100 ROWS
"""

with driver.session() as s:
    result = s.run(query, session_id="test_sess")
    try:
        print(result.consume().plan)
    except Exception as e:
        print(e)

