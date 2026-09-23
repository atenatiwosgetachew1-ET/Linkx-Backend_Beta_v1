import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

new_aggregator = """
def get_fraud_aggregator_query(label, scope_clause_t, session_id=None):
    return f'''
    MATCH (t:{label})-[r:SMURFING|CIRCULAR_FLOW|FUND_FLOW|DORMANT_TO_ACTIVE|ABNORMAL_BALANCE_CHANGE|HUB_AND_SPOKE|SHARED_IDENTIFIER|LATE_NIGHT_TX|JUST_BELOW_THRESHOLD|RAPID_WITHDRAWAL|ACCOUNT_ACTIVITY_SPIKE|HIGH_RISK_LINK]->()
    WHERE ({scope_clause_t}) 
      AND r.is_evidence = true
    WITH coalesce(t.LOGICAL_ACCOUNTNO, t.ACCOUNTNO) AS account_no,
         sum(r.anomaly_score) AS total_score,
         collect(distinct type(r)) AS evidence_types,
         count(r) AS evidence_count,
         collect(distinct t) AS involved_tx_nodes
    WHERE total_score >= 1.0
      AND account_no IS NOT NULL AND account_no <> ''
    
    CALL {{
      WITH account_no, total_score, evidence_types, evidence_count, involved_tx_nodes
      MERGE (a:AccountAlert {{account_no: account_no, session_id: $session_id}})
      SET a.total_score = total_score,
          a.evidence_types = evidence_types,
          a.evidence_count = evidence_count,
          a.created_at = datetime(),
          a.alert_type = 'FRAUD_ALERT_TARGET'
      
      WITH a, involved_tx_nodes
      UNWIND involved_tx_nodes AS t
      MERGE (a)-[fa:FRAUD_ALERT_TARGET {{session_id:$session_id}}]->(t)
      SET fa.bgcolor = '#ff0000', fa.directed_display = true, fa.reason = 'Aggregated Fraud Evidence'
    }} IN TRANSACTIONS OF 100 ROWS
    '''
"""

start_marker = "def get_fraud_aggregator_query"

start_idx = content.find(start_marker)

if start_idx != -1:
    content = content[:start_idx] + new_aggregator.strip() + "\n"
    with open(file_path, 'w') as f:
        f.write(content)
    print("Fixed FRAUD_AGGREGATOR to pass nodes directly.")
else:
    print("Could not find block!")
