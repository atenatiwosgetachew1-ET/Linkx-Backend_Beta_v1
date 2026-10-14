import sys

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

aggregator_code = """
def get_fraud_aggregator_query(label, scope_clause_t, session_id=None):
    return f'''
    MATCH (t:{label})-[r]->()
    WHERE ({scope_clause_t}) 
      AND r.is_evidence = true
    WITH coalesce(t.LOGICAL_ACCOUNTNO, t.ACCOUNTNO) AS account_no,
         sum(r.anomaly_score) AS total_score,
         collect(distinct type(r)) AS evidence_types,
         count(r) AS evidence_count
    WHERE total_score >= 1.0  // Configurable threshold for Fraud Alert
      AND account_no IS NOT NULL AND account_no <> ''
    MERGE (a:AccountAlert {{account_no: account_no, session_id: $session_id}})
    SET a.total_score = total_score,
        a.evidence_types = evidence_types,
        a.evidence_count = evidence_count,
        a.created_at = datetime(),
        a.alert_type = 'FRAUD_ALERT_TARGET'
    
    // Link the alert back to the transactions
    WITH a, account_no
    MATCH (t:{label})
    WHERE ({scope_clause_t}) 
      AND coalesce(t.LOGICAL_ACCOUNTNO, t.ACCOUNTNO) = account_no
    MERGE (a)-[fa:FRAUD_ALERT_TARGET {{session_id:$session_id}}]->(t)
    SET fa.bgcolor = '#ff0000', fa.directed_display = true, fa.reason = 'Aggregated Fraud Evidence'
    '''
"""

with open(file_path, 'a') as f:
    f.write(aggregator_code)

print("Appended aggregator rule.")
