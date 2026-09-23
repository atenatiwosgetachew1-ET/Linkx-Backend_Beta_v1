import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

new_fund_flow = """
def get_fund_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f'''
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count < 1000 AND NOT acc IN $pt

    // Directly index-match the inbound transactions
    MATCH (a:{label})
    WHERE ({scope_clause_a}) AND a.LOGICAL_BENACCOUNTNO = acc

    // Directly index-match the outbound transactions
    MATCH (b:{label})
    WHERE ({scope_clause_b}) AND b.LOGICAL_ACCOUNTNO = acc
      AND elementId(a) <> elementId(b)
      AND (
        coalesce(a.TRANSACTIONDATE, '') < coalesce(b.TRANSACTIONDATE, '')
        OR (
          coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
          AND coalesce(a.TRANSACTIONTIME, '') < coalesce(b.TRANSACTIONTIME, '')
        )
      )
      AND {trusted_pair_clause}
      {boundary_str}

    WITH a, b
    ORDER BY b.TRANSACTIONDATE ASC, b.TRANSACTIONTIME ASC
    WITH a, collect(b) AS downstream
    WITH a, downstream[..5] AS limited_downstream
    UNWIND limited_downstream AS b

    CALL {{
      WITH a, b
      MERGE (a)-[r:FUND_FLOW {{session_id:$session_id}}]->(b)
      SET r.is_evidence = true, r.anomaly_score = 0.5, r.bgcolor = '#d8a822', r.provisional = {prov_str},
          r.reason = 'beneficiary later acts as sender',
          r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    }} IN TRANSACTIONS OF 5000 ROWS
    '''
"""

start_marker = "def get_fund_flow_query"
end_marker = "def get_dormant_to_active_query"

start_idx = content.find(start_marker)
end_idx = content.find(end_marker)

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + new_fund_flow.strip() + "\n\n" + content[end_idx:]
    with open(file_path, 'w') as f:
        f.write(content)
    print("Replaced FUND_FLOW query.")
else:
    print("Could not find FUND_FLOW block!")
