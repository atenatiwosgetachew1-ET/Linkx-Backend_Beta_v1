import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

# Fix CIRCULAR_FLOW
new_circular = """
def get_circular_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f'''
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count <= 50

    MATCH (a:{label})
    WHERE ({scope_clause_a}) AND a.ACCOUNTNO = acc
      AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
      AND NOT a.LOGICAL_BENACCOUNTNO IN $pt
      
    // Force planner to resolve 'a' before scanning 'b'
    WITH acc, a

    MATCH (b:{label})
    WHERE ({scope_clause_b}) 
      AND b.ACCOUNTNO = a.LOGICAL_BENACCOUNTNO 
      AND b.BENACCOUNTNO = acc
      AND elementId(a) < elementId(b)
      AND coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
      AND {trusted_pair_clause}
      {boundary_str}

    WITH a, b, 
         coalesce(toFloat(a.AMOUNTINBIRR), toFloat(a.AMOUNT), toFloat(a.amount), toFloat(a.LOCAL_AMOUNT), 0.0) AS amt_a,
         coalesce(toFloat(b.AMOUNTINBIRR), toFloat(b.AMOUNT), toFloat(b.amount), toFloat(b.LOCAL_AMOUNT), 0.0) AS amt_b
    WHERE amt_a > 0 AND amt_b > 0
      AND abs(amt_a - amt_b) <= (amt_a * 0.05)

    CALL {{
      WITH a, b
      MERGE (a)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(b)
      SET r1.is_evidence = true, r1.anomaly_score = 0.6, r1.bgcolor = '#e6e6e6', r1.provisional = {prov_str}, r1.reason = 'same-day reverse transfer pair',
          r1.edge_semantic = 'OBSERVED_FLOW', r1.financial_flow = true, r1.directed_display = true
      MERGE (b)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(a)
      SET r2.is_evidence = true, r2.anomaly_score = 0.6, r2.bgcolor = '#e6e6e6', r2.provisional = {prov_str}, r2.reason = 'same-day reverse transfer pair',
          r2.edge_semantic = 'OBSERVED_FLOW', r2.financial_flow = true, r2.directed_display = true
    }} IN TRANSACTIONS OF 5000 ROWS
    '''
"""

# Fix FUND_FLOW
new_fund = """
def get_fund_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f'''
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count <= 50 AND NOT acc IN $pt

    MATCH (a:{label})
    WHERE ({scope_clause_a}) AND a.LOGICAL_BENACCOUNTNO = acc
    
    // Force planner to resolve 'a' before scanning 'b'
    WITH acc, a

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

start_circ = content.find("def get_circular_flow_query")
end_circ = content.find("def get_fund_flow_query")
end_fund = content.find("def get_dormant_to_active_query")

if start_circ != -1 and end_circ != -1 and end_fund != -1:
    content = content[:start_circ] + new_circular.strip() + "\n\n" + new_fund.strip() + "\n\n" + content[end_fund:]
    with open(file_path, 'w') as f:
        f.write(content)
    print("Replaced CIRCULAR_FLOW and FUND_FLOW to force eager planner.")
else:
    print("Could not find blocks!")
