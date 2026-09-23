import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

# In get_circular_flow_query, we change:
# MATCH (b:{label})
# WHERE ({scope_clause_b}) AND b.ACCOUNTNO IN target_b_accs
# WITH list_a, collect(b) AS list_b
#
# TO:
# MATCH (b:{label})
# WHERE ({scope_clause_b}) AND b.ACCOUNTNO IN target_b_accs AND b.BENACCOUNTNO = acc
# WITH acc, list_a, collect(b) AS list_b

# Also, in the WHERE clause below, we can optimize by ensuring we keep acc.

new_circular_flow = """
def get_circular_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f'''
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count < 1000

    // Partition by trigger account to prevent Cartesian explosion
    MATCH (a:{label})
    WHERE ({scope_clause_a}) AND a.ACCOUNTNO = acc
      AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
      AND NOT a.LOGICAL_BENACCOUNTNO IN $pt
    WITH acc, collect(a) AS list_a
    WITH acc, list_a, [x IN list_a | x.LOGICAL_BENACCOUNTNO] AS target_b_accs

    MATCH (b:{label})
    WHERE ({scope_clause_b}) AND b.ACCOUNTNO IN target_b_accs AND b.BENACCOUNTNO = acc
    WITH list_a, collect(b) AS list_b

    UNWIND list_a AS a
    UNWIND list_b AS b
    WITH a, b, 
         coalesce(toFloat(a.AMOUNTINBIRR), toFloat(a.AMOUNT), toFloat(a.amount), toFloat(a.LOCAL_AMOUNT), 0.0) AS amt_a,
         coalesce(toFloat(b.AMOUNTINBIRR), toFloat(b.AMOUNT), toFloat(b.amount), toFloat(b.LOCAL_AMOUNT), 0.0) AS amt_b
    WHERE elementId(a) < elementId(b)
      AND b.ACCOUNTNO = a.LOGICAL_BENACCOUNTNO 
      AND b.BENACCOUNTNO = a.LOGICAL_ACCOUNTNO
      AND coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
      AND amt_a > 0 AND amt_b > 0
      AND abs(amt_a - amt_b) <= (amt_a * 0.05)
      AND {trusted_pair_clause}
      {boundary_str}

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

# Replace the old function block entirely
old_func_start = "def get_circular_flow_query"
end_of_func = "def get_fund_flow_query"

start_idx = content.find(old_func_start)
end_idx = content.find(end_of_func)

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + new_circular_flow.strip() + "\n\n" + content[end_idx:]
    with open(file_path, 'w') as f:
        f.write(content)
    print("Fixed get_circular_flow_query")
else:
    print("Could not find blocks")
