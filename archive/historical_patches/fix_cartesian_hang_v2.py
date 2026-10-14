import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

def replace_func(func_name, new_code, text):
    pattern = r"def " + func_name + r"\(.*?\):\n.*?return f\"\"\"\n.*?(?=\n\s*\"\"\")\n\s*\"\"\""
    return re.sub(pattern, new_code, text, flags=re.DOTALL)


circular_flow = """def get_circular_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f\"\"\"
    // 1. Find potential pairs efficiently using aggregation
    MATCH (a:{label})
    WHERE ({scope_clause_a})
      AND coalesce(a.IGNORE_LOGICAL, false) = false
      AND a.LOGICAL_ACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO IS NOT NULL
      AND NOT a.LOGICAL_ACCOUNTNO IN $pt AND NOT a.LOGICAL_BENACCOUNTNO IN $pt
    WITH a.ACCOUNTNO AS a_acc, a.LOGICAL_BENACCOUNTNO AS a_lben, a.LOGICAL_ACCOUNTNO AS a_lacc, coalesce(a.TRANSACTIONDATE, '') AS tdate, count(a) AS a_count
    WHERE a_count < 1000

    // 2. Ensure reverse flow exists on the same day
    MATCH (b:{label})
    WHERE ({scope_clause_b})
      AND b.ACCOUNTNO = a_lben AND b.BENACCOUNTNO = a_lacc
      AND coalesce(b.TRANSACTIONDATE, '') = tdate
    WITH a_acc, a_lben, a_lacc, tdate, a_count, count(b) AS b_count
    WHERE b_count > 0 AND b_count < 1000

    // 3. Match actual pairs and create edges
    MATCH (a:{label} {{ACCOUNTNO: a_acc, LOGICAL_BENACCOUNTNO: a_lben, LOGICAL_ACCOUNTNO: a_lacc}})
    WHERE ({scope_clause_a}) AND coalesce(a.TRANSACTIONDATE, '') = tdate
    MATCH (b:{label} {{ACCOUNTNO: a_lben, BENACCOUNTNO: a_lacc}})
    WHERE ({scope_clause_b}) AND coalesce(b.TRANSACTIONDATE, '') = tdate
      AND elementId(a) < elementId(b)
      AND {trusted_pair_clause}
      {boundary_str}

    CALL {{
      WITH a, b
      MERGE (a)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(b)
      SET r1.bgcolor = '#e6e6e6', r1.provisional = {prov_str}, r1.reason = 'same-day reverse transfer pair',
          r1.edge_semantic = 'OBSERVED_FLOW', r1.financial_flow = true, r1.directed_display = true
      MERGE (b)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(a)
      SET r2.bgcolor = '#e6e6e6', r2.provisional = {prov_str}, r2.reason = 'same-day reverse transfer pair',
          r2.edge_semantic = 'OBSERVED_FLOW', r2.financial_flow = true, r2.directed_display = true
    }} IN TRANSACTIONS OF 5000 ROWS
    \"\"\""""


fund_flow = """def get_fund_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f\"\"\"
    // 1. Find nodes acting as an intermediary (received funds, then sent funds)
    MATCH (a:{label})
    WHERE ({scope_clause_a})
      AND coalesce(a.IGNORE_LOGICAL, false) = false
      AND a.LOGICAL_ACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO IS NOT NULL
      AND NOT a.LOGICAL_ACCOUNTNO IN $pt AND NOT a.LOGICAL_BENACCOUNTNO IN $pt
    WITH a.LOGICAL_BENACCOUNTNO AS intermediary, count(a) AS in_count
    WHERE in_count < 1000

    // 2. Validate intermediary has outgoing flows
    MATCH (b:{label})
    WHERE ({scope_clause_b})
      AND b.LOGICAL_ACCOUNTNO = intermediary
    WITH intermediary, in_count, count(b) AS out_count
    WHERE out_count > 0 AND out_count < 1000

    // 3. Match actual flows
    MATCH (a:{label} {{LOGICAL_BENACCOUNTNO: intermediary}})
    WHERE ({scope_clause_a})
    MATCH (b:{label} {{LOGICAL_ACCOUNTNO: intermediary}})
    WHERE ({scope_clause_b})
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
      SET r.bgcolor = '#d8a822', r.provisional = {prov_str},
          r.reason = 'beneficiary becomes sender subsequently',
          r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    }} IN TRANSACTIONS OF 5000 ROWS
    \"\"\""""

text = replace_func("get_circular_flow_query", circular_flow, text)
text = replace_func("get_fund_flow_query", fund_flow, text)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(text)

