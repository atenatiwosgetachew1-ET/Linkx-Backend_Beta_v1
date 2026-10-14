from textwrap import dedent

circular_flow = """def get_circular_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f\"\"\"
    CALL {{
      MATCH (t:{label})
      WHERE ({scope_clause_t})
        AND coalesce(t.IGNORE_LOGICAL, false) = false
        AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
      RETURN t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    }}
    WITH acc, out_count WHERE out_count < 1000 AND NOT acc IN $pt

    CALL {{
      WITH acc
      MATCH (a:{label} {{ACCOUNTNO: acc}})
      WHERE ({scope_clause_a})
        AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
        AND NOT a.LOGICAL_BENACCOUNTNO IN $pt
      RETURN a
    }}
    
    CALL {{
      WITH a
      MATCH (b:{label} {{ACCOUNTNO: a.LOGICAL_BENACCOUNTNO, BENACCOUNTNO: a.LOGICAL_ACCOUNTNO}})
      WHERE ({scope_clause_b})
        AND elementId(a) < elementId(b)
        AND coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
        AND {trusted_pair_clause}
        {boundary_str}
      RETURN b
    }}

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

print(circular_flow)
