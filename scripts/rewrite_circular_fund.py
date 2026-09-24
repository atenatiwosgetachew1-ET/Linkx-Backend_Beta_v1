import re
import sys

RULES_FILE = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"

with open(RULES_FILE, "r") as f:
    content = f.read()

# -------------------------------------------------------------
# Rewrite get_circular_flow_query
# -------------------------------------------------------------
circular_new = '''def get_circular_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count < 1000 AND NOT acc IN $pt
    WITH collect(acc) AS eligible_accounts

    CALL {{
        MATCH (x:{label})
        WHERE ({scope_clause_t})
          AND coalesce(x.IGNORE_LOGICAL, false) = false
          AND x.ACCOUNTNO IS NOT NULL AND x.ACCOUNTNO <> ''
        WITH x.ACCOUNTNO AS account, count(x) AS outgoing_count
        WHERE outgoing_count < 1000 AND NOT account IN $pt
        RETURN collect(account) AS eligible_beneficiaries
    }}

    UNWIND eligible_accounts AS acc

    MATCH (a:{label})
    WHERE ({scope_clause_a}) AND a.ACCOUNTNO = acc
      AND coalesce(a.IGNORE_LOGICAL, false) = false
      AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
      AND a.LOGICAL_BENACCOUNTNO IN eligible_beneficiaries
      
    MATCH (b:{label})
    WHERE ({scope_clause_b}) 
      AND b.ACCOUNTNO = a.LOGICAL_BENACCOUNTNO 
      AND b.BENACCOUNTNO = acc
      AND coalesce(b.IGNORE_LOGICAL, false) = false
      AND elementId(a) < elementId(b)
      AND coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
      {boundary_str}

    WITH a, b, 
         coalesce(toFloat(a.AMOUNTINBIRR), toFloat(a.AMOUNT), toFloat(a.amount), toFloat(a.LOCAL_AMOUNT), 0.0) AS amt_a,
         coalesce(toFloat(b.AMOUNTINBIRR), toFloat(b.AMOUNT), toFloat(b.amount), toFloat(b.LOCAL_AMOUNT), 0.0) AS amt_b
    WHERE amt_a > 0 AND amt_b > 0
      AND abs(amt_a - amt_b) <= (amt_a * 0.05)
      AND {trusted_pair_clause}

    CALL {{
      WITH a, b
      MERGE (a)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(b)
      SET r1.is_evidence = true, r1.anomaly_score = 0.6, r1.bgcolor = '#e6e6e6', r1.provisional = {prov_str}, r1.reason = 'same-day reverse transfer pair',
          r1.edge_semantic = 'OBSERVED_FLOW', r1.financial_flow = true, r1.directed_display = true
      MERGE (b)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(a)
      SET r2.is_evidence = true, r2.anomaly_score = 0.6, r2.bgcolor = '#e6e6e6', r2.provisional = {prov_str}, r2.reason = 'same-day reverse transfer pair',
          r2.edge_semantic = 'OBSERVED_FLOW', r2.financial_flow = true, r2.directed_display = true
    }} IN TRANSACTIONS OF 5000 ROWS
    """
'''

# Replace get_circular_flow_query using regex
circular_pattern = re.compile(r'def get_circular_flow_query.*?(?=def get_fund_flow_query)', re.DOTALL)
content = circular_pattern.sub(circular_new + '\n', content)


# -------------------------------------------------------------
# Rewrite get_fund_flow_query
# -------------------------------------------------------------
fund_new = '''def get_fund_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False):
    prov_str = "true" if is_provisional else "false"
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count < 1000 AND NOT acc IN $pt
    WITH collect(acc) AS eligible_middlemen

    CALL {{
        MATCH (x:{label})
        WHERE ({scope_clause_t})
          AND coalesce(x.IGNORE_LOGICAL, false) = false
          AND x.ACCOUNTNO IS NOT NULL AND x.ACCOUNTNO <> ''
        WITH x.ACCOUNTNO AS account, count(x) AS outgoing_count
        WHERE outgoing_count < 1000 AND NOT account IN $pt
        RETURN collect(account) AS eligible_senders
    }}

    UNWIND eligible_middlemen AS acc

    MATCH (a:{label})
    WHERE ({scope_clause_a}) AND a.LOGICAL_BENACCOUNTNO = acc
      AND coalesce(a.IGNORE_LOGICAL, false) = false
      AND a.ACCOUNTNO IS NOT NULL AND a.ACCOUNTNO <> ''
      AND a.ACCOUNTNO IN eligible_senders
    
    MATCH (b:{label})
    WHERE ({scope_clause_b}) AND b.LOGICAL_ACCOUNTNO = acc
      AND coalesce(b.IGNORE_LOGICAL, false) = false
      AND elementId(a) <> elementId(b)
      AND (
        coalesce(a.TRANSACTIONDATE, '') < coalesce(b.TRANSACTIONDATE, '')
        OR (
          coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
          AND coalesce(a.TRANSACTIONTIME, '') < coalesce(b.TRANSACTIONTIME, '')
        )
      )
      AND {trusted_pair_clause}

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
    """
'''

fund_pattern = re.compile(r'def get_fund_flow_query.*?(?=def get_abnormal_balance_query)', re.DOTALL)
content = fund_pattern.sub(fund_new + '\n', content)

with open(RULES_FILE, "w") as f:
    f.write(content)

print("Rewrote CIRCULAR_FLOW and FUND_FLOW with precomputation architecture.")
