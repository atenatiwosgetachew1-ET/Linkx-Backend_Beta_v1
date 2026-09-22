import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

def replace_block(pattern_start, pattern_end, replacement, text):
    pattern = pattern_start + r".*?" + pattern_end
    return re.sub(pattern, replacement, text, flags=re.DOTALL)

new_si_query = '''def get_shared_identifier_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"""
        MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} 
        WITH [{{kind:'BUSINESSMOBILENO', value:seed.BUSINESSMOBILENO}}, {{kind:'BENTELNO', value:seed.BENTELNO}}] AS identifiers 
        UNWIND identifiers AS seed_identifier
        WITH DISTINCT seed_identifier.kind AS identifier_type, trim(toString(seed_identifier.value)) AS identifier_value
        WHERE identifier_value <> ''
        """
        match_filters = "AND (t.BUSINESSMOBILENO = identifier_value OR t.BENTELNO = identifier_value)"

    with_identifiers = "identifier_type, identifier_value" if incremental_batch_id else "[{kind:'BUSINESSMOBILENO', value:t.BUSINESSMOBILENO, account:t.LOGICAL_ACCOUNTNO}, {kind:'BENTELNO', value:t.BENTELNO, account:t.LOGICAL_BENACCOUNTNO}] AS identifiers"
    with_account = "WITH identifier_type, identifier_value, CASE WHEN t.BUSINESSMOBILENO = identifier_value THEN t.LOGICAL_ACCOUNTNO ELSE t.LOGICAL_BENACCOUNTNO END AS account, t" if incremental_batch_id else "UNWIND identifiers AS identifier WITH identifier.kind AS identifier_type, trim(toString(identifier.value)) AS identifier_value, identifier.account AS account, t"

    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {match_filters}
    WITH t, {with_identifiers}
    {with_account}
    WHERE identifier_value <> '' AND account IS NOT NULL AND account <> ''
    WITH identifier_type, identifier_value, collect(DISTINCT account) AS accounts, collect(DISTINCT t) AS txns
    WHERE size(accounts) >= 2 AND size(txns) < 1000
    CALL (txns, identifier_type, identifier_value, accounts) {{
      UNWIND range(0, size(txns)-2) AS i
      WITH txns[i] AS a, txns[i+1] AS b, identifier_type, identifier_value, accounts
      MERGE (a)-[r:SHARED_IDENTIFIER {{session_id:$session_id}}]->(b)
      SET r.bgcolor = '#0d898a', r.textcolor = '#eeeeee', r.provisional = {prov_str},
          r.reason = 'same identifier appears on multiple accounts',
          r.identifier_type = identifier_type, r.identifier_value = identifier_value,
          r.account_count = size(accounts),
          r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
    }} IN TRANSACTIONS OF 1000 ROWS
    """'''

text = replace_block(r"def get_shared_identifier_query\(label, scope_clause_t, is_provisional=False, incremental_batch_id=None\):", r"\} IN TRANSACTIONS OF 1000 ROWS\n    \"\"\"", new_si_query, text)

new_rw_query = '''def get_rapid_withdrawal_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} WITH DISTINCT seed.LOGICAL_ACCOUNTNO AS acc, seed.TRANSACTIONDATE AS tx_day WHERE acc IS NOT NULL AND acc <> '' AND tx_day IS NOT NULL AND tx_day <> '' AND NOT acc IN $pt"
        match_filters = "AND (t.LOGICAL_ACCOUNTNO = acc OR t.LOGICAL_BENACCOUNTNO = acc) AND t.TRANSACTIONDATE = tx_day"

    where_filter = match_filters if incremental_batch_id else "AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> '' AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> '' AND NOT t.LOGICAL_ACCOUNTNO IN $pt"
    with_acc = "acc, tx_day," if incremental_batch_id else "t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day,"

    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {where_filter}
    WITH {with_acc} collect(t) AS txns
    WHERE size(txns) >= 2 AND size(txns) < 1000
    CALL (txns, acc, tx_day) {{
      UNWIND txns AS t1
      UNWIND txns AS t2
      WITH t1, t2, acc, tx_day
      WHERE elementId(t1) < elementId(t2)
        AND t1.LOGICAL_BENACCOUNTNO = acc
        AND t2.LOGICAL_ACCOUNTNO = acc
        AND coalesce(t1.TRANSACTIONTIME, '') <= coalesce(t2.TRANSACTIONTIME, '')
      WITH t1, t2, acc, tx_day,
           coalesce(toFloat(t1.AMOUNTINBIRR), toFloat(t1.AMOUNT), toFloat(t1.LOCAL_AMOUNT), 0.0) AS in_amt,
           coalesce(toFloat(t2.AMOUNTINBIRR), toFloat(t2.AMOUNT), toFloat(t2.LOCAL_AMOUNT), 0.0) AS out_amt
      WHERE in_amt > 0 AND out_amt > 0
        AND abs(in_amt - out_amt) <= (in_amt * $rapid_withdrawal_amount_tolerance)
      MERGE (t1)-[r:RAPID_WITHDRAWAL {{session_id:$session_id}}]->(t2)
      SET r.bgcolor = '#e07624', r.textcolor = '#eeeeee', r.provisional = {prov_str},
          r.reason = 'funds rapidly withdrawn or passed through on same day',
          r.in_amount = in_amt, r.out_amount = out_amt,
          r.edge_semantic = 'OBSERVED_FLOW', r.financial_flow = true, r.directed_display = true
    }} IN TRANSACTIONS OF 1000 ROWS
    """'''

text = replace_block(r"def get_rapid_withdrawal_query\(label, scope_clause_t, is_provisional=False, incremental_batch_id=None\):", r"\} IN TRANSACTIONS OF 1000 ROWS\n    \"\"\"", new_rw_query, text)

new_aas_query = '''def get_account_activity_spike_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} WITH DISTINCT seed.LOGICAL_ACCOUNTNO AS acc, seed.TRANSACTIONDATE AS tx_day WHERE acc IS NOT NULL AND acc <> '' AND tx_day IS NOT NULL AND tx_day <> '' AND NOT acc IN $pt"
        match_filters = "AND t.LOGICAL_ACCOUNTNO = acc AND t.TRANSACTIONDATE = tx_day"
        
    where_filter = match_filters if incremental_batch_id else "AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> '' AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> '' AND NOT t.LOGICAL_ACCOUNTNO IN $pt"
    with_acc = "acc, tx_day," if incremental_batch_id else "t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day,"

    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {where_filter}
    WITH {with_acc} count(t) AS daily_count, collect(t) AS txns
    WHERE daily_count >= $activity_spike_min_daily_count AND daily_count < 2000
    WITH acc, tx_day, daily_count, txns
    MATCH (history:{label} {{LOGICAL_ACCOUNTNO: acc}})
    WHERE coalesce(history.IGNORE_LOGICAL, false) = false
      AND history.TRANSACTIONDATE IS NOT NULL AND history.TRANSACTIONDATE <> tx_day
    WITH acc, tx_day, daily_count, txns, count(history) AS hist_count, count(DISTINCT history.TRANSACTIONDATE) AS hist_days
    WHERE hist_days > 0
    WITH acc, tx_day, daily_count, txns, (toFloat(hist_count) / hist_days) AS avg_daily
    WHERE daily_count > (avg_daily * $activity_spike_multiplier)
    CALL (txns, daily_count, avg_daily) {{
      UNWIND txns AS t
      MERGE (t)-[r:ACCOUNT_ACTIVITY_SPIKE {{session_id:$session_id}}]->(t)
      SET r.bgcolor = '#99153c', r.textcolor = '#eeeeee', r.provisional = {prov_str},
          r.reason = 'unusually high transaction volume for this account on this day',
          r.daily_count = daily_count, r.avg_daily = avg_daily,
          r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
    }} IN TRANSACTIONS OF 1000 ROWS
    """'''

text = replace_block(r"def get_account_activity_spike_query\(label, scope_clause_t, is_provisional=False, incremental_batch_id=None\):", r"\} IN TRANSACTIONS OF 1000 ROWS\n    \"\"\"", new_aas_query, text)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(text)

