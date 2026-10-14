def get_smurfing_query(label, scope_clause, trusted_pair_clause):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
    WITH
        t.LOGICAL_ACCOUNTNO AS acc,
        t.LOGICAL_BENACCOUNTNO AS beneficiary,
        t.TRANSACTIONDATE AS tx_day,
        t,
        coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amount
    WHERE acc IS NOT NULL AND acc <> ''
      AND beneficiary IS NOT NULL AND beneficiary <> ''
      AND tx_day IS NOT NULL AND tx_day <> ''
      AND amount IS NOT NULL AND amount > 0 AND amount < $smurfing_single_tx_threshold
    WITH acc, beneficiary, tx_day, t, amount
    ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
    WITH acc, beneficiary, tx_day, collect(t) AS txns, sum(amount) AS total_amount, count(t) AS tx_count
    WHERE tx_count >= $smurfing_min_tx_count AND total_amount >= $smurfing_cumulative_threshold
    UNWIND range(0, size(txns)-2) AS i
    WITH txns[i] AS a, txns[i+1] AS b, acc, beneficiary, tx_day, tx_count, total_amount
    WHERE {trusted_pair_clause}
    MERGE (a)-[r:SMURFING {{session_id:$session_id}}]->(b)
    SET r.bgcolor = '#d5d276', r.provisional = false,
        r.reason = 'multiple small same-day transfers below threshold',
        r.account = acc, r.beneficiary = beneficiary, r.tx_day = tx_day,
        r.tx_count = tx_count, r.total_amount = total_amount,
        r.single_tx_threshold = $smurfing_single_tx_threshold, r.total_threshold = $smurfing_cumulative_threshold,
        r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    """

def get_circular_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count < 1000

    MATCH (a:{label} {{ACCOUNTNO: acc}})
    WHERE ({scope_clause_a})
      AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
      AND NOT a.LOGICAL_BENACCOUNTNO IN $pt
    CALL (a) {{
      MATCH (b:{label} {{ACCOUNTNO: a.LOGICAL_BENACCOUNTNO, BENACCOUNTNO: a.LOGICAL_ACCOUNTNO}})
      WHERE ({scope_clause_b})
        AND elementId(a) < elementId(b)
        AND coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
        AND {trusted_pair_clause}
      MERGE (a)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(b)
      SET r1.bgcolor = '#e6e6e6', r1.provisional = false, r1.reason = 'same-day reverse transfer pair',
          r1.edge_semantic = 'OBSERVED_FLOW', r1.financial_flow = true, r1.directed_display = true
      MERGE (b)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(a)
      SET r2.bgcolor = '#e6e6e6', r2.provisional = false, r2.reason = 'same-day reverse transfer pair',
          r2.edge_semantic = 'OBSERVED_FLOW', r2.financial_flow = true, r2.directed_display = true
    }} IN TRANSACTIONS OF 1000 ROWS
    """

def get_fund_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count < 1000 AND NOT acc IN $pt

    MATCH (a:{label} {{LOGICAL_BENACCOUNTNO: acc}})
    WHERE ({scope_clause_a})
    CALL (a, acc) {{
      MATCH (b:{label} {{LOGICAL_ACCOUNTNO: acc}})
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
      WITH a, b
      ORDER BY b.TRANSACTIONDATE ASC, b.TRANSACTIONTIME ASC
      WITH a, collect(b) AS downstream
      WITH a, downstream[..5] AS limited_downstream
      UNWIND limited_downstream AS b
      MERGE (a)-[r:FUND_FLOW {{session_id:$session_id}}]->(b)
      SET r.bgcolor = '#d8a822', r.provisional = false,
          r.reason = 'beneficiary later acts as sender',
          r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    }} IN TRANSACTIONS OF 1000 ROWS
    """

def get_dormant_to_active_query(label, scope_clause_t):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant'
      AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active'
    MERGE (t)-[r:DORMANT_TO_ACTIVE {{session_id:$session_id}}]->(t)
    SET r.bgcolor = '#c20f0f', r.textcolor = '#eeeeee', r.provisional = false,
        r.reason = 'dormant source account transacts with active beneficiary',
        r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
    """

def get_abnormal_balance_query(label, scope_clause_t):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, t
    ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
    WITH acc, collect(t) AS txns
    WHERE size(txns) >= 5
    UNWIND range(3, size(txns)-1) AS i
    WITH txns[i] AS current, txns[i-1] AS previous, txns[0..i] AS history
    WITH current, previous,
         abs(coalesce(toFloat(current.SENDERPREVIOUSBALANCE), 0.0) - coalesce(toFloat(previous.SENDERPREVIOUSBALANCE), 0.0)) AS current_change,
         history
    WHERE current_change > 0
    WITH current, previous, current_change,
         reduce(s = 0.0, x IN history | s + abs(coalesce(toFloat(x.SENDERPREVIOUSBALANCE), 0.0))) / size(history) AS avg_change
    WHERE current_change > (avg_change * 3)
    MERGE (previous)-[r:ABNORMAL_BALANCE_CHANGE {{session_id:$session_id}}]->(current)
    SET r.bgcolor = '#196e08', r.textcolor = '#eeeeee', r.provisional = false,
        r.reason = 'balance change exceeds recent account baseline',
        r.change = current_change, r.average_recent_change = avg_change, r.threshold_multiplier = 3,
        r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    """

def get_hub_and_spoke_out_query(label, scope_clause_t, trusted_pair_clause):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> ''
      AND t.LOGICAL_BENACCOUNTNO IS NOT NULL AND t.LOGICAL_BENACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS hub, t.TRANSACTIONDATE AS tx_day, collect(t) AS txns, count(DISTINCT t.LOGICAL_BENACCOUNTNO) AS spoke_count
    WHERE hub IS NOT NULL AND hub <> '' AND NOT hub IN $pt AND spoke_count >= $hub_spoke_min_counterparties AND size(txns) < 1000
    CALL (txns, hub, tx_day, spoke_count) {{
      UNWIND range(0, size(txns)-2) AS i
      WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
      WHERE {trusted_pair_clause}
      MERGE (a)-[r:HUB_AND_SPOKE {{session_id:$session_id}}]->(b)
      SET r.bgcolor = '#6f42c1', r.textcolor = '#eeeeee', r.provisional = false,
          r.reason = 'account connects with multiple counterparties on same day',
          r.hub_account = hub, r.direction = 'outgoing', r.tx_day = tx_day, r.spoke_count = spoke_count,
          r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
    }} IN TRANSACTIONS OF 1000 ROWS
    """

def get_hub_and_spoke_in_query(label, scope_clause_t, trusted_pair_clause):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> ''
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_BENACCOUNTNO AS hub, t.TRANSACTIONDATE AS tx_day, collect(t) AS txns, count(DISTINCT t.LOGICAL_ACCOUNTNO) AS spoke_count
    WHERE hub IS NOT NULL AND hub <> '' AND NOT hub IN $pt AND spoke_count >= $hub_spoke_min_counterparties AND size(txns) < 1000
    CALL (txns, hub, tx_day, spoke_count) {{
      UNWIND range(0, size(txns)-2) AS i
      WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
      WHERE {trusted_pair_clause}
      MERGE (a)-[r:HUB_AND_SPOKE {{session_id:$session_id}}]->(b)
      SET r.bgcolor = '#6f42c1', r.textcolor = '#eeeeee', r.provisional = false,
          r.reason = 'account connects with multiple counterparties on same day',
          r.hub_account = hub, r.direction = 'incoming', r.tx_day = tx_day, r.spoke_count = spoke_count,
          r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
    }} IN TRANSACTIONS OF 1000 ROWS
    """

def get_shared_identifier_query(label, scope_clause_t):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
    WITH t,
         [{{kind:'BUSINESSMOBILENO', value:t.BUSINESSMOBILENO, account:t.LOGICAL_ACCOUNTNO}},
          {{kind:'BENTELNO', value:t.BENTELNO, account:t.LOGICAL_BENACCOUNTNO}}] AS identifiers
    UNWIND identifiers AS identifier
    WITH identifier.kind AS identifier_type,
         trim(toString(identifier.value)) AS identifier_value,
         identifier.account AS account, t
    WHERE identifier_value <> '' AND account IS NOT NULL AND account <> ''
    WITH identifier_type, identifier_value, collect(DISTINCT account) AS accounts, collect(DISTINCT t) AS txns
    WHERE size(accounts) >= 2 AND size(txns) < 1000
    CALL (txns, identifier_type, identifier_value, accounts) {{
      UNWIND range(0, size(txns)-2) AS i
      WITH txns[i] AS a, txns[i+1] AS b, identifier_type, identifier_value, accounts
      MERGE (a)-[r:SHARED_IDENTIFIER {{session_id:$session_id}}]->(b)
      SET r.bgcolor = '#0d898a', r.textcolor = '#eeeeee', r.provisional = false,
          r.reason = 'same identifier appears on multiple accounts',
          r.identifier_type = identifier_type, r.identifier_value = identifier_value,
          r.account_count = size(accounts),
          r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
    }} IN TRANSACTIONS OF 1000 ROWS
    """

def get_rapid_withdrawal_query(label, scope_clause_t):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> ''
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
      AND NOT t.LOGICAL_ACCOUNTNO IN $pt
    WITH t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day, collect(t) AS txns
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
      SET r.bgcolor = '#e07624', r.textcolor = '#eeeeee', r.provisional = false,
          r.reason = 'funds rapidly withdrawn or passed through on same day',
          r.in_amount = in_amt, r.out_amount = out_amt,
          r.edge_semantic = 'OBSERVED_FLOW', r.financial_flow = true, r.directed_display = true
    }} IN TRANSACTIONS OF 1000 ROWS
    """

def get_account_activity_spike_query(label, scope_clause_t):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> ''
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
      AND NOT t.LOGICAL_ACCOUNTNO IN $pt
    WITH t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day, count(t) AS daily_count, collect(t) AS txns
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
      SET r.bgcolor = '#99153c', r.textcolor = '#eeeeee', r.provisional = false,
          r.reason = 'unusually high transaction volume for this account on this day',
          r.daily_count = daily_count, r.avg_daily = avg_daily,
          r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
    }} IN TRANSACTIONS OF 1000 ROWS
    """

def get_high_risk_link_query(label, scope_clause_t):
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
    UNWIND $risk_entries AS risk_entity
    WITH t, risk_entity
    WHERE
       (risk_entity.account IS NOT NULL AND risk_entity.account <> '' AND (t.LOGICAL_ACCOUNTNO = risk_entity.account OR t.LOGICAL_BENACCOUNTNO = risk_entity.account)) OR
       (risk_entity.phone IS NOT NULL AND risk_entity.phone <> '' AND (t.BUSINESSMOBILENO = risk_entity.phone OR t.BENTELNO = risk_entity.phone)) OR
       (risk_entity.name IS NOT NULL AND risk_entity.name <> '' AND (toLower(t.SENDER_FULL_NAME) = toLower(risk_entity.name) OR toLower(t.RECEIVER_FULL_NAME) = toLower(risk_entity.name)))
    WITH t, collect(DISTINCT toUpper(risk_entity.category)) AS matched_categories
    WHERE size(matched_categories) > 0
    CALL (t, matched_categories) {{
      UNWIND matched_categories AS cat
      FOREACH (ignore IN CASE WHEN cat IN ['PEP', 'SANCTION', 'SANCTIONS', 'SANCTIONED'] THEN [1] ELSE [] END |
          MERGE (t)-[r:HIGH_RISK_LINK {{session_id:$session_id}}]->(t)
          SET r.bgcolor = '#de7d07', r.provisional = false, r.reason = 'Configured risk entity matched', r.risk_source = 'risk_entities', r.category = cat,
              r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
      )
    }} IN TRANSACTIONS OF 1000 ROWS
    """
