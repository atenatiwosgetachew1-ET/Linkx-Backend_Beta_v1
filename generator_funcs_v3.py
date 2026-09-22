def get_smurfing_query(label, scope_clause_t, trusted_pair_clause, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_t_filters = ""
    if incremental_batch_id:
        seed_block = f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = {incremental_batch_id}
        WITH DISTINCT seed.LOGICAL_ACCOUNTNO AS acc, seed.LOGICAL_BENACCOUNTNO AS beneficiary, seed.TRANSACTIONDATE AS tx_day
        WHERE acc IS NOT NULL AND acc <> ''
          AND beneficiary IS NOT NULL AND beneficiary <> ''
          AND tx_day IS NOT NULL AND tx_day <> ''
        """
        match_t_filters = "AND t.LOGICAL_ACCOUNTNO = acc AND t.LOGICAL_BENACCOUNTNO = beneficiary AND t.TRANSACTIONDATE = tx_day"
    
    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {match_t_filters}
    WITH
        {"t.LOGICAL_ACCOUNTNO AS acc," if not incremental_batch_id else "acc,"}
        {"t.LOGICAL_BENACCOUNTNO AS beneficiary," if not incremental_batch_id else "beneficiary,"}
        {"t.TRANSACTIONDATE AS tx_day," if not incremental_batch_id else "tx_day,"}
        t, coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amount
    WHERE {"acc IS NOT NULL AND acc <> '' AND beneficiary IS NOT NULL AND beneficiary <> '' AND tx_day IS NOT NULL AND tx_day <> '' AND " if not incremental_batch_id else ""} amount IS NOT NULL AND amount > 0 AND amount < $smurfing_single_tx_threshold
    WITH acc, beneficiary, tx_day, t, amount
    ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
    WITH acc, beneficiary, tx_day, collect(t) AS txns, sum(amount) AS total_amount, count(t) AS tx_count
    WHERE tx_count >= $smurfing_min_tx_count AND total_amount >= $smurfing_cumulative_threshold
    UNWIND range(0, size(txns)-2) AS i
    WITH txns[i] AS a, txns[i+1] AS b, acc, beneficiary, tx_day, tx_count, total_amount
    WHERE {trusted_pair_clause}
    MERGE (a)-[r:SMURFING {{session_id:$session_id}}]->(b)
    SET r.bgcolor = '#d5d276', r.provisional = {prov_str},
        r.reason = 'multiple small same-day transfers below threshold',
        r.account = acc, r.beneficiary = beneficiary, r.tx_day = tx_day,
        r.tx_count = tx_count, r.total_amount = total_amount,
        r.single_tx_threshold = $smurfing_single_tx_threshold, r.total_threshold = $smurfing_cumulative_threshold,
        r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    """

