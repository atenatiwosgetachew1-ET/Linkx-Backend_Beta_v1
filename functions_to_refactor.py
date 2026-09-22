def batch_graph_analysis_transactions(
    driver,
    log_file,
    session_id=None,
    nodes_label="Transactions",
    high_risk_accounts=None,
    threshold_multiplier=3,
    single_tx_threshold=10000,
    total_threshold=30000,
    min_tx_count=3,
    trusted_entities=None,
    risk_entities=None,
):
    if high_risk_accounts is None:
        high_risk_accounts = []

    log_writer(log_file, f"[{datetime.now()}] [Info] Starting transactions analysis")
    label = _safe_label(nodes_label)
    session_param = str(session_id) if session_id else ""
    trusted_entries = trusted_entities_cypher_entries(trusted_entities)
    risk_entries = risk_entities_cypher_entries(risk_entities)
    pass_through_accounts = _extract_pass_through_accounts(trusted_entities)

    with driver.session() as session:
        _create_transaction_indexes(session, nodes_label)
        if session_id:
            _clear_transaction_relationships(session, session_id)

        # ----------------------------
        # 0. EFFECTIVE_FLOW: trace funds through pass-through intermediaries
        # ----------------------------
        if pass_through_accounts:
            log_writer(log_file, f"[{datetime.now()}] [Info] Starting EFFECTIVE_FLOW rule (Python accelerated)")
            try:
                # 1. Fetch inbound
                inbound_res = session.run(f"MATCH (n:{label}) WHERE {_session_scope_clause('n')} AND n.BENACCOUNTNO IN $pt AND n.ACCOUNTNO IS NOT NULL AND n.ACCOUNTNO <> '' RETURN id(n) AS id, n.ACCOUNTNO AS acc, n.BENACCOUNTNO AS ben, n.TRANSACTIONDATE AS date, n.TRANSACTIONTIME AS time, coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt", session_param=session_param, pt=pass_through_accounts)
                inbounds = [dict(r) for r in inbound_res]
                
                # 2. Fetch outbound
                outbound_res = session.run(f"MATCH (n:{label}) WHERE {_session_scope_clause('n')} AND n.ACCOUNTNO IN $pt AND n.BENACCOUNTNO IS NOT NULL AND n.BENACCOUNTNO <> '' RETURN id(n) AS id, n.ACCOUNTNO AS acc, n.BENACCOUNTNO AS ben, n.TRANSACTIONDATE AS date, n.TRANSACTIONTIME AS time, coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt", session_param=session_param, pt=pass_through_accounts)
                outbounds = [dict(r) for r in outbound_res]
                
                from collections import defaultdict
                out_map = defaultdict(list)
                for o in outbounds:
                    out_map[(o["acc"], o["date"])].append(o)
                    
                for k in out_map:
                    out_map[k].sort(key=lambda x: str(x["time"]))
                    
                edges_to_create = []
                for i in inbounds:
                    candidates = out_map.get((i["ben"], i["date"]), [])
                    for o in candidates:
                        if o["ben"] != i["acc"] and str(o["time"]) >= str(i["time"]):
                            if i["amt"] > 0 and o["amt"] > 0 and abs(o["amt"] - i["amt"]) <= (i["amt"] * 0.1):
                                edges_to_create.append({
                                    "in_id": i["id"],
                                    "out_id": o["id"],
                                    "intermediary": i["ben"],
                                    "in_amt": i["amt"],
                                    "out_amt": o["amt"],
                                    "fee": i["amt"] - o["amt"],
                                    "sender": i["acc"],
                                    "receiver": o["ben"],
                                    "date": i["date"]
                                })
                                break
                                
                if edges_to_create:
                    session.run(f"UNWIND $edges AS e MATCH (inbound) WHERE id(inbound) = e.in_id MATCH (outbound) WHERE id(outbound) = e.out_id MERGE (inbound)-[r:EFFECTIVE_FLOW {{session_id:$session_id}}]->(outbound) SET r.intermediary = e.intermediary, r.hop_count = 2, r.in_amount = e.in_amt, r.out_amount = e.out_amt, r.fee_delta = e.fee, r.effective_sender = e.sender, r.effective_receiver = e.receiver, r.tx_date = e.date, r.bgcolor = '#9b59b6', r.textcolor = '#eeeeee', r.provisional = false, r.edge_semantic = 'EFFECTIVE_FLOW', r.financial_flow = true, r.directed_display = true", session_id=session_param, edges=edges_to_create)
                log_writer(log_file, f"[{datetime.now()}] [Info] EFFECTIVE_FLOW rule completed. Python matched {len(edges_to_create)} edges.")
            except Exception as e:
                log_writer(log_file, f"[{datetime.now()}] [Error] EFFECTIVE_FLOW rule failed: {e}")

        # ----------------------------
        # 0.5 LOGICAL TRANSACTION LAYER INITIALIZATION
        # ----------------------------
        session.run(f'''
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
        SET t.LOGICAL_ACCOUNTNO = coalesce(t.ACCOUNTNO, ''),
            t.LOGICAL_BENACCOUNTNO = coalesce(t.BENACCOUNTNO, ''),
            t.IGNORE_LOGICAL = false
        ''', session_id=session_param)
        
        if pass_through_accounts:
            session.run(f'''
            MATCH (inbound:{label})-[r:EFFECTIVE_FLOW]->(outbound:{label})
            WHERE ($session_id IS NULL OR {_session_scope_clause("inbound")})
            SET inbound.LOGICAL_BENACCOUNTNO = coalesce(outbound.BENACCOUNTNO, ''),
                outbound.IGNORE_LOGICAL = true
            ''', session_id=session_param)
        log_writer(log_file, f"[{datetime.now()}] [Info] Logical Layer initialized")

        # ----------------------------
        # 1. SMURFING: repeated small transfers from one account to one beneficiary
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
        WITH
            t.ACCOUNTNO AS acc,
            t.BENACCOUNTNO AS beneficiary,
            t.TRANSACTIONDATE AS tx_day,
            t,
            coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amount
        WHERE acc IS NOT NULL
          AND acc <> ''
          AND beneficiary IS NOT NULL
          AND beneficiary <> ''
          AND tx_day IS NOT NULL
          AND tx_day <> ''
          AND amount IS NOT NULL
          AND amount > 0
          AND amount < $single_tx_threshold
        WITH acc, beneficiary, tx_day, t, amount
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME 
        WITH acc, beneficiary, tx_day, collect(t) AS txns, sum(amount) AS total_amount, count(t) AS tx_count
        WHERE tx_count >= $min_tx_count
          AND total_amount >= $total_threshold
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, acc, beneficiary, tx_day, tx_count, total_amount
        WHERE {_trusted_pair_clause('a', 'b')}
        MERGE (a)-[r:SMURFING {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d5d276',
            r.provisional = false,
            r.reason = 'multiple small same-day transfers below threshold',
            r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true,
            r.account = acc,
            r.beneficiary = beneficiary,
            r.tx_day = tx_day,
            r.tx_count = tx_count,
            r.total_amount = total_amount,
            r.single_tx_threshold = $single_tx_threshold,
            r.total_threshold = $total_threshold
        """, session_id=session_param,
             trusted_entries=trusted_entries,
             single_tx_threshold=single_tx_threshold,
             total_threshold=total_threshold,
             min_tx_count=min_tx_count)

        # ----------------------------
        # 2. CIRCULAR_FLOW: direct account-to-beneficiary reversal
        # ----------------------------
        session.run(f"""
        MATCH (a:{label}), (b:{label})
        WHERE ($session_id IS NULL OR ({_session_scope_clause("a")} AND {_session_scope_clause("b")}))
          AND a.ACCOUNTNO = b.BENACCOUNTNO
          AND a.BENACCOUNTNO = b.ACCOUNTNO
          AND a.ACCOUNTNO IS NOT NULL
          AND a.ACCOUNTNO <> ''
          AND a.BENACCOUNTNO IS NOT NULL
          AND a.BENACCOUNTNO <> ''
          AND elementId(a) < elementId(b)
          AND coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
          AND {_trusted_pair_clause('a', 'b')}
        MERGE (a)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(b)
        SET r1.bgcolor = '#e6e6e6', r1.provisional = false, r1.reason = 'same-day reverse transfer pair'
        MERGE (b)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(a)
        SET r2.bgcolor = '#e6e6e6', r2.provisional = false, r2.reason = 'same-day reverse transfer pair'
        """, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 3. FUND_FLOW: beneficiary becomes sender in a later transaction
        # ----------------------------
        session.run(f"""
        MATCH (a:{label}), (b:{label})
        WHERE ($session_id IS NULL OR ({_session_scope_clause("a")} AND {_session_scope_clause("b")}))
          AND a.BENACCOUNTNO = b.ACCOUNTNO
          AND a.BENACCOUNTNO IS NOT NULL
          AND a.BENACCOUNTNO <> ''
          AND elementId(a) <> elementId(b)
          AND (
            coalesce(a.TRANSACTIONDATE, '') < coalesce(b.TRANSACTIONDATE, '')
            OR (
              coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
              AND coalesce(a.TRANSACTIONTIME, '') < coalesce(b.TRANSACTIONTIME, '')
            )
          )
        WITH a, b
        ORDER BY a.TRANSACTIONDATE, a.TRANSACTIONTIME, b.TRANSACTIONDATE, b.TRANSACTIONTIME
        WITH a, collect(b) AS downstream
        WITH a, downstream[..5] AS limited_downstream
        UNWIND limited_downstream AS b
        WITH a, b
        WHERE b IS NOT NULL
          AND {_trusted_pair_clause('a', 'b')}
        MERGE (a)-[r:FUND_FLOW {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d8a822',
            r.provisional = false,
            r.reason = 'beneficiary later acts as sender',
            r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
        """, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 4. DORMANT_TO_ACTIVE
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant'
          AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active'
          AND {_trusted_node_clause('t')}
        MERGE (t)-[r:DORMANT_TO_ACTIVE {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#ff8c8c',
            r.provisional = false,
            r.reason = 'dormant source account transacts with active beneficiary',
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ---------------------------- 
        # 5. HIGH_RISK_LINK: configured risky account directly appears in transaction
        # ----------------------------
        session.run(f"""
        UNWIND $accounts AS acc
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND (t.ACCOUNTNO = acc OR t.BENACCOUNTNO = acc)
          AND {_trusted_node_clause('t')}
        MERGE (t)-[r:HIGH_RISK_LINK {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#de7d07',
            r.provisional = false,
            r.reason = 'configured high-risk account appears in transaction',
            r.account = acc,
            r.risk_source = 'built_in_account_list',
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, accounts=high_risk_accounts, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        session.run(f"""
        UNWIND $risk_entries AS entry
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND {_trusted_node_clause('t')}
          AND {_trusted_entry_match('t')}
        WITH t, entry, toUpper(coalesce(entry.category, entry.CATEGORY, entry.type, entry.TYPE, 'RISK')) AS cat
        
        FOREACH (ignore IN CASE WHEN cat = 'PEP' THEN [1] ELSE [] END |
            MERGE (t)-[r:PEP_INVOLVED {{session_id:$session_id}}]->(t)
            SET r.bgcolor = '#0099ff', r.provisional = false, r.reason = 'PEP matched', r.risk_source = 'risk_entities', r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        )
        FOREACH (ignore IN CASE WHEN cat IN ['SANCTION', 'SANCTIONS', 'SANCTIONED'] THEN [1] ELSE [] END |
            MERGE (t)-[r:SANCTIONED_ENTITY_MATCH {{session_id:$session_id}}]->(t)
            SET r.bgcolor = '#ff3b3b', r.provisional = false, r.reason = 'Sanctioned entity matched', r.risk_source = 'risk_entities', r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        )
        FOREACH (ignore IN CASE WHEN NOT cat IN ['PEP', 'SANCTION', 'SANCTIONS', 'SANCTIONED'] THEN [1] ELSE [] END |
            MERGE (t)-[r:HIGH_RISK_LINK {{session_id:$session_id}}]->(t)
            SET r.bgcolor = '#de7d07', r.provisional = false, r.reason = 'Configured risk entity matched', r.risk_source = 'risk_entities', r.category = cat, r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        )
        """, session_id=session_param, trusted_entries=trusted_entries, risk_entries=risk_entries)

        # ----------------------------
        # 6. ABNORMAL_BALANCE_CHANGE: current balance move is an outlier for the account
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
        WITH t.ACCOUNTNO AS acc, t,
             coalesce(toFloat(t.BALANCEHELD), toFloat(t.BALANCE), toFloat(t.balance)) AS balance
        WHERE acc IS NOT NULL AND acc <> '' AND balance IS NOT NULL
        WITH t.ACCOUNTNO AS acc, t
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, collect(t) AS txns
        UNWIND range(1, size(txns)-1) AS i
        WITH txns[i] AS current,
             txns[i-1] AS previous,
             txns[CASE WHEN i-11 < 0 THEN 0 ELSE i-11 END .. i] AS history
        WITH current, previous,
             abs(
               coalesce(toFloat(current.BALANCEHELD), toFloat(current.BALANCE), toFloat(current.balance)) -
               coalesce(toFloat(previous.BALANCEHELD), toFloat(previous.BALANCE), toFloat(previous.balance))
             ) AS current_change,
             [j IN range(1, size(history)-1) |
               abs(
                 coalesce(toFloat(history[j].BALANCEHELD), toFloat(history[j].BALANCE), toFloat(history[j].balance)) -
                 coalesce(toFloat(history[j-1].BALANCEHELD), toFloat(history[j-1].BALANCE), toFloat(history[j-1].balance))
               )
             ] AS changes
        WITH current, previous, current_change, [c IN changes WHERE c IS NOT NULL AND c > 0] AS valid_changes
        WHERE size(valid_changes) >= 3
        WITH current, previous, current_change,
             reduce(s = 0.0, c IN valid_changes | s + c) / size(valid_changes) AS avg_change
        WHERE avg_change > 0 AND current_change >= avg_change * $threshold
          AND {_trusted_pair_clause('previous', 'current')}
        MERGE (previous)-[r:ABNORMAL_BALANCE_CHANGE {{session_id:$session_id}}]->(current)
        SET r.bgcolor = '#8fde86',
            r.provisional = false,
            r.reason = 'balance change exceeds recent account baseline',
            r.change = current_change,
            r.average_recent_change = avg_change,
            r.threshold_multiplier = $threshold,
            r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
        """, threshold=threshold_multiplier, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 7. HUB_AND_SPOKE: one account fans out to, or receives from, many counterparties
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND t.TRANSACTIONDATE IS NOT NULL
          AND t.TRANSACTIONDATE <> ''
          AND t.BENACCOUNTNO IS NOT NULL
          AND t.BENACCOUNTNO <> ''
        WITH t.ACCOUNTNO AS hub, t.TRANSACTIONDATE AS tx_day, collect(t) AS txns, count(DISTINCT t.BENACCOUNTNO) AS spoke_count
        WHERE hub IS NOT NULL
          AND hub <> ''
          AND NOT hub IN $pt
          AND spoke_count >= $min_tx_count
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
        WHERE {_trusted_pair_clause('a', 'b')}
        MERGE (a)-[r:HUB_AND_SPOKE {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d0b3ff',
            r.provisional = false,
            r.reason = 'account connects with multiple counterparties on same day',
            r.hub_account = hub,
            r.direction = 'outgoing',
            r.tx_day = tx_day,
            r.spoke_count = spoke_count,
            r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
        """, session_id=session_param, min_tx_count=min_tx_count, trusted_entries=trusted_entries, pt=pass_through_accounts)

        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND t.TRANSACTIONDATE IS NOT NULL
          AND t.TRANSACTIONDATE <> ''
          AND t.ACCOUNTNO IS NOT NULL
          AND t.ACCOUNTNO <> ''
        WITH t.BENACCOUNTNO AS hub, t.TRANSACTIONDATE AS tx_day, collect(t) AS txns, count(DISTINCT t.ACCOUNTNO) AS spoke_count
        WHERE hub IS NOT NULL
          AND hub <> ''
          AND NOT hub IN $pt
          AND spoke_count >= $min_tx_count
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
        WHERE {_trusted_pair_clause('a', 'b')}
        MERGE (a)-[r:HUB_AND_SPOKE {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d0b3ff',
            r.provisional = false,
            r.reason = 'account connects with multiple counterparties on same day',
            r.hub_account = hub,
            r.direction = 'incoming',
            r.tx_day = tx_day,
            r.spoke_count = spoke_count,
            r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
        """, session_id=session_param, min_tx_count=min_tx_count, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 8. SHARED_IDENTIFIER: same phone identifier appears on multiple accounts
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
        WITH t,
             [
               {{kind:'BUSINESSMOBILENO', value:t.BUSINESSMOBILENO, account:t.ACCOUNTNO}},
               {{kind:'BENTELNO', value:t.BENTELNO, account:t.BENACCOUNTNO}}
             ] AS identifiers
        UNWIND identifiers AS identifier
        WITH identifier.kind AS identifier_type,
             trim(toString(identifier.value)) AS identifier_value,
             identifier.account AS account,
             t
        WHERE identifier_value <> ''
          AND account IS NOT NULL
          AND account <> ''
        WITH identifier_type, identifier_value, collect(DISTINCT account) AS accounts, collect(DISTINCT t) AS txns
        WHERE size(accounts) >= 2
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, identifier_type, identifier_value, accounts
        WHERE {_trusted_pair_clause('a', 'b')}
        MERGE (a)-[r:SHARED_IDENTIFIER {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#8be0f0',
            r.provisional = false,
            r.reason = 'same identifier appears on multiple accounts',
            r.identifier_type = identifier_type,
            r.identifier_value = identifier_value,
            r.account_count = size(accounts),
            r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
        """, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 9. LATE_NIGHT_TX
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND t.TRANSACTIONTIME IS NOT NULL
          AND toString(t.TRANSACTIONTIME) <> ''
          AND {_trusted_node_clause('t')}
        WITH t, toInteger(substring(replace(toString(t.TRANSACTIONTIME), ':', ''), 0, 4)) AS t_time
        WHERE t_time >= 2300 OR t_time <= 400
        MERGE (t)-[r:LATE_NIGHT_TX {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#00c1a2',
            r.provisional = false,
            r.reason = 'transaction occurred outside typical business hours',
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 10. JUST_BELOW_THRESHOLD
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) > 0
          AND {_trusted_node_clause('t')}
        WITH t, coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amt
        WHERE amt >= ($single_tx_threshold * 0.9) AND amt < $single_tx_threshold
        MERGE (t)-[r:JUST_BELOW_THRESHOLD {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#dba124',
            r.provisional = false,
            r.reason = 'transaction amount is suspiciously close to reporting threshold',
            r.amount = amt,
            r.threshold = $single_tx_threshold,
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, session_id=session_param, trusted_entries=trusted_entries, single_tx_threshold=single_tx_threshold)

        # ----------------------------
        # 11. RAPID_WITHDRAWAL
        # ----------------------------
        session.run(f"""
        MATCH (a:{label}), (b:{label})
        WHERE ($session_id IS NULL OR ({_session_scope_clause("a")} AND {_session_scope_clause("b")}))
          AND a.BENACCOUNTNO = b.ACCOUNTNO
          AND a.BENACCOUNTNO IS NOT NULL
          AND a.BENACCOUNTNO <> ''
          AND coalesce(toString(a.TRANSACTIONDATE), '') = coalesce(toString(b.TRANSACTIONDATE), '')
          AND elementId(a) <> elementId(b)
          AND coalesce(toString(a.TRANSACTIONTIME), '') < coalesce(toString(b.TRANSACTIONTIME), '')
          AND {_trusted_pair_clause('a', 'b')}
        WITH a, b, 
             coalesce(toFloat(a.AMOUNTINBIRR), toFloat(a.AMOUNT), toFloat(a.amount), toFloat(a.LOCAL_AMOUNT), 0.0) AS in_amt,
             coalesce(toFloat(b.AMOUNTINBIRR), toFloat(b.AMOUNT), toFloat(b.amount), toFloat(b.LOCAL_AMOUNT), 0.0) AS out_amt
        WHERE in_amt > 0 AND out_amt >= (in_amt * 0.9) AND out_amt <= (in_amt * 1.1)
        MERGE (a)-[r:RAPID_WITHDRAWAL {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d5d276',
            r.provisional = false,
            r.reason = 'funds rapidly withdrawn or passed through on same day',
            r.in_amount = in_amt,
            r.out_amount = out_amt,
            r.edge_semantic = 'OBSERVED_FLOW', r.financial_flow = true, r.directed_display = true
        """, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 12. ACCOUNT_ACTIVITY_SPIKE
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND coalesce(toString(t.ACCOUNTNO), '') <> ''
          AND coalesce(toString(t.TRANSACTIONDATE), '') <> ''
        WITH t.ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day, count(t) AS daily_count, collect(t) AS day_txns
        WHERE daily_count >= 10
        MATCH (all_t:{label})
        WHERE all_t.ACCOUNTNO = acc AND coalesce(toString(all_t.TRANSACTIONDATE), '') <> ''
        WITH acc, tx_day, daily_count, day_txns, count(all_t) AS total_count, count(DISTINCT all_t.TRANSACTIONDATE) AS total_days
        WITH acc, tx_day, daily_count, day_txns, (toFloat(total_count) / toFloat(CASE WHEN total_days = 0 THEN 1 ELSE total_days END)) AS avg_daily
        WHERE daily_count >= (avg_daily * 3)
        UNWIND day_txns AS t
        WITH t, acc, tx_day, daily_count, avg_daily
        WHERE {_trusted_node_clause('t')}
        MERGE (t)-[r:ACCOUNT_ACTIVITY_SPIKE {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#e6e6e6',
            r.provisional = false,
            r.reason = 'unusually high transaction volume for this account on this day',
            r.daily_count = daily_count,
            r.avg_daily = avg_daily,
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        counts = _count_transaction_relationships(session, session_param) if session_param else {}
        _write_gds_metrics(session, f"{session_param}_transactions", nodes_label, session_param, TRANSACTION_RELATIONSHIPS, log_file)

    log_writer(log_file, f"[{datetime.now()}] [Success] Transactions analysis completed")
    return counts


def incremental_graph_analysis_transactions(
    driver,
    session_id,
    nodes_label,
    batch_id,
    log_file,
    high_risk_accounts=None,
    threshold_multiplier=3,
    single_tx_threshold=10000,
    total_threshold=30000,
    min_tx_count=3,
    trusted_entities=None,
    risk_entities=None,
):
    if high_risk_accounts is None:
        high_risk_accounts = []

    session_param = str(session_id)
    label = _safe_label(nodes_label)
    trusted_entries = trusted_entities_cypher_entries(trusted_entities)
    risk_entries = risk_entities_cypher_entries(risk_entities)
    pass_through_accounts = _extract_pass_through_accounts(trusted_entities)
    log_writer(log_file, f"[{datetime.now()}] [Info] Running incremental transaction analysis for batch {batch_id}")

    with driver.session() as session:
        _create_transaction_indexes(session, nodes_label)

        # ----------------------------
        # 0. EFFECTIVE_FLOW (incremental): trace funds through pass-through intermediaries for new batch
        # ----------------------------
        if pass_through_accounts:
            session.run(f"""
            MATCH (inbound:{label})
            WHERE inbound.batch_id = $batch_id
              AND inbound.BENACCOUNTNO IN $pass_through_accounts
              AND inbound.ACCOUNTNO IS NOT NULL AND inbound.ACCOUNTNO <> ''

            MATCH (outbound:{label})
            WHERE outbound.ACCOUNTNO = inbound.BENACCOUNTNO
              AND {_session_scope_clause("outbound")}
              AND outbound.BENACCOUNTNO IS NOT NULL
              AND outbound.BENACCOUNTNO <> ''
              AND outbound.BENACCOUNTNO <> inbound.ACCOUNTNO
              AND coalesce(outbound.TRANSACTIONDATE, '') = coalesce(inbound.TRANSACTIONDATE, '')
              AND coalesce(outbound.TRANSACTIONTIME, '') >= coalesce(inbound.TRANSACTIONTIME, '')

            WITH inbound, outbound,
                 coalesce(toFloat(inbound.AMOUNTINBIRR), toFloat(inbound.AMOUNT),
                          toFloat(inbound.amount), toFloat(inbound.LOCAL_AMOUNT), 0.0) AS in_amt,
                 coalesce(toFloat(outbound.AMOUNTINBIRR), toFloat(outbound.AMOUNT),
                          toFloat(outbound.amount), toFloat(outbound.LOCAL_AMOUNT), 0.0) AS out_amt
            WHERE in_amt > 0 AND out_amt > 0
              AND abs(out_amt - in_amt) <= (in_amt * 0.1)

            MERGE (inbound)-[r:EFFECTIVE_FLOW {{session_id:$session_id}}]->(outbound)
            SET r.intermediary = inbound.BENACCOUNTNO,
                r.hop_count = 2,
                r.in_amount = in_amt,
                r.out_amount = out_amt,
                r.fee_delta = in_amt - out_amt,
                r.effective_sender = inbound.ACCOUNTNO,
                r.effective_receiver = outbound.BENACCOUNTNO,
                r.tx_date = inbound.TRANSACTIONDATE,
                r.bgcolor = '#9b59b6',
                r.textcolor = '#eeeeee',
                r.provisional = true,
                r.edge_semantic = 'EFFECTIVE_FLOW',
                r.financial_flow = true,
                r.directed_display = true

        # ----------------------------
        # 0.5 LOGICAL TRANSACTION LAYER INITIALIZATION
        # ----------------------------
        session.run(f'''
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
        SET t.LOGICAL_ACCOUNTNO = coalesce(t.ACCOUNTNO, ''),
            t.LOGICAL_BENACCOUNTNO = coalesce(t.BENACCOUNTNO, ''),
            t.IGNORE_LOGICAL = false
        ''', batch_id=batch_id)
        
        if pass_through_accounts:
            session.run(f'''
            MATCH (inbound:{label})-[r:EFFECTIVE_FLOW]->(outbound:{label})
            WHERE inbound.batch_id = $batch_id
            SET inbound.LOGICAL_BENACCOUNTNO = coalesce(outbound.BENACCOUNTNO, ''),
                outbound.IGNORE_LOGICAL = true
            ''', batch_id=batch_id)
        log_writer(log_file, f"[{datetime.now()}] [Info] Logical Layer initialized")
            """, session_id=session_param, batch_id=batch_id, pass_through_accounts=pass_through_accounts)

        # Smurfing: start from new rows, then inspect only matching account/beneficiary/day groups.
        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        WITH DISTINCT seed.ACCOUNTNO AS acc, seed.BENACCOUNTNO AS beneficiary, seed.TRANSACTIONDATE AS tx_day
        WHERE acc IS NOT NULL AND acc <> ''
          AND beneficiary IS NOT NULL AND beneficiary <> ''
          AND tx_day IS NOT NULL AND tx_day <> ''
        MATCH (t:{label})
        WHERE {_session_scope_clause("t")}
          AND t.ACCOUNTNO = acc
          AND t.BENACCOUNTNO = beneficiary
          AND t.TRANSACTIONDATE = tx_day
        WITH acc, beneficiary, tx_day, t,
             coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amount
        WHERE amount IS NOT NULL
          AND amount > 0
          AND amount < $single_tx_threshold
        WITH acc, beneficiary, tx_day, t, amount
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, beneficiary, tx_day, collect(t) AS txns, sum(amount) AS total_amount, count(t) AS tx_count
        WHERE tx_count >= $min_tx_count
          AND total_amount >= $total_threshold
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, acc, beneficiary, tx_day, tx_count, total_amount
        WHERE {_trusted_pair_clause('a', 'b')}
        MERGE (a)-[r:SMURFING {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d5d276',
            r.provisional = true,
            r.reason = 'multiple small same-day transfers below threshold',
            r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true,
            r.account = acc,
            r.beneficiary = beneficiary,
            r.tx_day = tx_day,
            r.tx_count = tx_count,
            r.total_amount = total_amount,
            r.single_tx_threshold = $single_tx_threshold,
            r.total_threshold = $total_threshold
        """, batch_id=batch_id,
             session_id=session_param,
             trusted_entries=trusted_entries,
             single_tx_threshold=single_tx_threshold,
             total_threshold=total_threshold,
             min_tx_count=min_tx_count)

        # Circular flow: only pairs where the current batch is one side of the reversal.
        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        MATCH (other:{label})
        WHERE {_session_scope_clause("other")}
          AND seed.ACCOUNTNO = other.BENACCOUNTNO
          AND seed.BENACCOUNTNO = other.ACCOUNTNO
          AND seed.ACCOUNTNO IS NOT NULL
          AND seed.ACCOUNTNO <> ''
          AND seed.BENACCOUNTNO IS NOT NULL
          AND seed.BENACCOUNTNO <> ''
          AND elementId(seed) <> elementId(other)
          AND coalesce(seed.TRANSACTIONDATE, '') = coalesce(other.TRANSACTIONDATE, '')
          AND {_trusted_pair_clause('seed', 'other')}
        MERGE (seed)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(other)
        SET r1.bgcolor = '#e6e6e6', r1.provisional = true, r1.reason = 'same-day reverse transfer pair'
        MERGE (other)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(seed)
        SET r2.bgcolor = '#e6e6e6', r2.provisional = true, r2.reason = 'same-day reverse transfer pair'
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # Fund flow: new nodes can either precede or complete a downstream flow.
        session.run(f"""
        MATCH (a:{label}), (b:{label})
        WHERE (a.batch_id = $batch_id OR b.batch_id = $batch_id)
          AND {_session_scope_clause("a")}
          AND {_session_scope_clause("b")}
          AND a.BENACCOUNTNO = b.ACCOUNTNO
          AND a.BENACCOUNTNO IS NOT NULL
          AND a.BENACCOUNTNO <> ''
          AND elementId(a) <> elementId(b)
          AND (
            coalesce(a.TRANSACTIONDATE, '') < coalesce(b.TRANSACTIONDATE, '')
            OR (
              coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
              AND coalesce(a.TRANSACTIONTIME, '') < coalesce(b.TRANSACTIONTIME, '')
            )
          )
          AND {_trusted_pair_clause('a', 'b')}
        WITH a, b
        ORDER BY a.TRANSACTIONDATE, a.TRANSACTIONTIME, b.TRANSACTIONDATE, b.TRANSACTIONTIME
        WITH a, collect(b) AS downstream
        WITH a, downstream[..5] AS limited_downstream
        UNWIND limited_downstream AS b
        MERGE (a)-[r:FUND_FLOW {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d8a822',
            r.provisional = true,
            r.reason = 'beneficiary later acts as sender',
            r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # Cheap row-local flags: only new batch rows.
        session.run(f"""
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
          AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant'
          AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active'
          AND {_trusted_node_clause('t')}
        MERGE (t)-[r:DORMANT_TO_ACTIVE {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#ff8c8c',
            r.provisional = true,
            r.reason = 'dormant source account transacts with active beneficiary',
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        session.run(f"""
        UNWIND $accounts AS acc
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
          AND (t.ACCOUNTNO = acc OR t.BENACCOUNTNO = acc)
          AND {_trusted_node_clause('t')}
        MERGE (t)-[r:HIGH_RISK_LINK {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#de7d07',
            r.provisional = true,
            r.reason = 'configured high-risk account appears in transaction',
            r.account = acc,
            r.risk_source = 'built_in_account_list',
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, accounts=high_risk_accounts, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        session.run(f"""
        UNWIND $risk_entries AS entry
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
          AND {_trusted_node_clause('t')}
          AND {_trusted_entry_match('t')}
        WITH t, entry, toUpper(coalesce(entry.category, entry.CATEGORY, entry.type, entry.TYPE, 'RISK')) AS cat
        
        FOREACH (ignore IN CASE WHEN cat = 'PEP' THEN [1] ELSE [] END |
            MERGE (t)-[r:PEP_INVOLVED {{session_id:$session_id}}]->(t)
            SET r.bgcolor = '#0099ff', r.provisional = true, r.reason = 'PEP matched', r.risk_source = 'risk_entities', r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        )
        FOREACH (ignore IN CASE WHEN cat IN ['SANCTION', 'SANCTIONS', 'SANCTIONED'] THEN [1] ELSE [] END |
            MERGE (t)-[r:SANCTIONED_ENTITY_MATCH {{session_id:$session_id}}]->(t)
            SET r.bgcolor = '#ff3b3b', r.provisional = true, r.reason = 'Sanctioned entity matched', r.risk_source = 'risk_entities', r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        )
        FOREACH (ignore IN CASE WHEN NOT cat IN ['PEP', 'SANCTION', 'SANCTIONS', 'SANCTIONED'] THEN [1] ELSE [] END |
            MERGE (t)-[r:HIGH_RISK_LINK {{session_id:$session_id}}]->(t)
            SET r.bgcolor = '#de7d07', r.provisional = true, r.reason = 'Configured risk entity matched', r.risk_source = 'risk_entities', r.category = cat, r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        )
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, risk_entries=risk_entries)

        # Balance outlier: recalculate only accounts touched by this batch.
        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        WITH collect(DISTINCT seed.ACCOUNTNO) AS affected_accounts
        MATCH (t:{label})
        WHERE {_session_scope_clause("t")}
          AND t.ACCOUNTNO IN affected_accounts
        WITH t.ACCOUNTNO AS acc, t,
             coalesce(toFloat(t.BALANCEHELD), toFloat(t.BALANCE), toFloat(t.balance)) AS balance
        WHERE acc IS NOT NULL AND acc <> '' AND balance IS NOT NULL
        WITH acc, t
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, collect(t) AS txns
        UNWIND range(1, size(txns)-1) AS i
        WITH txns[i] AS current,
             txns[i-1] AS previous,
             txns[CASE WHEN i-11 < 0 THEN 0 ELSE i-11 END .. i] AS history
        WHERE current.batch_id = $batch_id OR previous.batch_id = $batch_id
        WITH current, previous,
             abs(
               coalesce(toFloat(current.BALANCEHELD), toFloat(current.BALANCE), toFloat(current.balance)) -
               coalesce(toFloat(previous.BALANCEHELD), toFloat(previous.BALANCE), toFloat(previous.balance))
             ) AS current_change,
             [j IN range(1, size(history)-1) |
               abs(
                 coalesce(toFloat(history[j].BALANCEHELD), toFloat(history[j].BALANCE), toFloat(history[j].balance)) -
                 coalesce(toFloat(history[j-1].BALANCEHELD), toFloat(history[j-1].BALANCE), toFloat(history[j-1].balance))
               )
             ] AS changes
        WITH current, previous, current_change, [c IN changes WHERE c IS NOT NULL AND c > 0] AS valid_changes
        WHERE size(valid_changes) >= 3
        WITH current, previous, current_change,
             reduce(s = 0.0, c IN valid_changes | s + c) / size(valid_changes) AS avg_change
        WHERE avg_change > 0 AND current_change >= avg_change * $threshold
          AND {_trusted_pair_clause('previous', 'current')}
        MERGE (previous)-[r:ABNORMAL_BALANCE_CHANGE {{session_id:$session_id}}]->(current)
        SET r.bgcolor = '#8fde86',
            r.provisional = true,
            r.reason = 'balance change exceeds recent account baseline',
            r.change = current_change,
            r.average_recent_change = avg_change,
            r.threshold_multiplier = $threshold,
            r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
        """, batch_id=batch_id, session_id=session_param, threshold=threshold_multiplier, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # Hub-and-spoke: recalculate account fans touched by this batch.
        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        WITH DISTINCT seed.ACCOUNTNO AS hub, seed.TRANSACTIONDATE AS tx_day
        WHERE hub IS NOT NULL AND hub <> ''
          AND tx_day IS NOT NULL AND tx_day <> ''
          AND NOT hub IN $pt
        MATCH (t:{label})
        WHERE {_session_scope_clause("t")}
          AND t.ACCOUNTNO = hub
          AND t.TRANSACTIONDATE = tx_day
          AND t.BENACCOUNTNO IS NOT NULL
          AND t.BENACCOUNTNO <> ''
        WITH hub, tx_day, collect(t) AS txns, count(DISTINCT t.BENACCOUNTNO) AS spoke_count
        WHERE spoke_count >= $min_tx_count
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
        MERGE (a)-[r:HUB_AND_SPOKE {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d0b3ff',
            r.provisional = true,
            r.reason = 'account connects with multiple counterparties on same day',
            r.hub_account = hub,
            r.direction = 'outgoing',
            r.tx_day = tx_day,
            r.spoke_count = spoke_count,
            r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
        """, batch_id=batch_id, session_id=session_param, min_tx_count=min_tx_count, trusted_entries=trusted_entries, pt=pass_through_accounts)

        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        WITH DISTINCT seed.BENACCOUNTNO AS hub, seed.TRANSACTIONDATE AS tx_day
        WHERE hub IS NOT NULL AND hub <> ''
          AND tx_day IS NOT NULL AND tx_day <> ''
          AND NOT hub IN $pt
        MATCH (t:{label})
        WHERE {_session_scope_clause("t")}
          AND t.BENACCOUNTNO = hub
          AND t.TRANSACTIONDATE = tx_day
          AND t.ACCOUNTNO IS NOT NULL
          AND t.ACCOUNTNO <> ''
        WITH hub, tx_day, collect(t) AS txns, count(DISTINCT t.ACCOUNTNO) AS spoke_count
        WHERE spoke_count >= $min_tx_count
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
        MERGE (a)-[r:HUB_AND_SPOKE {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d0b3ff',
            r.provisional = true,
            r.reason = 'account connects with multiple counterparties on same day',
            r.hub_account = hub,
            r.direction = 'incoming',
            r.tx_day = tx_day,
            r.spoke_count = spoke_count,
            r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
        """, batch_id=batch_id, session_id=session_param, min_tx_count=min_tx_count, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # Shared identifier: recalculate phone identifiers touched by this batch.
        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        WITH [
               {{kind:'BUSINESSMOBILENO', value:seed.BUSINESSMOBILENO}},
               {{kind:'BENTELNO', value:seed.BENTELNO}}
             ] AS identifiers
        UNWIND identifiers AS seed_identifier
        WITH DISTINCT seed_identifier.kind AS identifier_type, trim(toString(seed_identifier.value)) AS identifier_value
        WHERE identifier_value <> ''
        MATCH (t:{label})
        WHERE {_session_scope_clause("t")}
        WITH identifier_type, identifier_value, t,
             [
               {{kind:'BUSINESSMOBILENO', value:t.BUSINESSMOBILENO, account:t.ACCOUNTNO}},
               {{kind:'BENTELNO', value:t.BENTELNO, account:t.BENACCOUNTNO}}
             ] AS identifiers
        UNWIND identifiers AS identifier
        WITH identifier_type,
             identifier_value,
             identifier.account AS account,
             t,
             identifier.kind AS matched_type,
             trim(toString(identifier.value)) AS matched_value
        WHERE matched_type = identifier_type
          AND matched_value = identifier_value
          AND account IS NOT NULL
          AND account <> ''
        WITH identifier_type, identifier_value, collect(DISTINCT account) AS accounts, collect(DISTINCT t) AS txns
        WHERE size(accounts) >= 2
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, identifier_type, identifier_value, accounts
        WHERE {_trusted_pair_clause('a', 'b')}
        MERGE (a)-[r:SHARED_IDENTIFIER {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#8be0f0',
            r.provisional = true,
            r.reason = 'same identifier appears on multiple accounts',
            r.identifier_type = identifier_type,
            r.identifier_value = identifier_value,
            r.account_count = size(accounts),
            r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        session.run(f"""
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
          AND t.TRANSACTIONTIME IS NOT NULL
          AND toString(t.TRANSACTIONTIME) <> ''
          AND {_trusted_node_clause('t')}
        WITH t, toInteger(substring(replace(toString(t.TRANSACTIONTIME), ':', ''), 0, 4)) AS t_time
        WHERE t_time >= 2300 OR t_time <= 400
        MERGE (t)-[r:LATE_NIGHT_TX {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#00c1a2',
            r.provisional = true,
            r.reason = 'transaction occurred outside typical business hours',
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        session.run(f"""
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
          AND coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) > 0
          AND {_trusted_node_clause('t')}
        WITH t, coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amt
        WHERE amt >= ($single_tx_threshold * 0.9) AND amt < $single_tx_threshold
        MERGE (t)-[r:JUST_BELOW_THRESHOLD {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#dba124',
            r.provisional = true,
            r.reason = 'transaction amount is suspiciously close to reporting threshold',
            r.amount = amt,
            r.threshold = $single_tx_threshold,
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, single_tx_threshold=single_tx_threshold)

        session.run(f"""
        MATCH (a:{label}), (b:{label})
        WHERE (a.batch_id = $batch_id OR b.batch_id = $batch_id)
          AND ($session_id IS NULL OR ({_session_scope_clause("a")} AND {_session_scope_clause("b")}))
          AND a.BENACCOUNTNO = b.ACCOUNTNO
          AND a.BENACCOUNTNO IS NOT NULL
          AND a.BENACCOUNTNO <> ''
          AND coalesce(toString(a.TRANSACTIONDATE), '') = coalesce(toString(b.TRANSACTIONDATE), '')
          AND elementId(a) <> elementId(b)
          AND coalesce(toString(a.TRANSACTIONTIME), '') < coalesce(toString(b.TRANSACTIONTIME), '')
          AND {_trusted_pair_clause('a', 'b')}
        WITH a, b, 
             coalesce(toFloat(a.AMOUNTINBIRR), toFloat(a.AMOUNT), toFloat(a.amount), toFloat(a.LOCAL_AMOUNT), 0.0) AS in_amt,
             coalesce(toFloat(b.AMOUNTINBIRR), toFloat(b.AMOUNT), toFloat(b.amount), toFloat(b.LOCAL_AMOUNT), 0.0) AS out_amt
        WHERE in_amt > 0 AND out_amt >= (in_amt * 0.9) AND out_amt <= (in_amt * 1.1)
        MERGE (a)-[r:RAPID_WITHDRAWAL {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d5d276',
            r.provisional = true,
            r.reason = 'funds rapidly withdrawn or passed through on same day',
            r.in_amount = in_amt,
            r.out_amount = out_amt,
            r.edge_semantic = 'OBSERVED_FLOW', r.financial_flow = true, r.directed_display = true
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        session.run(f"""
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
          AND coalesce(toString(t.ACCOUNTNO), '') <> ''
          AND coalesce(toString(t.TRANSACTIONDATE), '') <> ''
        WITH t.ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day, count(t) AS daily_count, collect(t) AS day_txns
        WHERE daily_count >= 10
        MATCH (all_t:{label})
        WHERE all_t.ACCOUNTNO = acc AND coalesce(toString(all_t.TRANSACTIONDATE), '') <> ''
        WITH acc, tx_day, daily_count, day_txns, count(all_t) AS total_count, count(DISTINCT all_t.TRANSACTIONDATE) AS total_days
        WITH acc, tx_day, daily_count, day_txns, (toFloat(total_count) / toFloat(CASE WHEN total_days = 0 THEN 1 ELSE total_days END)) AS avg_daily
        WHERE daily_count >= (avg_daily * 3)
        UNWIND day_txns AS t
        WITH t, acc, tx_day, daily_count, avg_daily
        WHERE {_trusted_node_clause('t')}
        MERGE (t)-[r:ACCOUNT_ACTIVITY_SPIKE {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#e6e6e6',
            r.provisional = true,
            r.reason = 'unusually high transaction volume for this account on this day',
            r.daily_count = daily_count,
            r.avg_daily = avg_daily,
            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
        """, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        counts = _count_transaction_relationships(session, session_param)

    log_writer(log_file, f"[{datetime.now()}] [Info] Incremental analysis for batch {batch_id} flags: {counts}")
    return counts

# ====================================================
# Shared graph metrics
# ====================================================

def _cypher_string(value):
