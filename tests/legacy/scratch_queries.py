        # ---- 2. CIRCULAR_FLOW (OPTIMIZED: index-assisted, no cartesian product) ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
                WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
                WHERE out_count < 1000

                MATCH (a:{label} {{ACCOUNTNO: acc}})
                WHERE ($session_id IS NULL OR a.session_id = $session_id)
                  AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
                  AND NOT a.LOGICAL_BENACCOUNTNO IN $pass_through_accounts
                CALL (a) {{
                  MATCH (b:{label} {{ACCOUNTNO: a.LOGICAL_BENACCOUNTNO, BENACCOUNTNO: a.LOGICAL_ACCOUNTNO}})
                  WHERE ($session_id IS NULL OR b.session_id = $session_id)
                    AND elementId(a) < elementId(b)
                    AND coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
                    AND {_trusted_pair_clause('a', 'b')}
                  MERGE (a)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(b)
                  SET r1.bgcolor = '#e6e6e6', r1.provisional = false, r1.reason = 'same-day reverse transfer pair',
                      r1.edge_semantic = 'OBSERVED_FLOW', r1.financial_flow = true, r1.directed_display = true
                  MERGE (b)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(a)
                  SET r2.bgcolor = '#e6e6e6', r2.provisional = false, r2.reason = 'same-day reverse transfer pair',
                      r2.edge_semantic = 'OBSERVED_FLOW', r2.financial_flow = true, r2.directed_display = true
                }} IN TRANSACTIONS OF 1000 ROWS
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pass_through_accounts=pass_through_accounts, pt=pass_through_accounts)
            rules_completed.append("CIRCULAR_FLOW")
            print(f"  [Rule] CIRCULAR_FLOW ✓", flush=True)
        except Exception as e:
            rules_failed.append(("CIRCULAR_FLOW", str(e)[:100]))
            print(f"  [Rule] CIRCULAR_FLOW ✗ {str(e)[:100]}", flush=True)

        # ---- 3. FUND_FLOW (OPTIMIZED: index-assisted, no cartesian product) ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
                WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
                WHERE out_count < 1000 AND NOT acc IN $pt

                MATCH (a:{label} {{LOGICAL_BENACCOUNTNO: acc}})
                WHERE ($session_id IS NULL OR a.session_id = $session_id)
                CALL (a, acc) {{
                  MATCH (b:{label} {{LOGICAL_ACCOUNTNO: acc}})
                  WHERE ($session_id IS NULL OR b.session_id = $session_id)
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
                  ORDER BY b.TRANSACTIONDATE ASC, b.TRANSACTIONTIME ASC
                  WITH a, collect(b) AS downstream
                  WITH a, downstream[..5] AS limited_downstream
                  UNWIND limited_downstream AS b
                  MERGE (a)-[r:FUND_FLOW {{session_id:$session_id}}]->(b)
                  SET r.bgcolor = '#d8a822', r.provisional = false,
                      r.reason = 'beneficiary later acts as sender',
                      r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
                }} IN TRANSACTIONS OF 1000 ROWS
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pass_through_accounts=pass_through_accounts, pt=pass_through_accounts)
            rules_completed.append("FUND_FLOW")
            print(f"  [Rule] FUND_FLOW ✓", flush=True)
        except Exception as e:
            rules_failed.append(("FUND_FLOW", str(e)[:100]))
            print(f"  [Rule] FUND_FLOW ✗ {str(e)[:100]}", flush=True)

        # ---- 4. DORMANT_TO_ACTIVE ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND {_trusted_node_clause('t')}
                  AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant'
                  AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active'
                MERGE (t)-[r:DORMANT_TO_ACTIVE {{session_id:$session_id}}]->(t)
                SET r.bgcolor = '#c20f0f', r.textcolor = '#eeeeee', r.provisional = false,
                    r.reason = 'dormant source account transacts with active beneficiary',
                    r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pass_through_accounts=pass_through_accounts, pt=pass_through_accounts)
            rules_completed.append("DORMANT_TO_ACTIVE")
            print(f"  [Rule] DORMANT_TO_ACTIVE ✓", flush=True)
        except Exception as e:
            rules_failed.append(("DORMANT_TO_ACTIVE", str(e)[:100]))
            print(f"  [Rule] DORMANT_TO_ACTIVE ✗ {str(e)[:100]}", flush=True)

        # ---- 5. ABNORMAL_BALANCE_CHANGE ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                WITH t.LOGICAL_ACCOUNTNO AS acc, t,
                     coalesce(toFloat(t.BALANCEHELD), toFloat(t.BALANCE), toFloat(t.balance)) AS balance
                WHERE acc IS NOT NULL AND acc <> '' AND balance IS NOT NULL
                WITH t.LOGICAL_ACCOUNTNO AS acc, t
                ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
                With acc, collect(t) AS txns
                UNWIND range(1, size(txns)-1) AS i
                WITH txns[i] AS current, txns[i-1] AS previous,
                     txns[CASE WHEN i-11 < 0 THEN 0 ELSE i-11 END .. i] AS history
                WITH current, previous,
                     abs(coalesce(toFloat(current.BALANCEHELD), toFloat(current.BALANCE), toFloat(current.balance)) -
                         coalesce(toFloat(previous.BALANCEHELD), toFloat(previous.BALANCE), toFloat(previous.balance))) AS current_change,
                     [j IN range(1, size(history)-1) |
                       abs(coalesce(toFloat(history[j].BALANCEHELD), toFloat(history[j].BALANCE), toFloat(history[j].balance)) -
                           coalesce(toFloat(history[j-1].BALANCEHELD), toFloat(history[j-1].BALANCE), toFloat(history[j-1].balance)))] AS changes
                WITH current, previous, current_change, [c IN changes WHERE c IS NOT NULL AND c > 0] AS valid_changes
                WHERE size(valid_changes) >= 3
                WITH current, previous, current_change,
                     reduce(s = 0.0, c IN valid_changes | s + c) / size(valid_changes) AS avg_change
                WHERE avg_change > 0 AND current_change >= avg_change * 3
                  AND {_trusted_pair_clause('previous', 'current')}
                MERGE (previous)-[r:ABNORMAL_BALANCE_CHANGE {{session_id:$session_id}}]->(current)
                SET r.bgcolor = '#196e08', r.textcolor = '#eeeeee', r.provisional = false,
                    r.reason = 'balance change exceeds recent account baseline',
                    r.change = current_change, r.average_recent_change = avg_change, r.threshold_multiplier = 3,
                    r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pass_through_accounts=pass_through_accounts, pt=pass_through_accounts)
            rules_completed.append("ABNORMAL_BALANCE_CHANGE")
            print(f"  [Rule] ABNORMAL_BALANCE_CHANGE ✓", flush=True)
        except Exception as e:
            rules_failed.append(("ABNORMAL_BALANCE_CHANGE", str(e)[:100]))
            print(f"  [Rule] ABNORMAL_BALANCE_CHANGE ✗ {str(e)[:100]}", flush=True)

        # ---- 6. HUB_AND_SPOKE (outgoing) ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> ''
                  AND t.LOGICAL_BENACCOUNTNO IS NOT NULL AND t.LOGICAL_BENACCOUNTNO <> ''
                WITH t.LOGICAL_ACCOUNTNO AS hub, t.TRANSACTIONDATE AS tx_day, collect(t) AS txns, count(DISTINCT t.LOGICAL_BENACCOUNTNO) AS spoke_count
                WHERE hub IS NOT NULL AND hub <> '' AND NOT hub IN $pt AND spoke_count >= $hub_spoke_min_counterparties AND size(txns) < 1000
                CALL (txns, hub, tx_day, spoke_count) {{
                  UNWIND range(0, size(txns)-2) AS i
                  WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
                  WHERE {_trusted_pair_clause('a', 'b')}
                  MERGE (a)-[r:HUB_AND_SPOKE {{session_id:$session_id}}]->(b)
                  SET r.bgcolor = '#6f42c1', r.textcolor = '#eeeeee', r.provisional = false,
                      r.reason = 'account connects with multiple counterparties on same day',
                      r.hub_account = hub, r.direction = 'outgoing', r.tx_day = tx_day, r.spoke_count = spoke_count,
                      r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
                }} IN TRANSACTIONS OF 1000 ROWS
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                     hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties"), pt=pass_through_accounts)
            rules_completed.append("HUB_AND_SPOKE_OUT")
            print(f"  [Rule] HUB_AND_SPOKE (outgoing) ✓", flush=True)
        except Exception as e:
            rules_failed.append(("HUB_AND_SPOKE_OUT", str(e)[:100]))
            print(f"  [Rule] HUB_AND_SPOKE (outgoing) ✗ {str(e)[:100]}", flush=True)

        # ---- 7. HUB_AND_SPOKE (incoming) ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> ''
                  AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
                WITH t.LOGICAL_BENACCOUNTNO AS hub, t.TRANSACTIONDATE AS tx_day, collect(t) AS txns, count(DISTINCT t.LOGICAL_ACCOUNTNO) AS spoke_count
                WHERE hub IS NOT NULL AND hub <> '' AND NOT hub IN $pt AND spoke_count >= $hub_spoke_min_counterparties AND size(txns) < 1000
                CALL (txns, hub, tx_day, spoke_count) {{
                  UNWIND range(0, size(txns)-2) AS i
                  WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
                  WHERE {_trusted_pair_clause('a', 'b')}
                  MERGE (a)-[r:HUB_AND_SPOKE {{session_id:$session_id}}]->(b)
                  SET r.bgcolor = '#6f42c1', r.textcolor = '#eeeeee', r.provisional = false,
                      r.reason = 'account connects with multiple counterparties on same day',
                      r.hub_account = hub, r.direction = 'incoming', r.tx_day = tx_day, r.spoke_count = spoke_count,
                      r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
                }} IN TRANSACTIONS OF 1000 ROWS
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                     hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties"), pt=pass_through_accounts)
            rules_completed.append("HUB_AND_SPOKE_IN")
            print(f"  [Rule] HUB_AND_SPOKE (incoming) ✓", flush=True)
        except Exception as e:
            rules_failed.append(("HUB_AND_SPOKE_IN", str(e)[:100]))
            print(f"  [Rule] HUB_AND_SPOKE (incoming) ✗ {str(e)[:100]}", flush=True)

        # ---- 8. SHARED_IDENTIFIER ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
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
                  WHERE {_trusted_pair_clause('a', 'b')}
                  MERGE (a)-[r:SHARED_IDENTIFIER {{session_id:$session_id}}]->(b)
                  SET r.bgcolor = '#0b7285', r.textcolor = '#eeeeee', r.provisional = false,
                      r.reason = 'same identifier appears on multiple accounts',
                      r.identifier_type = identifier_type, r.identifier_value = identifier_value,
                      r.account_count = size(accounts),
                      r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
                }} IN TRANSACTIONS OF 1000 ROWS
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pass_through_accounts=pass_through_accounts, pt=pass_through_accounts)
            rules_completed.append("SHARED_IDENTIFIER")
            print(f"  [Rule] SHARED_IDENTIFIER ✓", flush=True)
        except Exception as e:
            rules_failed.append(("SHARED_IDENTIFIER", str(e)[:100]))
            print(f"  [Rule] SHARED_IDENTIFIER ✗ {str(e)[:100]}", flush=True)

        # ---- 9. LATE_NIGHT_TX ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND t.TRANSACTIONTIME IS NOT NULL
                  AND toString(t.TRANSACTIONTIME) <> ''
                  AND {_trusted_node_clause('t')}
                WITH t, toInteger(substring(replace(toString(t.TRANSACTIONTIME), ':', ''), 0, 4)) AS t_time
                WHERE t_time >= $late_night_start OR t_time <= $late_night_end
                MERGE (t)-[r:LATE_NIGHT_TX {{session_id:$session_id}}]->(t)
                SET r.bgcolor = '#00c1a2', r.provisional = false,
                    r.reason = 'transaction occurred outside typical business hours',
                    r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                     late_night_start=thresholds.get("late_night_start"),
                     late_night_end=thresholds.get("late_night_end"))
            rules_completed.append("LATE_NIGHT_TX")
            print(f"  [Rule] LATE_NIGHT_TX ✓", flush=True)
        except Exception as e:
            rules_failed.append(("LATE_NIGHT_TX", str(e)[:100]))
            print(f"  [Rule] LATE_NIGHT_TX ✗ {str(e)[:100]}", flush=True)

        # ---- 10. JUST_BELOW_THRESHOLD ----
        try:
            reporting_threshold = thresholds.get("reporting_threshold")
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) > 0
                  AND {_trusted_node_clause('t')}
                WITH t, coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amt
                WHERE amt >= ($reporting_threshold * 0.9) AND amt < $reporting_threshold
                MERGE (t)-[r:JUST_BELOW_THRESHOLD {{session_id:$session_id}}]->(t)
                SET r.bgcolor = '#dba124', r.provisional = false,
                    r.reason = 'transaction amount is suspiciously close to reporting threshold',
                    r.amount = amt, r.threshold = $reporting_threshold,
                    r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                     reporting_threshold=thresholds.get("reporting_threshold"))
            rules_completed.append("JUST_BELOW_THRESHOLD")
            print(f"  [Rule] JUST_BELOW_THRESHOLD ✓", flush=True)
        except Exception as e:
            rules_failed.append(("JUST_BELOW_THRESHOLD", str(e)[:100]))
            print(f"  [Rule] JUST_BELOW_THRESHOLD ✗ {str(e)[:100]}", flush=True)

        # ---- 11. RAPID_WITHDRAWAL ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND t.LOGICAL_BENACCOUNTNO IS NOT NULL AND t.LOGICAL_BENACCOUNTNO <> ''
                WITH t.LOGICAL_BENACCOUNTNO AS acc, count(t) AS out_count
                WHERE out_count < 1000 AND NOT acc IN $pt

                MATCH (a:{label} {{LOGICAL_BENACCOUNTNO: acc}})
                WHERE ($session_id IS NULL OR a.session_id = $session_id)
                CALL (a, acc) {{
                  MATCH (b:{label} {{LOGICAL_ACCOUNTNO: acc}})
                  WHERE ($session_id IS NULL OR b.session_id = $session_id)
                    AND elementId(a) <> elementId(b)
                    AND coalesce(toString(a.TRANSACTIONDATE), '') = coalesce(toString(b.TRANSACTIONDATE), '')
                    AND coalesce(toString(a.TRANSACTIONTIME), '') < coalesce(toString(b.TRANSACTIONTIME), '')
                    AND {_trusted_pair_clause('a', 'b')}
                  WITH a, b,
                       coalesce(toFloat(a.AMOUNTINBIRR), toFloat(a.AMOUNT), toFloat(a.amount), toFloat(a.LOCAL_AMOUNT), 0.0) AS in_amt,
                       coalesce(toFloat(b.AMOUNTINBIRR), toFloat(b.AMOUNT), toFloat(b.amount), toFloat(b.LOCAL_AMOUNT), 0.0) AS out_amt
                  WHERE in_amt > 0 AND out_amt >= (in_amt * (1 - $rapid_withdrawal_amount_tolerance)) AND out_amt <= (in_amt * (1 + $rapid_withdrawal_amount_tolerance))
                  MERGE (a)-[r:RAPID_WITHDRAWAL {{session_id:$session_id}}]->(b)
                  SET r.bgcolor = '#d5d276', r.provisional = false,
                      r.reason = 'funds rapidly withdrawn or passed through on same day',
                      r.in_amount = in_amt, r.out_amount = out_amt,
                      r.edge_semantic = 'OBSERVED_FLOW', r.financial_flow = true, r.directed_display = true
                }} IN TRANSACTIONS OF 1000 ROWS
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                     rapid_withdrawal_amount_tolerance=thresholds.get("rapid_withdrawal_amount_tolerance"), pt=pass_through_accounts)
            rules_completed.append("RAPID_WITHDRAWAL")
            print(f"  [Rule] RAPID_WITHDRAWAL ✓", flush=True)
        except Exception as e:
            rules_failed.append(("RAPID_WITHDRAWAL", str(e)[:100]))
            print(f"  [Rule] RAPID_WITHDRAWAL ✗ {str(e)[:100]}", flush=True)

        # ---- 12. ACCOUNT_ACTIVITY_SPIKE ----
        try:
            with driver.session() as s:
                s.run(f"""
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND coalesce(toString(t.LOGICAL_ACCOUNTNO), '') <> ''
                  AND coalesce(toString(t.TRANSACTIONDATE), '') <> ''
                WITH t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day, count(t) AS daily_count, collect(t) AS day_txns
                WHERE daily_count >= $activity_spike_min_daily_count AND NOT acc IN $pt AND NOT acc IN $pt AND NOT acc IN $pt
                MATCH (all_t:{label})
                WHERE all_t.LOGICAL_ACCOUNTNO = acc AND coalesce(toString(all_t.TRANSACTIONDATE), '') <> '' AND coalesce(all_t.IGNORE_LOGICAL, false) = false
                WITH acc, tx_day, daily_count, day_txns, count(all_t) AS total_count, count(DISTINCT all_t.TRANSACTIONDATE) AS total_days
                WITH acc, tx_day, daily_count, day_txns, (toFloat(total_count) / toFloat(CASE WHEN total_days = 0 THEN 1 ELSE total_days END)) AS avg_daily
                WHERE daily_count >= (avg_daily * $activity_spike_multiplier)
                UNWIND day_txns AS t
                WITH t, acc, tx_day, daily_count, avg_daily
                WHERE {_trusted_node_clause('t')}
                MERGE (t)-[r:ACCOUNT_ACTIVITY_SPIKE {{session_id:$session_id}}]->(t)
                SET r.bgcolor = '#e6e6e6', r.provisional = false,
                    r.reason = 'unusually high transaction volume for this account on this day',
                    r.daily_count = daily_count, r.avg_daily = avg_daily,
                    r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                     activity_spike_min_daily_count=thresholds.get("activity_spike_min_daily_count"),
                     activity_spike_multiplier=thresholds.get("activity_spike_multiplier"), pt=pass_through_accounts)
            rules_completed.append("ACCOUNT_ACTIVITY_SPIKE")
            print(f"  [Rule] ACCOUNT_ACTIVITY_SPIKE ✓", flush=True)
        except Exception as e:
            rules_failed.append(("ACCOUNT_ACTIVITY_SPIKE", str(e)[:100]))
            print(f"  [Rule] ACCOUNT_ACTIVITY_SPIKE ✗ {str(e)[:100]}", flush=True)

        # ---- 13. HIGH_RISK_LINK (from risk_entities) ----
        try:
            with driver.session() as s:
                s.run(f"""
                UNWIND $risk_entries AS entry
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                  AND coalesce(t.IGNORE_LOGICAL, false) = false
                  AND {_trusted_node_clause('t')}
                  AND {_trusted_entry_match('t')}
                WITH t, entry, toUpper(coalesce(entry.category, entry.CATEGORY, entry.type, entry.TYPE, 'RISK')) AS cat

                FOREACH (ignore IN CASE WHEN cat = 'PEP' THEN [1] ELSE [] END |
                    MERGE (t)-[r:PEP_INVOLVED {{session_id:$session_id}}]->(t)
                    SET r.bgcolor = '#0099ff', r.provisional = false, r.reason = 'PEP matched', r.risk_source = 'risk_entities',
                        r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
                )
                FOREACH (ignore IN CASE WHEN cat IN ['SANCTION', 'SANCTIONS', 'SANCTIONED'] THEN [1] ELSE [] END |
                    MERGE (t)-[r:SANCTIONED_ENTITY_MATCH {{session_id:$session_id}}]->(t)
                    SET r.bgcolor = '#ff3b3b', r.provisional = false, r.reason = 'Sanctioned entity matched', r.risk_source = 'risk_entities',
                        r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
                )
                FOREACH (ignore IN CASE WHEN NOT cat IN ['PEP', 'SANCTION', 'SANCTIONS', 'SANCTIONED'] THEN [1] ELSE [] END |
                    MERGE (t)-[r:HIGH_RISK_LINK {{session_id:$session_id}}]->(t)
                    SET r.bgcolor = '#de7d07', r.provisional = false, r.reason = 'Configured risk entity matched', r.risk_source = 'risk_entities', r.category = cat,
                        r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
                )
                """, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pass_through_accounts=pass_through_accounts, pt=pass_through_accounts)
            rules_completed.append("HIGH_RISK_LINK")
