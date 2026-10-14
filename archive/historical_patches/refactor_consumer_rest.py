import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py', 'r') as f:
    text = f.read()

def replace_block(pattern_start, pattern_end, replacement, text):
    pattern = pattern_start + r".*?" + pattern_end
    return re.sub(pattern, replacement, text, flags=re.DOTALL)

# 3. FUND_FLOW
ff_rep = """        try:
            with driver.session() as s:
                query = get_fund_flow_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    scope_clause_a="$session_id IS NULL OR a.session_id = $session_id",
                    scope_clause_b="$session_id IS NULL OR b.session_id = $session_id",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("FUND_FLOW")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true\n                \}\} IN TRANSACTIONS OF 1000 ROWS\n                \"\"\", .*?\)\n            rules_completed.append\(\"FUND_FLOW\"\)", r"", ff_rep, text)

# 4. DORMANT_TO_ACTIVE
da_rep = """        try:
            with driver.session() as s:
                query = get_dormant_to_active_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("DORMANT_TO_ACTIVE")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false\n                \"\"\", .*?\)\n            rules_completed.append\(\"DORMANT_TO_ACTIVE\"\)", r"", da_rep, text)

# 5. ABNORMAL_BALANCE_CHANGE
ab_rep = """        try:
            with driver.session() as s:
                query = get_abnormal_balance_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("ABNORMAL_BALANCE_CHANGE")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true\n                \"\"\", .*?\)\n            rules_completed.append\(\"ABNORMAL_BALANCE_CHANGE\"\)", r"", ab_rep, text)

# 6. HUB_AND_SPOKE (outgoing)
hso_rep = """        try:
            with driver.session() as s:
                query = get_hub_and_spoke_out_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts, hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties"))
            rules_completed.append("HUB_AND_SPOKE_OUT")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false\n                \}\} IN TRANSACTIONS OF 1000 ROWS\n                \"\"\", .*?\)\n            rules_completed.append\(\"HUB_AND_SPOKE_OUT\"\)", r"", hso_rep, text)

# 7. HUB_AND_SPOKE (incoming)
hsi_rep = """        try:
            with driver.session() as s:
                query = get_hub_and_spoke_in_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts, hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties"))
            rules_completed.append("HUB_AND_SPOKE_IN")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false\n                \}\} IN TRANSACTIONS OF 1000 ROWS\n                \"\"\", .*?\)\n            rules_completed.append\(\"HUB_AND_SPOKE_IN\"\)", r"", hsi_rep, text)

# 8. SHARED_IDENTIFIER
si_rep = """        try:
            with driver.session() as s:
                query = get_shared_identifier_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("SHARED_IDENTIFIER")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false\n                \}\} IN TRANSACTIONS OF 1000 ROWS\n                \"\"\", .*?\)\n            rules_completed.append\(\"SHARED_IDENTIFIER\"\)", r"", si_rep, text)

# 9. RAPID_WITHDRAWAL
rw_rep = """        try:
            with driver.session() as s:
                query = get_rapid_withdrawal_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts, rapid_withdrawal_amount_tolerance=thresholds.get("rapid_withdrawal_amount_tolerance"))
            rules_completed.append("RAPID_WITHDRAWAL")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'OBSERVED_FLOW', r.financial_flow = true, r.directed_display = true\n                \}\} IN TRANSACTIONS OF 1000 ROWS\n                \"\"\", .*?\)\n            rules_completed.append\(\"RAPID_WITHDRAWAL\"\)", r"", rw_rep, text)

# 10. ACCOUNT_ACTIVITY_SPIKE
aas_rep = """        try:
            with driver.session() as s:
                query = get_account_activity_spike_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts, activity_spike_min_daily_count=thresholds.get("activity_spike_min_daily_count"), activity_spike_multiplier=thresholds.get("activity_spike_multiplier"))
            rules_completed.append("ACCOUNT_ACTIVITY_SPIKE")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false\n                \}\} IN TRANSACTIONS OF 1000 ROWS\n                \"\"\", .*?\)\n            rules_completed.append\(\"ACCOUNT_ACTIVITY_SPIKE\"\)", r"", aas_rep, text)

# 11. HIGH_RISK_LINK
hrl_rep = """        try:
            with driver.session() as s:
                query = get_high_risk_link_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("HIGH_RISK_LINK")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false\n                \)\n                \}\} IN TRANSACTIONS OF 1000 ROWS\n                \"\"\", .*?\)\n            rules_completed.append\(\"HIGH_RISK_LINK\"\)", r"", hrl_rep, text)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py', 'w') as f:
    f.write(text)

