import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py', 'r') as f:
    text = f.read()

# Add import at the top
import_str = "from batch_manager.analyzing.LA_rules_script import (\n    get_smurfing_query,\n    get_circular_flow_query,\n    get_fund_flow_query,\n    get_dormant_to_active_query,\n    get_abnormal_balance_query,\n    get_hub_and_spoke_out_query,\n    get_hub_and_spoke_in_query,\n    get_shared_identifier_query,\n    get_rapid_withdrawal_query,\n    get_account_activity_spike_query,\n    get_high_risk_link_query\n)\n"
text = text.replace("import logging", "import logging\n" + import_str, 1)

# Function to replace a block
def replace_block(pattern_start, pattern_end, replacement, text):
    pattern = pattern_start + r".*?" + pattern_end
    return re.sub(pattern, replacement, text, flags=re.DOTALL)

# 1. SMURFING
smurf_rep = """        try:
            with driver.session() as s:
                query = get_smurfing_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                     smurfing_single_tx_threshold=thresholds.get("smurfing_single_tx_threshold"),
                     smurfing_min_tx_count=thresholds.get("smurfing_min_tx_count"),
                     smurfing_cumulative_threshold=thresholds.get("smurfing_cumulative_threshold"))
            rules_completed.append("SMURFING")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?amount < \$smurfing_single_tx_threshold.*?financial_flow = false, r.directed_display = true\n                \"\"\", .*?\)\n            rules_completed.append\(\"SMURFING\"\)", r"", smurf_rep, text)

# 2. CIRCULAR_FLOW
circ_rep = """        try:
            with driver.session() as s:
                query = get_circular_flow_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    scope_clause_a="$session_id IS NULL OR a.session_id = $session_id",
                    scope_clause_b="$session_id IS NULL OR b.session_id = $session_id",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("CIRCULAR_FLOW")"""
text = replace_block(r"        try:\n            with driver.session\(\) as s:\n                s.run\(f\"\"\"\n                MATCH \(t:\{label\}\)\n                WHERE \(\$session_id IS NULL OR t.session_id = \$session_id\)\n.*?r2.edge_semantic = 'OBSERVED_FLOW', r2.financial_flow = true, r2.directed_display = true\n                \}\} IN TRANSACTIONS OF 1000 ROWS\n                \"\"\", .*?\)\n            rules_completed.append\(\"CIRCULAR_FLOW\"\)", r"", circ_rep, text)

# Write test to check if substitution worked
with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py', 'w') as f:
    f.write(text)

print("Consumer rewritten!")
