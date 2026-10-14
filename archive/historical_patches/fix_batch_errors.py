import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

def replace_block(pattern_start, pattern_end, replacement, text):
    pattern = pattern_start + r".*?" + pattern_end
    return re.sub(pattern, replacement, text, flags=re.DOTALL)

batch_smurf_rep = """        # 1. SMURFING: repeated small transfers from one account to one beneficiary
        # ----------------------------
        query = get_smurfing_query(
            label=label,
            scope_clause_t="$session_id IS NULL OR t.session_id = $session_id OR t.batch_id STARTS WITH $session_id",
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=False
        )
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts,
             smurfing_single_tx_threshold=single_tx_threshold,
             smurfing_cumulative_threshold=total_threshold,
             smurfing_min_tx_count=min_tx_count)"""

text = replace_block(r"        # 1\. SMURFING: repeated small transfers from one account to one beneficiary\n        # ----------------------------\n        query = get_smurfing_query\(", r"smurfing_min_tx_count=min_tx_count\)", batch_smurf_rep, text)

batch_circ_rep = """        # 2. CIRCULAR_FLOW: direct account-to-beneficiary reversal
        # ----------------------------
        query = get_circular_flow_query(
            label=label,
            scope_clause_t="$session_id IS NULL OR t.session_id = $session_id OR t.batch_id STARTS WITH $session_id",
            scope_clause_a="$session_id IS NULL OR a.session_id = $session_id OR a.batch_id STARTS WITH $session_id",
            scope_clause_b="$session_id IS NULL OR b.session_id = $session_id OR b.batch_id STARTS WITH $session_id",
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=False
        )
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)"""
text = replace_block(r"        # 2\. CIRCULAR_FLOW: direct account-to-beneficiary reversal\n        # ----------------------------\n        query = get_circular_flow_query\(", r"trusted_entries=trusted_entries, pt=pass_through_accounts\)", batch_circ_rep, text)

batch_fund_rep = """        # 3. FUND_FLOW: beneficiary becomes sender in a later transaction
        # ----------------------------
        query = get_fund_flow_query(
            label=label,
            scope_clause_t="$session_id IS NULL OR t.session_id = $session_id OR t.batch_id STARTS WITH $session_id",
            scope_clause_a="$session_id IS NULL OR a.session_id = $session_id OR a.batch_id STARTS WITH $session_id",
            scope_clause_b="$session_id IS NULL OR b.session_id = $session_id OR b.batch_id STARTS WITH $session_id",
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=False
        )
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)"""
text = replace_block(r"        # 3\. FUND_FLOW: beneficiary becomes sender in a later transaction\n        # ----------------------------\n        query = get_fund_flow_query\(", r"trusted_entries=trusted_entries, pt=pass_through_accounts\)", batch_fund_rep, text)

batch_dorm_rep = """        query = get_dormant_to_active_query(
            label=label, 
            scope_clause_t="$session_id IS NULL OR t.session_id = $session_id OR t.batch_id STARTS WITH $session_id", 
            is_provisional=False
        )
        session.run(query, session_id=session_param)"""
text = replace_block(r"        query = get_dormant_to_active_query\(label=label, scope_clause_t=\"t\.batch_id = \$batch_id\", is_provisional=False\)\n        session\.run\(query, session_id=session_param, batch_id=batch_id\)", r"", batch_dorm_rep, text)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(text)

