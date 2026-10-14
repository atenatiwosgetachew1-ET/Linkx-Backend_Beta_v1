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
            scope_clause_t="t.batch_id = $batch_id",
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=False
        )
        session.run(query, session_id=session_param, batch_id=batch_id, trusted_entries=trusted_entries, pt=pass_through_accounts,
             smurfing_single_tx_threshold=single_tx_threshold,
             smurfing_cumulative_threshold=total_threshold,
             smurfing_min_tx_count=min_tx_count)"""

text = replace_block(r"        # 1\. SMURFING: repeated small transfers from one account to one beneficiary\n        # ----------------------------\n        session.run\(f\"\"\"\n        MATCH \(t:\{label\}\)\n        WHERE \(\$session_id IS NULL OR \{_session_scope_clause\(\"t\"\)\}\)\n.*?r\.single_tx_threshold = \$single_tx_threshold,\n            r\.total_threshold = \$total_threshold\n        \"\"\", session_id=session_param,\n             trusted_entries=trusted_entries,\n             single_tx_threshold=single_tx_threshold,\n             total_threshold=total_threshold,\n             min_tx_count=min_tx_count\)", r"", batch_smurf_rep, text)

incr_smurf_rep = """        # Smurfing: start from new rows, then inspect only matching account/beneficiary/day groups.
        query = get_smurfing_query(
            label=label,
            scope_clause_t=_session_scope_clause("t"),
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=True,
            incremental_batch_id="$batch_id"
        )
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts,
             smurfing_single_tx_threshold=single_tx_threshold,
             smurfing_cumulative_threshold=total_threshold,
             smurfing_min_tx_count=min_tx_count)"""

text = replace_block(r"        # Smurfing: start from new rows, then inspect only matching account/beneficiary/day groups.\n        session.run\(f\"\"\"\n        MATCH \(seed:\{label\}\)\n        WHERE seed.batch_id = \$batch_id\n.*?r\.single_tx_threshold = \$single_tx_threshold,\n            r\.total_threshold = \$total_threshold\n        \"\"\", batch_id=batch_id,\n             session_id=session_param,\n             trusted_entries=trusted_entries,\n             single_tx_threshold=single_tx_threshold,\n             total_threshold=total_threshold,\n             min_tx_count=min_tx_count\)", r"", incr_smurf_rep, text)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(text)

