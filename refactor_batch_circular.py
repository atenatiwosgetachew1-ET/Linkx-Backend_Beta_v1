import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

def replace_block(pattern_start, pattern_end, replacement, text):
    pattern = pattern_start + r".*?" + pattern_end
    return re.sub(pattern, replacement, text, flags=re.DOTALL)

batch_circ_rep = """        # 2. CIRCULAR_FLOW: direct account-to-beneficiary reversal
        # ----------------------------
        query = get_circular_flow_query(
            label=label,
            scope_clause_t="t.batch_id = $batch_id",
            scope_clause_a="a.batch_id = $batch_id",
            scope_clause_b="b.batch_id = $batch_id",
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=False
        )
        session.run(query, session_id=session_param, batch_id=batch_id, trusted_entries=trusted_entries, pt=pass_through_accounts)"""

text = replace_block(r"        # 2\. CIRCULAR_FLOW: direct account-to-beneficiary reversal\n        # ----------------------------\n        session.run\(f\"\"\"\n        MATCH \(a:\{label\}\), \(b:\{label\}\)\n        WHERE \(\$session_id IS NULL OR \(\{_session_scope_clause\(\"a\"\)\} AND \{_session_scope_clause\(\"b\"\)\}\)\)\n.*?MERGE \(b\)-\[r2:CIRCULAR_FLOW \{\{session_id:\$session_id\}\}\]->\(a\)\n        SET r2.bgcolor = '#e6e6e6', r2.provisional = false, r2.reason = 'same-day reverse transfer pair'\n        \"\"\", session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts\)", r"", batch_circ_rep, text)

incr_circ_rep = """        # Circular flow: only pairs where the current batch is one side of the reversal.
        query = get_circular_flow_query(
            label=label,
            scope_clause_t=_session_scope_clause("t"),
            scope_clause_a=_session_scope_clause("a"),
            scope_clause_b=_session_scope_clause("b"),
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=True,
            boundary_clause="(a.batch_id = $batch_id OR b.batch_id = $batch_id)"
        )
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)"""

text = replace_block(r"        # Circular flow: only pairs where the current batch is one side of the reversal.\n        session.run\(f\"\"\"\n        MATCH \(seed:\{label\}\)\n        WHERE seed.batch_id = \$batch_id\n.*?SET r2.bgcolor = '#e6e6e6', r2.provisional = true, r2.reason = 'same-day reverse transfer pair'\n        \"\"\", batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts\)", r"", incr_circ_rep, text)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(text)

