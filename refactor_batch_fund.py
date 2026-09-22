import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

def replace_block(pattern_start, pattern_end, replacement, text):
    pattern = pattern_start + r".*?" + pattern_end
    return re.sub(pattern, replacement, text, flags=re.DOTALL)

batch_fund_rep = """        # 3. FUND_FLOW: beneficiary becomes sender in a later transaction
        # ----------------------------
        query = get_fund_flow_query(
            label=label,
            scope_clause_t="t.batch_id = $batch_id",
            scope_clause_a="a.batch_id = $batch_id",
            scope_clause_b="b.batch_id = $batch_id",
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=False
        )
        session.run(query, session_id=session_param, batch_id=batch_id, trusted_entries=trusted_entries, pt=pass_through_accounts)"""

text = replace_block(r"        # 3\. FUND_FLOW: beneficiary becomes sender in a later transaction\n        # ----------------------------\n        session.run\(f\"\"\"\n        MATCH \(a:\{label\}\), \(b:\{label\}\)\n        WHERE \(\$session_id IS NULL OR \(\{_session_scope_clause\(\"a\"\)\} AND \{_session_scope_clause\(\"b\"\)\}\)\)\n.*?r\.edge_semantic = 'TEMPORAL_SEQUENCE', r\.financial_flow = false, r\.directed_display = true\n        \"\"\", session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts\)", r"", batch_fund_rep, text)

incr_fund_rep = """        # Fund flow: new nodes can either precede or complete a downstream flow.
        query = get_fund_flow_query(
            label=label,
            scope_clause_t=_session_scope_clause("t"),
            scope_clause_a=_session_scope_clause("a"),
            scope_clause_b=_session_scope_clause("b"),
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=True,
            boundary_clause="(a.batch_id = $batch_id OR b.batch_id = $batch_id)"
        )
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)"""

text = replace_block(r"        # Fund flow: new nodes can either precede or complete a downstream flow.\n        session.run\(f\"\"\"\n        MATCH \(a:\{label\}\), \(b:\{label\}\)\n        WHERE \(a.batch_id = \$batch_id OR b.batch_id = \$batch_id\)\n.*?r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true\n        \"\"\", batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts\)", r"", incr_fund_rep, text)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(text)

