import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

def replace_block(pattern_start, pattern_end, replacement, text):
    pattern = pattern_start + r".*?" + pattern_end
    return re.sub(pattern, replacement, text, flags=re.DOTALL)

batch_dorm_rep = """        query = get_dormant_to_active_query(label=label, scope_clause_t="t.batch_id = $batch_id", is_provisional=False)
        session.run(query, session_id=session_param, batch_id=batch_id)"""
text = replace_block(r"        session.run\(f\"\"\"\n        MATCH \(t:\{label\}\)\n        WHERE \(\$session_id IS NULL OR \{_session_scope_clause\(\"t\"\)\}\)\n          AND coalesce\(t.IGNORE_LOGICAL, false\) = false\n          AND toLower\(coalesce\(t.ACCOUNTSTATE, ''\)\) = 'dormant'\n          AND toLower\(coalesce\(t.BENACCOUNTSTATE, ''\)\) = 'active'\n        MERGE \(t\)-\[r:DORMANT_TO_ACTIVE \{\{session_id:\$session_id\}\}\]->\(t\)\n        SET r.bgcolor = '#c20f0f', r.textcolor = '#eeeeee', r.provisional = false,\n            r.reason = 'dormant source account transacts with active beneficiary',\n            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false\n        \"\"\", session_id=session_param\)", r"", batch_dorm_rep, text)

incr_dorm_rep = """        query = get_dormant_to_active_query(label=label, scope_clause_t=_session_scope_clause("t"), is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param)"""
text = replace_block(r"        session.run\(f\"\"\"\n        MATCH \(t:\{label\}\)\n        WHERE t.batch_id = \$batch_id\n          AND coalesce\(t.IGNORE_LOGICAL, false\) = false\n          AND toLower\(coalesce\(t.ACCOUNTSTATE, ''\)\) = 'dormant'\n          AND toLower\(coalesce\(t.BENACCOUNTSTATE, ''\)\) = 'active'\n        MERGE \(t\)-\[r:DORMANT_TO_ACTIVE \{\{session_id:\$session_id\}\}\]->\(t\)\n        SET r.bgcolor = '#c20f0f', r.textcolor = '#eeeeee', r.provisional = true,\n            r.reason = 'dormant source account transacts with active beneficiary',\n            r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false\n        \"\"\", batch_id=batch_id, session_id=session_param\)", r"", incr_dorm_rep, text)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(text)

