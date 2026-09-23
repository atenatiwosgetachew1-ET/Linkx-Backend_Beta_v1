import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    content = f.read()

# Fix ACCOUNT_ACTIVITY_SPIKE
# from: 
# query = get_account_activity_spike_query(
#     label=label,
#     scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
#     trusted_node_clause=_trusted_node_clause('t')
# )
# to:
# query = get_account_activity_spike_query(
#     label=label,
#     scope_clause_t="$session_id IS NULL OR t.session_id = $session_id"
# )
content = re.sub(
    r'query = get_account_activity_spike_query\(\s*label=label,\s*scope_clause_t="\$session_id IS NULL OR t\.session_id = \$session_id",\s*trusted_node_clause=_trusted_node_clause\(\'t\'\)\s*\)',
    'query = get_account_activity_spike_query(\n                    label=label,\n                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id"\n                )',
    content
)

# Fix HIGH_RISK_LINK
# from:
# query = get_high_risk_link_query(
#     label=label,
#     scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
#     trusted_node_clause=_trusted_node_clause('t'),
#     trusted_entry_match=_trusted_entry_match('t')
# )
# to:
# query = get_high_risk_link_query(
#     label=label,
#     scope_clause_t="$session_id IS NULL OR t.session_id = $session_id"
# )
content = re.sub(
    r'query = get_high_risk_link_query\(\s*label=label,\s*scope_clause_t="\$session_id IS NULL OR t\.session_id = \$session_id",\s*trusted_node_clause=_trusted_node_clause\(\'t\'\),\s*trusted_entry_match=_trusted_entry_match\(\'t\'\)\s*\)',
    'query = get_high_risk_link_query(\n                    label=label,\n                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id"\n                )',
    content
)

with open(file_path, 'w') as f:
    f.write(content)

print("Fixed consumer kwargs.")
