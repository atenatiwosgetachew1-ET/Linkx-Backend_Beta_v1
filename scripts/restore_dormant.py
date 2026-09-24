import re

RULES_FILE = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"

with open(RULES_FILE, "r") as f:
    content = f.read()

dormant_func = '''def get_dormant_to_active_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = f"MATCH (t:{label}) WHERE t.batch_id = {incremental_batch_id} AND coalesce(t.IGNORE_LOGICAL, false) = false AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant' AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active' " if incremental_batch_id else f"MATCH (t:{label}) WHERE ({scope_clause_t}) AND coalesce(t.IGNORE_LOGICAL, false) = false AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant' AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active' "
    return f"""
    {seed_block}
    MERGE (t)-[r:DORMANT_TO_ACTIVE {{session_id:$session_id}}]->(t)
    SET r.is_evidence = true, r.anomaly_score = 0.4, r.bgcolor = '#c20f0f', r.textcolor = '#eeeeee', r.provisional = {prov_str},
        r.reason = 'dormant source account transacts with active beneficiary',
        r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
    """

'''

# Insert it back right before get_abnormal_balance_query
content = content.replace("def get_abnormal_balance_query", dormant_func + "def get_abnormal_balance_query")

with open(RULES_FILE, "w") as f:
    f.write(content)
