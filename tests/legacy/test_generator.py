import sys
sys.path.append('/var/www/linkx-backend/service_factory/services/linkx-worker/src')

from batch_manager.analyzing.LA_rules_script import get_circular_flow_query, get_fund_flow_query

try:
    print("--- CIRCULAR FLOW ---")
    print(get_circular_flow_query(
        label="bank_transactions",
        scope_clause_t="t.session_id = 'test'",
        scope_clause_a="a.session_id = 'test'",
        scope_clause_b="b.session_id = 'test'",
        trusted_pair_clause="TRUE",
        is_provisional=False
    ))
    print("\n--- FUND FLOW ---")
    print(get_fund_flow_query(
        label="bank_transactions",
        scope_clause_t="t.session_id = 'test'",
        scope_clause_a="a.session_id = 'test'",
        scope_clause_b="b.session_id = 'test'",
        trusted_pair_clause="TRUE",
        is_provisional=False
    ))
except Exception as e:
    print("ERROR:", e)
