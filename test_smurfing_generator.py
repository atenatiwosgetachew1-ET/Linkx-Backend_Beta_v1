import sys
sys.path.insert(0, '/var/www/linkx-backend/service_factory/services/linkx-worker/src')

from batch_manager.analyzing.LA_rules_script import get_smurfing_query

print("=== REAL-TIME CONSUMER QUERY ===")
rt_query = get_smurfing_query(
    label="`bank_transactions`",
    scope_clause_t="t.session_id = $session_id",
    trusted_pair_clause="NOT a.id IN $trusted AND NOT b.id IN $trusted",
    is_provisional=False,
    incremental_batch_id=None
)
print(rt_query)

print("\n=== 10K BATCH INCREMENTAL QUERY ===")
batch_query = get_smurfing_query(
    label="`bank_transactions`",
    scope_clause_t="t.session_id = $session_id",
    trusted_pair_clause="NOT a.id IN $trusted AND NOT b.id IN $trusted",
    is_provisional=True,
    incremental_batch_id="$batch_id"
)
print(batch_query)
