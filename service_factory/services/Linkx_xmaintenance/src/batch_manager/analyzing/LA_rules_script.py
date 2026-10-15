"""Backward compatibility shim for LA_rules_script.

ALL existing imports across LinkX continue to work unchanged:
    from batch_manager.analyzing.LA_rules_script import TRANSACTION_RELATIONSHIPS
    from batch_manager.analyzing.LA_rules_script import batch_graph_analysis_transactions
    ...

The underlying implementations are now modularized in `batch_manager.analyzing.rule_engine`.
"""

# Thresholds & Dynamic Config
from batch_manager.analyzing.rule_engine.thresholds import fetch_rule_thresholds

# Shared Cypher constants, scope clauses, and entity matching
from batch_manager.analyzing.rule_engine.shared import (
    _CK_ACCTNO,
    _CK_BENTEL,
    _CK_BENTEL_SEED,
    _CK_BIZMOB,
    _CK_BIZMOB_SEED,
    _CK_DAYS,
    _CK_LOGACC,
    _CK_LOWENG,
    _CK_NEGSENT,
    _CK_SUSPCLUSTER,
    _CK_USER,
    _SID,
    _OPEN,
    _CLOSE,
    _safe_label,
    _session_scope_clause,
    _safe_index_name,
    _trusted_entry_match,
    _trusted_node_clause,
    _trusted_pair_clause,
    _risk_node_clause,
    _extract_pass_through_accounts,
    _is_trusted_entity_dict,
    risk_entities_cypher_entries,
    trusted_entities_cypher_entries,
)

# GDS Metrics
from batch_manager.analyzing.rule_engine.gds import (
    _cypher_string,
    _write_gds_metrics,
)

# Banking / BDR Domain
from batch_manager.analyzing.rule_engine.domains.banking import (
    TRANSACTION_RELATIONSHIPS,
    get_smurfing_query,
    get_circular_flow_query,
    get_fund_flow_query,
    get_dormant_to_active_query,
    get_abnormal_balance_query,
    get_hub_and_spoke_out_query,
    get_hub_and_spoke_in_query,
    get_shared_identifier_query,
    get_rapid_withdrawal_query,
    get_account_activity_spike_query,
    get_high_risk_link_query,
    get_late_night_tx_query,
    get_just_below_threshold_query,
    get_fraud_aggregator_query,
    _create_transaction_indexes,
    _clear_transaction_relationships,
    _count_transaction_relationships,
    execute_effective_flow_rule,
    execute_circular_flow_rule,
    execute_fund_flow_rule,
    get_logical_layer_query,
    batch_graph_analysis_transactions,
    incremental_graph_analysis_transactions,
)

# Social Media Domain
from batch_manager.analyzing.rule_engine.domains.social_media import (
    POST_RELATIONSHIPS,
    _create_post_indexes,
    _clear_post_relationships,
    _count_post_relationships,
    _run_post_rules,
    batch_graph_analysis_posts,
    incremental_graph_analysis_posts,
)

# CDR Domain
from batch_manager.analyzing.rule_engine.domains.cdr import (
    CDR_RELATIONSHIPS,
    _create_cdr_indexes,
    _clear_cdr_relationships,
    _count_cdr_relationships,
    _run_cdr_rules,
    batch_graph_analysis_cdr,
    incremental_graph_analysis_cdr,
)
