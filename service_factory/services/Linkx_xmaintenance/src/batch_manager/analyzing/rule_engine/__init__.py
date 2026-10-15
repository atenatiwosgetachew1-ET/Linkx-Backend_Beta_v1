"""LinkX Rule Engine Library.

A formal library for graph anomaly detection across financial, telecom, and social media domains.
Supports built-in and operator-uploaded rule modules under a unified contract.
"""

from batch_manager.analyzing.rule_engine.base import RuleDomain
from batch_manager.analyzing.rule_engine.registry import (
    RuleRegistry,
    default_registry,
    get_domain,
    list_domains,
    get_relationships,
)
from batch_manager.analyzing.rule_engine.thresholds import fetch_rule_thresholds
from batch_manager.analyzing.rule_engine.gds import _write_gds_metrics
from batch_manager.analyzing.rule_engine.domains import (
    BankingDomain,
    CdrDomain,
    SocialMediaDomain,
)
from batch_manager.analyzing.rule_engine.domains.banking import (
    TRANSACTION_RELATIONSHIPS,
    batch_graph_analysis_transactions,
    incremental_graph_analysis_transactions,
)
from batch_manager.analyzing.rule_engine.domains.cdr import (
    CDR_RELATIONSHIPS,
    batch_graph_analysis_cdr,
    incremental_graph_analysis_cdr,
)
from batch_manager.analyzing.rule_engine.domains.social_media import (
    POST_RELATIONSHIPS,
    batch_graph_analysis_posts,
    incremental_graph_analysis_posts,
)

__all__ = [
    # Contracts & Registry
    "RuleDomain",
    "RuleRegistry",
    "default_registry",
    "get_domain",
    "list_domains",
    "get_relationships",
    # Domain classes
    "BankingDomain",
    "CdrDomain",
    "SocialMediaDomain",
    # Relationship constants
    "TRANSACTION_RELATIONSHIPS",
    "CDR_RELATIONSHIPS",
    "POST_RELATIONSHIPS",
    # Main entry points
    "batch_graph_analysis_transactions",
    "incremental_graph_analysis_transactions",
    "batch_graph_analysis_cdr",
    "incremental_graph_analysis_cdr",
    "batch_graph_analysis_posts",
    "incremental_graph_analysis_posts",
    # Helpers
    "fetch_rule_thresholds",
    "_write_gds_metrics",
]
