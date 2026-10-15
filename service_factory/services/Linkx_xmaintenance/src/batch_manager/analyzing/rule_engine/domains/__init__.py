"""Domain rule implementations for LinkX rule engine.

Exports domain classes conforming to the RuleDomain contract:
- BankingDomain ("bank transactions")
- CdrDomain ("call data records")
- SocialMediaDomain ("social media (tweeter)")
"""

from typing import List, Dict, Any, Optional
from batch_manager.analyzing.rule_engine.base import RuleDomain
from batch_manager.analyzing.rule_engine.domains import banking, cdr, social_media


class BankingDomain(RuleDomain):
    """Banking / Bank Data Records (BDR) domain."""

    @property
    def name(self) -> str:
        return "bank transactions"

    @property
    def relationships(self) -> List[str]:
        return banking.TRANSACTION_RELATIONSHIPS

    def create_indexes(self, session, label: str) -> None:
        banking._create_transaction_indexes(session, label)

    def clear_relationships(self, session, session_id: str) -> None:
        banking._clear_transaction_relationships(session, session_id)

    def count_relationships(self, session, session_id: str) -> Dict[str, int]:
        return banking._count_transaction_relationships(session, session_id)

    def batch_analysis(self, driver, log_file, session_id=None, nodes_label="Transactions", **kwargs) -> Dict[str, int]:
        return banking.batch_graph_analysis_transactions(
            driver, log_file, session_id=session_id, nodes_label=nodes_label, **kwargs
        )

    def incremental_analysis(self, driver, session_id, nodes_label, batch_id, log_file, **kwargs) -> Dict[str, int]:
        return banking.incremental_graph_analysis_transactions(
            driver, session_id, nodes_label, batch_id, log_file, **kwargs
        )


class CdrDomain(RuleDomain):
    """Call Data Records (CDR) telecom domain."""

    @property
    def name(self) -> str:
        return "call data records"

    @property
    def relationships(self) -> List[str]:
        return cdr.CDR_RELATIONSHIPS

    def create_indexes(self, session, label: str) -> None:
        cdr._create_cdr_indexes(session, label)

    def clear_relationships(self, session, session_id: str) -> None:
        cdr._clear_cdr_relationships(session, session_id)

    def count_relationships(self, session, session_id: str) -> Dict[str, int]:
        return cdr._count_cdr_relationships(session, session_id)

    def batch_analysis(self, driver, log_file, session_id=None, nodes_label="CallDataRecords", **kwargs) -> Dict[str, int]:
        return cdr.batch_graph_analysis_cdr(
            driver, log_file, session_id=session_id, nodes_label=nodes_label, **kwargs
        )

    def incremental_analysis(self, driver, session_id, nodes_label, batch_id, log_file, **kwargs) -> Dict[str, int]:
        return cdr.incremental_graph_analysis_cdr(
            driver, session_id, nodes_label, batch_id, log_file, **kwargs
        )


class SocialMediaDomain(RuleDomain):
    """Social Media / Post domain."""

    @property
    def name(self) -> str:
        return "social media (tweeter)"

    @property
    def relationships(self) -> List[str]:
        return social_media.POST_RELATIONSHIPS

    def create_indexes(self, session, label: str) -> None:
        social_media._create_post_indexes(session, label)

    def clear_relationships(self, session, session_id: str) -> None:
        social_media._clear_post_relationships(session, session_id)

    def count_relationships(self, session, session_id: str) -> Dict[str, int]:
        return social_media._count_post_relationships(session, session_id)

    def batch_analysis(self, driver, log_file, session_id=None, nodes_label="Tweet", **kwargs) -> Dict[str, int]:
        return social_media.batch_graph_analysis_posts(
            driver, log_file, session_id=session_id, nodes_label=nodes_label
        )

    def incremental_analysis(self, driver, session_id, nodes_label, batch_id, log_file, **kwargs) -> Dict[str, int]:
        return social_media.incremental_graph_analysis_posts(
            driver, session_id, nodes_label, batch_id, log_file
        )


__all__ = [
    "BankingDomain",
    "CdrDomain",
    "SocialMediaDomain",
]
