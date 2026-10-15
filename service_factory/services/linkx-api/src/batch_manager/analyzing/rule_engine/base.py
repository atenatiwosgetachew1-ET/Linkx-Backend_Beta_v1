"""RuleDomain contract — formal interface for all LinkX rule domains.

Every domain (built-in or operator-uploaded) implements this interface,
ensuring uniform discovery, indexing, execution, and metric computation.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class RuleDomain(ABC):
    """Contract that every LinkX rule domain must implement.

    Built-in domains (banking, CDR, social media) and operator-uploaded
    rule modules both follow this interface. The registry discovers and
    loads them uniformly.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable domain name, e.g. 'bank transactions', 'call data records'."""
        pass

    @property
    @abstractmethod
    def relationships(self) -> List[str]:
        """List of Neo4j relationship types this domain creates."""
        pass

    @abstractmethod
    def create_indexes(self, session, label: str) -> None:
        """Create Neo4j indexes required for this domain's queries."""
        pass

    @abstractmethod
    def clear_relationships(self, session, session_id: str) -> None:
        """Remove all relationships created by this domain for a session."""
        pass

    @abstractmethod
    def count_relationships(self, session, session_id: str) -> Dict[str, int]:
        """Return {relationship_type: count} for this session."""
        pass

    @abstractmethod
    def batch_analysis(self, driver, log_file, session_id=None, nodes_label=None, **kwargs) -> Dict[str, int]:
        """Execute full batch analysis for this domain."""
        pass

    @abstractmethod
    def incremental_analysis(self, driver, session_id, nodes_label, batch_id, log_file, **kwargs) -> Dict[str, int]:
        """Execute incremental analysis on a single batch."""
        pass
