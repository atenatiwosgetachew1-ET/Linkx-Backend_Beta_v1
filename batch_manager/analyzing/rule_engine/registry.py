"""Rule engine registry — auto-discovery and resolution of rule domains.

Discovers domains from:
1. Built-in domains (banking, CDR, social_media)
2. Operator-uploaded rule modules (in public/temp_rules/ or custom paths)

Allows lookup by domain name or rule filename.
"""

import importlib
import inspect
import logging
import os
from typing import Dict, List, Optional, Type

from batch_manager.analyzing.rule_engine.base import RuleDomain
from batch_manager.analyzing.rule_engine.domains import (
    BankingDomain,
    CdrDomain,
    SocialMediaDomain,
)

logger = logging.getLogger("linkx.rule_engine.registry")


class RuleRegistry:
    """Central registry for discovering, registering, and retrieving RuleDomain instances."""

    def __init__(self):
        self._domains: Dict[str, RuleDomain] = {}
        self._filename_mapping: Dict[str, str] = {}
        self._register_builtins()

    def _register_builtins(self):
        """Register the default out-of-the-box domains."""
        self.register(BankingDomain(), aliases=["bank_transactions_rules", "banking", "bdr"])
        self.register(CdrDomain(), aliases=["call_data_records_rules", "cdr", "telecom"])
        self.register(SocialMediaDomain(), aliases=["social_media_(tweeter)_rules", "social_media", "tweeter"])

    def register(self, domain: RuleDomain, aliases: Optional[List[str]] = None) -> None:
        """Register a domain by its canonical name and optional alias names."""
        canonical_name = domain.name.lower().strip()
        self._domains[canonical_name] = domain
        if aliases:
            for alias in aliases:
                self._filename_mapping[alias.lower().strip()] = canonical_name

    def get_domain(self, name_or_alias: str) -> Optional[RuleDomain]:
        """Look up a domain by canonical name or alias (e.g. rule file name)."""
        if not name_or_alias:
            return None
        cleaned = name_or_alias.lower().strip()
        if cleaned.endswith(".py"):
            cleaned = cleaned[:-3]

        if cleaned in self._domains:
            return self._domains[cleaned]

        resolved_name = self._filename_mapping.get(cleaned)
        if resolved_name and resolved_name in self._domains:
            return self._domains[resolved_name]

        return None

    def list_domains(self) -> List[str]:
        """Return a list of all registered canonical domain names."""
        return sorted(list(self._domains.keys()))

    def get_relationships(self, name_or_alias: str) -> List[str]:
        """Return the relationship types defined for a domain, or empty list if not found."""
        domain = self.get_domain(name_or_alias)
        if domain:
            return domain.relationships
        return []

    def discover_uploaded_rules(self, directory: Optional[str] = None):
        """Optionally scan and register custom uploaded rule scripts."""
        if not directory:
            repo_root = os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            )
            directory = os.path.join(repo_root, "public", "temp_rules")

        if not os.path.isdir(directory):
            return

        for filename in os.listdir(directory):
            if filename.endswith(".py") and not filename.startswith("__"):
                mod_name = filename[:-3]
                file_path = os.path.join(directory, filename)
                try:
                    spec = importlib.util.spec_from_file_location(mod_name, file_path)
                    if spec and spec.loader:
                        mod = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(mod)
                        # Check for custom RuleDomain class
                        for _, obj in inspect.getmembers(mod, inspect.isclass):
                            if issubclass(obj, RuleDomain) and obj is not RuleDomain:
                                self.register(obj())
                                break
                except Exception as exc:
                    logger.debug("Could not auto-load uploaded rule module %s: %s", filename, exc)


# Global singleton registry
default_registry = RuleRegistry()


def get_domain(name: str) -> Optional[RuleDomain]:
    """Convenience helper to retrieve domain from default registry."""
    return default_registry.get_domain(name)


def list_domains() -> List[str]:
    """Convenience helper to list all domain names from default registry."""
    return default_registry.list_domains()


def get_relationships(name: str) -> List[str]:
    """Convenience helper to retrieve relationships for a domain."""
    return default_registry.get_relationships(name)
