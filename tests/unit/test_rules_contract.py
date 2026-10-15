"""Contract verification tests for rule modules, rule_engine library, and LA_rules_script.

Tests:
1. Backward-compatibility of LA_rules_script.py (exports all required relationships & functions).
2. temp_rules contract (main & incremental entry points).
3. RuleDomain interface compliance for built-in domains (BankingDomain, CdrDomain, SocialMediaDomain).
4. RuleRegistry lookup, alias resolution, and relationship querying.
"""

import ast
import os
import pytest

from batch_manager.analyzing.rule_engine.base import RuleDomain
from batch_manager.analyzing.rule_engine.domains import (
    BankingDomain,
    CdrDomain,
    SocialMediaDomain,
)
from batch_manager.analyzing.rule_engine.registry import (
    RuleRegistry,
    default_registry,
    get_domain,
    get_relationships,
    list_domains,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LA_RULES_PATH = os.path.join(
    REPO_ROOT, "batch_manager", "analyzing", "LA_rules_script.py"
)


def _parse_top_level_names(filepath):
    """Return all top-level names (functions, variables, assignments, and imports) from a .py file."""
    with open(filepath, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=filepath)
    names = set()
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname or alias.name)
    return names


class TestLARulesScriptExports:
    """Verify LA_rules_script.py exports all required relationship lists and functions."""

    @classmethod
    @pytest.fixture(autouse=True, scope="class")
    def parsed_names(cls, request):
        request.cls.names = _parse_top_level_names(LA_RULES_PATH)

    def test_transaction_relationships_defined(self):
        assert "TRANSACTION_RELATIONSHIPS" in self.names

    def test_cdr_relationships_defined(self):
        assert "CDR_RELATIONSHIPS" in self.names

    def test_post_relationships_defined(self):
        assert "POST_RELATIONSHIPS" in self.names

    @pytest.mark.parametrize("func_name", [
        "batch_graph_analysis_transactions",
        "incremental_graph_analysis_transactions",
        "batch_graph_analysis_cdr",
        "incremental_graph_analysis_cdr",
        "batch_graph_analysis_posts",
        "incremental_graph_analysis_posts",
    ])
    def test_required_callable_exists(self, func_name):
        assert func_name in self.names, f"Missing top-level function: {func_name}"

    def test_runtime_backward_compat_import(self):
        """Verify that importing from LA_rules_script succeeds at runtime."""
        from batch_manager.analyzing import LA_rules_script as la

        assert isinstance(la.TRANSACTION_RELATIONSHIPS, list)
        assert len(la.TRANSACTION_RELATIONSHIPS) >= 14
        assert isinstance(la.CDR_RELATIONSHIPS, list)
        assert len(la.CDR_RELATIONSHIPS) >= 14
        assert isinstance(la.POST_RELATIONSHIPS, list)
        assert len(la.POST_RELATIONSHIPS) >= 6
        assert callable(la.batch_graph_analysis_transactions)
        assert callable(la.incremental_graph_analysis_transactions)
        assert callable(la.batch_graph_analysis_cdr)
        assert callable(la.incremental_graph_analysis_cdr)
        assert callable(la.batch_graph_analysis_posts)
        assert callable(la.incremental_graph_analysis_posts)


class TestTempRulesContract:
    """Verify temp_rules modules export main() and incremental() entrypoints (via AST)."""

    TEMP_RULES_DIR = os.path.join(REPO_ROOT, "public", "temp_rules")

    @pytest.mark.parametrize("module_file", [
        "bank_transactions_rules.py",
        "call_data_records_rules.py",
        "social_media_(tweeter)_rules.py",
    ])
    def test_temp_rule_entrypoints(self, module_file):
        filepath = os.path.join(self.TEMP_RULES_DIR, module_file)
        assert os.path.isfile(filepath), f"Rule file not found: {filepath}"
        names = _parse_top_level_names(filepath)
        assert "main" in names, f"{module_file} missing 'main' entrypoint"
        assert "incremental" in names, f"{module_file} missing 'incremental' entrypoint"


class TestRuleEngineLibraryContract:
    """Verify RuleDomain contract implementation across all built-in domains."""

    @pytest.mark.parametrize("domain_cls,expected_name,min_rels", [
        (BankingDomain, "bank transactions", 14),
        (CdrDomain, "call data records", 14),
        (SocialMediaDomain, "social media (tweeter)", 6),
    ])
    def test_domain_contract_compliance(self, domain_cls, expected_name, min_rels):
        domain = domain_cls()
        assert isinstance(domain, RuleDomain)
        assert domain.name == expected_name
        assert isinstance(domain.relationships, list)
        assert len(domain.relationships) >= min_rels
        assert callable(domain.create_indexes)
        assert callable(domain.clear_relationships)
        assert callable(domain.count_relationships)
        assert callable(domain.batch_analysis)
        assert callable(domain.incremental_analysis)


class TestRuleRegistry:
    """Verify registry discovery, resolution, and querying."""

    def test_default_registry_has_builtins(self):
        domains = list_domains()
        assert "bank transactions" in domains
        assert "call data records" in domains
        assert "social media (tweeter)" in domains

    @pytest.mark.parametrize("lookup_key,expected_canonical", [
        ("bank transactions", "bank transactions"),
        ("bank_transactions_rules", "bank transactions"),
        ("bank_transactions_rules.py", "bank transactions"),
        ("banking", "bank transactions"),
        ("call data records", "call data records"),
        ("call_data_records_rules", "call data records"),
        ("cdr", "call data records"),
        ("social media (tweeter)", "social media (tweeter)"),
        ("social_media_(tweeter)_rules", "social media (tweeter)"),
    ])
    def test_registry_resolution_and_aliases(self, lookup_key, expected_canonical):
        domain = get_domain(lookup_key)
        assert domain is not None
        assert domain.name == expected_canonical

    def test_get_relationships(self):
        rels = get_relationships("bank transactions")
        assert "SMURFING" in rels
        assert "CIRCULAR_FLOW" in rels
        assert "EFFECTIVE_FLOW" in rels

        cdr_rels = get_relationships("call data records")
        assert "CALL_SEQUENCE" in cdr_rels
        assert "CALLBACK_PATTERN" in cdr_rels

    def test_unknown_domain_returns_none_and_empty_list(self):
        assert get_domain("non_existent_domain") is None
        assert get_relationships("non_existent_domain") == []
