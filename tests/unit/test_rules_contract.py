"""Contract verification tests for rule modules and LA_rules_script.

Uses AST-based parsing so the test suite does not require runtime
dependencies (psycopg, neo4j driver, Classified_entities module)
that only exist on the deployed servers.
"""

import ast
import os
import importlib
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LA_RULES_PATH = os.path.join(
    REPO_ROOT, "batch_manager", "analyzing", "LA_rules_script.py"
)


def _parse_top_level_names(filepath):
    """Return all top-level names (functions, variables, assignments) from a .py file."""
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
    return names


class TestLARulesScriptExports:
    """Verify LA_rules_script.py exports all required relationship lists and functions (via AST)."""

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
