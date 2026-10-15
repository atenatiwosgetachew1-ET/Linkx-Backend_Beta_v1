"""Domain profile validation tests.

Validates that all profile.json files across domains/ adhere to the
required schema, have non-empty configurations, and valid field types.
"""

import json
import os
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOMAINS_DIR = os.path.join(REPO_ROOT, "domains")

REQUIRED_FIELDS = [
    "domain_id",
    "display_name",
    "active_rule",
    "default_source_col",
    "default_target_col",
    "default_relationship",
    "date_column",
    "storage_path",
    "storage_databases",
    "storage_tables",
    "search_columns_strict",
    "search_columns_fuzzy",
    "fetch_columns",
    "neo4j_node_label",
    "relationships",
]


def _get_active_domains():
    domains = []
    for item in os.listdir(DOMAINS_DIR):
        profile_path = os.path.join(DOMAINS_DIR, item, "profile.json")
        if os.path.isfile(profile_path):
            domains.append((item, profile_path))
    return domains


@pytest.mark.parametrize("domain_name,profile_path", _get_active_domains())
def test_domain_profile_schema(domain_name, profile_path):
    with open(profile_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for field in REQUIRED_FIELDS:
        assert field in data, f"Domain '{domain_name}' missing required field '{field}'"

    assert isinstance(data["active_rule"], list) and len(data["active_rule"]) > 0
    assert isinstance(data["storage_databases"], list) and len(data["storage_databases"]) > 0
    assert isinstance(data["storage_tables"], list) and len(data["storage_tables"]) > 0
    assert isinstance(data["fetch_columns"], list) and len(data["fetch_columns"]) > 0
    assert isinstance(data["relationships"], list) and len(data["relationships"]) > 0
    assert isinstance(data["default_source_col"], str) and len(data["default_source_col"]) > 0
    assert isinstance(data["default_target_col"], str) and len(data["default_target_col"]) > 0


def test_domain_template_validity():
    template_path = os.path.join(DOMAINS_DIR, "_template", "profile.json.template")
    assert os.path.isfile(template_path)
    with open(template_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for field in REQUIRED_FIELDS:
        assert field in data
