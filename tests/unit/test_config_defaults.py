"""Unit tests for batch_manager/config_defaults.py.

Verifies that get_default_session_config produces a valid session configuration
and respects environment variable overrides.
"""

import os
import pytest
from batch_manager.config_defaults import get_default_session_config


def test_default_session_config_structure():
    config = get_default_session_config("test-session-123")
    assert isinstance(config, dict)
    assert config["session_id"] == "test-session-123"

    expected_keys = [
        "rule_names",
        "rule_file_names",
        "active_rule",
        "default_source_col",
        "default_target_col",
        "default_relationship",
        "storage_path",
        "storage_databases",
        "storage_tables",
        "search_columns_strict",
        "search_columns_fuzzy",
        "fetch_columns",
        "tools",
        "active_tool",
    ]
    for key in expected_keys:
        assert key in config, f"Missing key in default session config: '{key}'"


def test_session_config_env_overrides(monkeypatch):
    monkeypatch.setenv("LINKX_DEFAULT_SOURCE_COL", "CUSTOM_SOURCE")
    monkeypatch.setenv("LINKX_DEFAULT_TARGET_COL", "CUSTOM_TARGET")
    monkeypatch.setenv("LINKX_STORAGE_PATH", "user/custom/data")

    config = get_default_session_config("override-session")
    assert config["default_source_col"] == "CUSTOM_SOURCE"
    assert config["default_target_col"] == "CUSTOM_TARGET"
    assert config["storage_path"] == "user/custom/data"
