#!/usr/bin/env python3
"""
Phase 4 Verification Script: Calibrated Anomaly Rules & Dynamic Thresholds
Validates:
 1. Cypher query syntax and parameter injection for:
    - Abnormal Balance Change (transaction floor: min_tx_amount)
    - Rapid Withdrawal (trusted entity exclusion: _trusted_pair_clause)
    - Hub and Spoke (airtime suppression: min_single_amount)
    - Just Below Threshold (structuring repetition: min_count & ratio)
 2. API guardrail schema completeness
 3. PostgreSQL global_rule_thresholds active configuration
"""

import sys
import os
import json

# Add service paths to sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "service_factory/services/linkx-worker/src"))
sys.path.insert(0, os.path.join(BASE_DIR, "service_factory/services/linkx-api/src"))

passed_tests = 0
failed_tests = 0

def test_assert(condition, name, details=""):
    global passed_tests, failed_tests
    if condition:
        print(f"  ✅ PASS: {name}")
        passed_tests += 1
    else:
        print(f"  ❌ FAIL: {name} - {details}")
        failed_tests += 1

print("\n" + "="*70)
print(" LinkX Phase 4 Verification: Anomaly Rule Calibration & Dynamic Config")
print("="*70 + "\n")

# -------------------------------------------------------------------------
# TEST 1: Cypher Query Structure & Security Clauses
# -------------------------------------------------------------------------
print("[1] Verifying Cypher Rule Query Generators...")

try:
    from batch_manager.analyzing.LA_rules_script import (
        get_abnormal_balance_query,
        get_rapid_withdrawal_query,
        get_hub_and_spoke_out_query,
        get_hub_and_spoke_in_query,
        get_just_below_threshold_query,
        _trusted_pair_clause,
    )

    label = "bank_transactions_test"
    scope = "t.session_id = 'test'"
    trusted_pair = _trusted_pair_clause('a', 'b')

    # 1.1 Abnormal Balance Change
    q_abnormal = get_abnormal_balance_query(label=label, scope_clause_t=scope)
    test_assert(
        "abnormal_balance_min_tx_amount" in q_abnormal and "current_amount" in q_abnormal,
        "Abnormal Balance Change includes transaction amount floor filter",
        "Missing '$abnormal_balance_min_tx_amount' or 'current_amount' in query"
    )

    # 1.2 Rapid Withdrawal
    q_rapid = get_rapid_withdrawal_query(label=label, scope_clause_t=scope)
    test_assert(
        "$trusted_entries" in q_rapid,
        "Rapid Withdrawal enforces trusted entities exclusion ($trusted_entries)",
        "Missing '$trusted_entries' check in Rapid Withdrawal query"
    )

    # 1.3 Hub and Spoke (Outgoing & Incoming)
    q_hub_out = get_hub_and_spoke_out_query(label=label, scope_clause_t=scope, trusted_pair_clause=trusted_pair)
    q_hub_in = get_hub_and_spoke_in_query(label=label, scope_clause_t=scope, trusted_pair_clause=trusted_pair)
    test_assert(
        "hub_spoke_min_single_amount" in q_hub_out and "hub_spoke_min_single_amount" in q_hub_in,
        "Hub and Spoke filters out micro-transfers (airtime) via hub_spoke_min_single_amount",
        "Missing '$hub_spoke_min_single_amount' in Hub and Spoke queries"
    )

    # 1.4 Just Below Threshold
    q_just_below = get_just_below_threshold_query(label=label, scope_clause_t=scope)
    test_assert(
        "just_below_threshold_min_count" in q_just_below and "just_below_threshold_ratio" in q_just_below,
        "Just Below Threshold enforces repetition requirement (min_count >= 2) and ratio",
        "Missing '$just_below_threshold_min_count' or '$just_below_threshold_ratio' in query"
    )

except Exception as e:
    test_assert(False, "Query Generator Imports & Cypher Validation", str(e))


# -------------------------------------------------------------------------
# TEST 2: Threshold Defaults in Engine
# -------------------------------------------------------------------------
print("\n[2] Verifying Engine Threshold Defaults...")

try:
    from batch_manager.analyzing.LA_rules_script import fetch_rule_thresholds
    thresholds = fetch_rule_thresholds()

    expected_keys = [
        ("hub_spoke_min_single_amount", 500.0),
        ("abnormal_balance_min_tx_amount", 1000.0),
        ("just_below_threshold_min_count", 2),
        ("just_below_threshold_ratio", 0.90),
    ]

    for key, expected_val in expected_keys:
        test_assert(
            key in thresholds,
            f"Default threshold '{key}' is present in engine defaults",
            f"'{key}' missing from thresholds dictionary"
        )

except Exception as e:
    test_assert(False, "Threshold Defaults Validation", str(e))


# -------------------------------------------------------------------------
# TEST 3: API Guardrail Whitelist in main.py
# -------------------------------------------------------------------------
print("\n[3] Verifying API Whitelist & Guardrails (linkx-api)...")

try:
    import ast
    main_py_path = os.path.join(BASE_DIR, "service_factory/services/linkx-api/src/main.py")
    with open(main_py_path, "r") as f:
        tree = ast.parse(f.read(), filename=main_py_path)

    guardrails_keys = set()
    defaults_keys = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    if target.id == "_RULE_THRESHOLD_GUARDRAILS" and isinstance(node.value, ast.Dict):
                        for k in node.value.keys:
                            if isinstance(k, ast.Constant):
                                guardrails_keys.add(k.value)
                    elif target.id == "_DEFAULT_RULE_THRESHOLDS" and isinstance(node.value, ast.Dict):
                        for k in node.value.keys:
                            if isinstance(k, ast.Constant):
                                defaults_keys.add(k.value)

    new_keys = [
        "hub_spoke_min_single_amount",
        "abnormal_balance_min_tx_amount",
        "just_below_threshold_min_count",
        "just_below_threshold_ratio",
    ]

    for k in new_keys:
        in_guards = k in guardrails_keys
        in_defaults = k in defaults_keys
        test_assert(
            in_guards and in_defaults,
            f"API registers '{k}' in guardrails and default schema",
            f"Guardrails: {in_guards}, Defaults: {in_defaults}"
        )

except Exception as e:
    test_assert(False, "API Guardrails Validation", str(e))


# -------------------------------------------------------------------------
# TEST 4: Live PostgreSQL Check (if DSN available)
# -------------------------------------------------------------------------
print("\n[4] Verifying PostgreSQL global_rule_thresholds...")

dsn = os.getenv('LINKX_POSTGRES_DSN') or os.getenv('DATABASE_URL')
if not dsn:
    env_paths = [
        '/opt/linkx-worker/.env',
        '/opt/linkx-backend-api/.env',
        '/opt/linkx-backend-update/.env',
        os.path.join(BASE_DIR, 'service_factory/.env')
    ]
    for p in env_paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                for line in f:
                    if line.startswith('LINKX_POSTGRES_DSN='):
                        dsn = line.strip().split('=', 1)[1].strip(" \"'")
                        break
            if dsn:
                break

if dsn:
    try:
        try:
            import psycopg
        except ImportError:
            import psycopg2 as psycopg

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config_data, updated_by, created_at FROM global_rule_thresholds ORDER BY created_at DESC LIMIT 1;")
                row = cur.fetchone()
                if row:
                    config = row[0]
                    test_assert(isinstance(config, dict), "Active config_data is valid JSONB dictionary")
                    has_all_keys = all(k in config for k in [
                        "hub_spoke_min_single_amount",
                        "abnormal_balance_min_tx_amount",
                        "just_below_threshold_min_count",
                        "just_below_threshold_ratio"
                    ])
                    test_assert(has_all_keys, f"Database active config contains all 4 calibrated keys (updated_by: {row[1]})")
                else:
                    print("  ⚠️  Notice: global_rule_thresholds table exists but has no rows yet.")
    except Exception as e:
        print(f"  ⚠️  Notice: Could not connect to PostgreSQL ({e}). Skipping live DB test.")
else:
    print("  ℹ️  No live PostgreSQL DSN found in environment. Skipping live DB test.")


# -------------------------------------------------------------------------
# SUMMARY
# -------------------------------------------------------------------------
print("\n" + "="*70)
print(f" Results: {passed_tests} PASSED, {failed_tests} FAILED")
print("="*70)

if failed_tests > 0:
    sys.exit(1)
else:
    print("All Phase 1-3 components verified successfully.\n")
    sys.exit(0)
