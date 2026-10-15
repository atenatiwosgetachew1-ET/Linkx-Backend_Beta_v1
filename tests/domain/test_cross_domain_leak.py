"""Cross-domain configuration isolation and leak detection tests.

Guarantees that telecom/CDR profiles never leak banking terms (ACCOUNTNO,
AMOUNTINBIRR, bankdb, etc.) and that banking/BDR profiles never leak
telecom terms (CALLING_NO, CALLED_NO, cdrdb, IMEI, etc.).
"""

import json
import os
import re
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOMAINS_DIR = os.path.join(REPO_ROOT, "domains")

BANKING_SPECIFIC_TERMS = [
    "amountinbirr",
    "amount_in_birr",
    "accountno",
    "benaccountno",
    "balanceheld",
    "bankdb",
    "bank_db",
    "individual_transactions",
    "entity_transactions",
]

CDR_SPECIFIC_TERMS = [
    "calling_no",
    "called_no",
    "callingnumber",
    "callednumber",
    "cdrdb",
    "cell_tower_id",
    "call_records",
    "sms_records",
    "call_duration",
]


def _flatten_values(data):
    """Recursively collect all string values from dict or list."""
    terms = []
    if isinstance(data, dict):
        for k, v in data.items():
            terms.append(str(k).lower())
            terms.extend(_flatten_values(v))
    elif isinstance(data, list):
        for item in data:
            terms.extend(_flatten_values(item))
    elif isinstance(data, str):
        terms.append(data.lower())
    return terms


def test_cdr_profile_has_no_banking_leaks():
    cdr_profile = os.path.join(DOMAINS_DIR, "cdr", "profile.json")
    with open(cdr_profile, "r", encoding="utf-8") as f:
        data = json.load(f)

    flat_strings = " ".join(_flatten_values(data))
    for term in BANKING_SPECIFIC_TERMS:
        assert term not in flat_strings, f"Leak detected! Banking term '{term}' found in CDR profile."


def test_banking_profile_has_no_cdr_leaks():
    bank_profile = os.path.join(DOMAINS_DIR, "banking", "profile.json")
    with open(bank_profile, "r", encoding="utf-8") as f:
        data = json.load(f)

    flat_strings = " ".join(_flatten_values(data))
    for term in CDR_SPECIFIC_TERMS:
        assert term not in flat_strings, f"Leak detected! CDR term '{term}' found in Banking profile."


def test_cdr_seed_sql_has_no_banking_leaks():
    cdr_seed = os.path.join(DOMAINS_DIR, "cdr", "seed.sql")
    if not os.path.isfile(cdr_seed):
        pytest.skip("domains/cdr/seed.sql not found")

    with open(cdr_seed, "r", encoding="utf-8") as f:
        content = f.read().lower()

    for term in BANKING_SPECIFIC_TERMS:
        assert term not in content, f"Leak detected! Banking term '{term}' found in domains/cdr/seed.sql"


def test_banking_seed_sql_has_no_cdr_leaks():
    bank_seed = os.path.join(DOMAINS_DIR, "banking", "seed.sql")
    if not os.path.isfile(bank_seed):
        pytest.skip("domains/banking/seed.sql not found")

    with open(bank_seed, "r", encoding="utf-8") as f:
        content = f.read().lower()

    for term in CDR_SPECIFIC_TERMS:
        assert term not in content, f"Leak detected! CDR term '{term}' found in domains/banking/seed.sql"
