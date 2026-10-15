"""Shared utilities for the LinkX rule engine.

Contains Cypher map-literal constants, scope clauses, trusted/risk entity
matching logic, label sanitization, and pass-through account extraction.
These are used by all domain rule modules.
"""

import re
from datetime import timedelta

try:
    from logger import log_writer
except ImportError:
    def log_writer(path, msg):
        print(msg, flush=True)

try:
    from batch_manager.utils.Classified_entities import (
        risk_entities_cypher_entries,
        trusted_entities_cypher_entries,
    )
except ImportError:
    risk_entities_cypher_entries = []
    trusted_entities_cypher_entries = []

# --- Cypher map-literal constants (Python 3.12 f-string compat) ---
_CK_ACCTNO = "{account_no: account_no, session_id: $session_id}"
_CK_BENTEL = "{kind:'BENTELNO', value:t.BENTELNO, account:t.BENACCOUNTNO}"
_CK_BENTEL_SEED = "{kind:'BENTELNO', value:seed.BENTELNO}"
_CK_BIZMOB = "{kind:'BUSINESSMOBILENO', value:t.BUSINESSMOBILENO, account:t.ACCOUNTNO}"
_CK_BIZMOB_SEED = "{kind:'BUSINESSMOBILENO', value:seed.BUSINESSMOBILENO}"
_CK_DAYS = "{days: $historical_baseline_days}"
_CK_LOGACC = "{LOGICAL_ACCOUNTNO: acc}"
_CK_LOWENG = "{flag:'LOW_ENG', session_id:$session_id}"
_CK_NEGSENT = "{flag:'NEG_SENTIMENT', session_id:$session_id}"
_CK_SUSPCLUSTER = "{type:'LOW_ENG_NEG_SENT', session_id:$session_id}"
_CK_USER = "{Username: coalesce(t.USERNAME, t.Username, t.username), session_id: $session_id}"
_SID = "{session_id:$session_id}"
_OPEN = "{"
_CLOSE = "}"
# --- End Cypher map-literal constants ---



def _safe_label(label):
    return f"`{str(label).replace('`', '')}`"


def _session_scope_clause(alias="t"):
    return f"($session_id IS NULL OR $session_id = '' OR {alias}.batch_id STARTS WITH $session_id OR {alias}.session_id = $session_id)"


def _safe_index_name(*parts):
    text = "_".join(str(part) for part in parts if part is not None)
    text = re.sub(r"[^A-Za-z0-9_]+", "_", text).strip("_").lower()
    return text or "idx"


def _trusted_entry_match(alias):
    # Support matching against both UI key/value format and legacy flat properties
    return (
        f"("
        f"(entry.key IS NOT NULL AND entry.value IS NOT NULL AND ("
        f"toString(coalesce({alias}[entry.key], {alias}[toLower(entry.key)], {alias}[toUpper(entry.key)], '')) = toString(entry.value) "
        f"OR (toUpper(entry.key) IN ['ACCOUNTNO', 'ACCOUNT_NO', 'ACCOUNT', 'BENACCOUNTNO'] AND ("
        f"toString(coalesce({alias}.ACCOUNTNO, {alias}.accountno, {alias}.LOGICAL_ACCOUNTNO, '')) = toString(entry.value) "
        f"OR toString(coalesce({alias}.BENACCOUNTNO, {alias}.benaccountno, {alias}.LOGICAL_BENACCOUNTNO, '')) = toString(entry.value)"
        f")) "
        f"OR (toUpper(entry.key) IN ['BUSINESSMOBILENO', 'PHONE', 'PHONENO', 'BENTELNO', 'MOBILENO', 'TELNO'] AND ("
        f"toString(coalesce({alias}.BUSINESSMOBILENO, {alias}.businessmobileno, '')) = toString(entry.value) "
        f"OR toString(coalesce({alias}.BENTELNO, {alias}.bentelno, '')) = toString(entry.value)"
        f")) "
        f"OR (toUpper(entry.key) IN ['CUSTOMERNAME', 'NAME', 'SENDER_FULL_NAME', 'RECEIVER_FULL_NAME'] AND ("
        f"toLower(toString(coalesce({alias}.SENDER_FULL_NAME, {alias}.sender_full_name, ''))) = toLower(toString(entry.value)) "
        f"OR toLower(toString(coalesce({alias}.RECEIVER_FULL_NAME, {alias}.receiver_full_name, ''))) = toLower(toString(entry.value))"
        f"))"
        f")) "
        f"OR "
        f"(entry.key IS NULL AND all(k IN keys(entry) WHERE toLower(k) IN ['category', 'type', 'reason', 'pass_through', 'passthrough', 'classification', 'notes', 'name', 'account', 'phone'] OR toString(coalesce({alias}[k], {alias}[toLower(k)], '')) = toString(entry[k])))"
        f")"
    )


def _trusted_node_clause(alias):
    return f"NOT any(entry IN $trusted_entries WHERE {_trusted_entry_match(alias)})"


def _trusted_pair_clause(left_alias, right_alias):
    return (
        "NOT any(entry IN $trusted_entries WHERE "
        f"({_trusted_entry_match(left_alias)} OR {_trusted_entry_match(right_alias)}))"
    )


def _risk_node_clause(alias):
    return f"any(entry IN $risk_entries WHERE {_trusted_entry_match(alias)})"


def _extract_pass_through_accounts(trusted_entities_raw):
    """Extract account numbers of entities marked as pass-through intermediaries.

    Supports both UI format ({"key": "ACCOUNTNO", "value": "..."}) and flat format ({"ACCOUNTNO": "..."}).
    Works with both the raw DB format (bool True) and stringified format ("True"/"true"/"1").
    """
    accounts = set()
    if isinstance(trusted_entities_raw, dict):
        trusted_entities_raw = trusted_entities_raw.get("trusted_entities") or []
    if not trusted_entities_raw or not isinstance(trusted_entities_raw, list):
        return list(accounts)
    for entity in trusted_entities_raw:
        if not isinstance(entity, dict):
            continue
        pt = entity.get("pass_through", entity.get("passthrough", ""))
        if str(pt).lower() in ("true", "1", "yes"):
            key = str(entity.get("key") or "").upper()
            val = entity.get("value")

            # If flat dictionary format
            if not val:
                for k in ("ACCOUNTNO", "accountno", "account_no", "account", "BENACCOUNTNO", "benaccountno"):
                    if entity.get(k):
                        val = entity.get(k)
                        break

            if val and str(val).strip():
                if key in ("ACCOUNTNO", "ACCOUNT_NO", "ACCOUNT", "BENACCOUNTNO", "") or any(k in entity for k in ("ACCOUNTNO", "accountno")):
                    val_str = str(val).strip()
                    accounts.add(val_str)
                    if val_str.isdigit():
                        try:
                            accounts.add(int(val_str))
                        except Exception:
                            pass
    return sorted(list(accounts), key=lambda x: str(x))


def _is_trusted_entity_dict(node_dict, trusted_entries):
    """Check if node dictionary matches any trusted entity entry (Python equivalent
    of the Cypher _trusted_entry_match)."""
    if not trusted_entries:
        return False
    for entry in trusted_entries:
        key = entry.get("key")
        val = entry.get("value")
        if key and val:
            val_str = str(val).strip()
            for k in (key, str(key).upper(), str(key).lower()):
                if str(node_dict.get(k, "")).strip() == val_str:
                    return True
            if str(key).upper() in ("ACCOUNTNO", "ACCOUNT_NO", "ACCOUNT", "BENACCOUNTNO"):
                for k in ("ACCOUNTNO", "BENACCOUNTNO", "LOGICAL_ACCOUNTNO", "LOGICAL_BENACCOUNTNO", "accountno", "benaccountno"):
                    if str(node_dict.get(k, "")).strip() == val_str:
                        return True
            if str(key).upper() in ("BUSINESSMOBILENO", "PHONE", "PHONENO", "BENTELNO", "MOBILENO", "TELNO"):
                for k in ("BUSINESSMOBILENO", "BENTELNO", "businessmobileno", "bentelno"):
                    if str(node_dict.get(k, "")).strip() == val_str:
                        return True
            if str(key).upper() in ("CUSTOMERNAME", "NAME", "SENDER_FULL_NAME", "RECEIVER_FULL_NAME"):
                for k in ("SENDER_FULL_NAME", "RECEIVER_FULL_NAME", "sender_full_name", "receiver_full_name"):
                    if str(node_dict.get(k, "")).strip().lower() == val_str.lower():
                        return True
        else:
            match = True
            for k, v in entry.items():
                if str(k).lower() in ("category", "type", "reason", "pass_through", "passthrough", "classification", "notes", "name", "key", "value", "account", "phone"):
                    continue
                if str(node_dict.get(k, "")).strip() != str(v).strip():
                    match = False
                    break
            if match:
                return True
    return False
