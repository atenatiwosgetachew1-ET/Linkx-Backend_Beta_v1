import psycopg
import os
from datetime import datetime

def fetch_rule_thresholds():
    defaults = {
        "smurfing_single_tx_threshold": 300000,
        "smurfing_min_tx_count": 3,
        "smurfing_cumulative_threshold": 900000,
        "reporting_threshold": 300000,
        "circular_flow_check_amounts": False,
        "circular_flow_amount_tolerance": 0.05,
        "late_night_start": 2300,
        "late_night_end": 400,
        "hub_spoke_min_counterparties": 3,
        "activity_spike_multiplier": 3,
        "activity_spike_min_daily_count": 10,
        "rapid_withdrawal_amount_tolerance": 0.1
    }
    try:
        dsn = os.getenv('DATABASE_URL') or os.getenv('LINKX_POSTGRES_DSN')
        
        if dsn:
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT config_data FROM global_rule_thresholds ORDER BY created_at DESC LIMIT 1")
                    row = cur.fetchone()
                    if row and row[0]:
                        defaults.update(row[0])
    except Exception as e:
        print(f"fetch_rule_thresholds error: {e}", flush=True)
    return defaults

from datetime import timedelta
from logger import log_writer
import re
from batch_manager.utils.Classified_entities import risk_entities_cypher_entries, trusted_entities_cypher_entries

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
    # Support matching against both raw and logical fields
    return f"all(k IN keys(entry) WHERE toLower(k) IN ['category', 'type', 'reason', 'pass_through', 'passthrough', 'classification', 'notes'] OR toString(coalesce({alias}[k], \"\")) = toString(entry[k]))"


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

    Works with both the raw DB format (bool ``True``) and the stringified
    Cypher-parameter format (``"True"``/``"true"``/``"1"``).
    """
    accounts = set()
    if not trusted_entities_raw or not isinstance(trusted_entities_raw, list):
        return list(accounts)
    for entity in trusted_entities_raw:
        if not isinstance(entity, dict):
            continue
        pt = entity.get("pass_through", entity.get("passthrough", ""))
        if str(pt).lower() in ("true", "1", "yes"):
            for key in ("ACCOUNTNO", "accountno", "account_no", "account"):
                val = entity.get(key)
                if val and str(val).strip():
                    accounts.add(str(val).strip())
                    break
    return list(accounts)


TRANSACTION_RELATIONSHIPS = [
    "EFFECTIVE_FLOW",
    "SMURFING",
    "CIRCULAR_FLOW",
    "FUND_FLOW",
    "DORMANT_TO_ACTIVE",
    "HIGH_RISK_LINK",
    "ABNORMAL_BALANCE_CHANGE",
    "HUB_AND_SPOKE",
    "SHARED_IDENTIFIER",
    "PEP_INVOLVED",
    "SANCTIONED_ENTITY_MATCH",
    "LATE_NIGHT_TX",
    "JUST_BELOW_THRESHOLD",
    "RAPID_WITHDRAWAL",
    "ACCOUNT_ACTIVITY_SPIKE",
]


# --- RULE GENERATORS (PHASE 1) ---

def get_smurfing_query(label, scope_clause_t, trusted_pair_clause, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_t_filters = ""
    if incremental_batch_id:
        seed_block = f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = {incremental_batch_id}
        WITH DISTINCT seed.LOGICAL_ACCOUNTNO AS acc, seed.LOGICAL_BENACCOUNTNO AS beneficiary, seed.TRANSACTIONDATE AS tx_day
        WHERE acc IS NOT NULL AND acc <> ''
          AND beneficiary IS NOT NULL AND beneficiary <> ''
          AND tx_day IS NOT NULL AND tx_day <> ''
        """
        match_t_filters = "AND t.LOGICAL_ACCOUNTNO = acc AND t.LOGICAL_BENACCOUNTNO = beneficiary AND t.TRANSACTIONDATE = tx_day"
    
    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {match_t_filters}
    WITH
        {"t.LOGICAL_ACCOUNTNO AS acc," if not incremental_batch_id else "acc,"}
        {"t.LOGICAL_BENACCOUNTNO AS beneficiary," if not incremental_batch_id else "beneficiary,"}
        {"t.TRANSACTIONDATE AS tx_day," if not incremental_batch_id else "tx_day,"}
        t, coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amount
    WHERE {"acc IS NOT NULL AND acc <> '' AND beneficiary IS NOT NULL AND beneficiary <> '' AND tx_day IS NOT NULL AND tx_day <> '' AND " if not incremental_batch_id else ""} amount IS NOT NULL AND amount > 0 AND amount < $smurfing_single_tx_threshold
    WITH acc, beneficiary, tx_day, t, amount
    ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
    WITH acc, beneficiary, tx_day, collect(t) AS txns, sum(amount) AS total_amount, count(t) AS tx_count
    WHERE tx_count >= $smurfing_min_tx_count AND total_amount >= $smurfing_cumulative_threshold
    UNWIND range(0, size(txns)-2) AS i
    WITH txns[i] AS a, txns[i+1] AS b, acc, beneficiary, tx_day, tx_count, total_amount
    WHERE {trusted_pair_clause}
    MERGE (a)-[r:SMURFING {_SID}]->(b)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#d5d276', r.provisional = {prov_str},
        r.reason = 'multiple small same-day transfers below threshold',
        r.account = acc, r.beneficiary = beneficiary, r.tx_day = tx_day,
        r.tx_count = tx_count, r.total_amount = total_amount,
        r.single_tx_threshold = $smurfing_single_tx_threshold, r.total_threshold = $smurfing_cumulative_threshold,
        r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    """

def get_circular_flow_query(
    label,
    scope_clause_t,
    trusted_pair_clause,
    is_provisional=False,
    incremental_batch_id=None,
    scope_clause_a=None,
    scope_clause_b=None,
    boundary_clause=None,
    **kwargs
):
    prov_str = "true" if is_provisional else "false"

    if incremental_batch_id is not None:
        seed_block = f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $incremental_batch_id
          AND coalesce(seed.IGNORE_LOGICAL, false) = false
          AND seed.LOGICAL_ACCOUNTNO IS NOT NULL
          AND trim(toString(seed.LOGICAL_ACCOUNTNO)) <> ''
          AND seed.LOGICAL_BENACCOUNTNO IS NOT NULL
          AND trim(toString(seed.LOGICAL_BENACCOUNTNO)) <> ''
          AND seed.TRANSACTIONDATE IS NOT NULL
          AND trim(toString(seed.TRANSACTIONDATE)) <> ''

        WITH DISTINCT
            trim(toString(seed.LOGICAL_ACCOUNTNO)) AS seed_sender,
            trim(toString(seed.LOGICAL_BENACCOUNTNO)) AS seed_receiver,
            trim(toString(seed.TRANSACTIONDATE)) AS seed_day

        WHERE seed_sender <> seed_receiver
          AND NOT seed_sender IN $pt
          AND NOT seed_receiver IN $pt
        """
    else:
        scope_clause_seed = scope_clause_t.replace("t.", "seed.")

        seed_block = f"""
        MATCH (seed:{label})
        WHERE ({scope_clause_seed})
          AND coalesce(seed.IGNORE_LOGICAL, false) = false
          AND seed.LOGICAL_ACCOUNTNO IS NOT NULL
          AND trim(toString(seed.LOGICAL_ACCOUNTNO)) <> ''
          AND seed.LOGICAL_BENACCOUNTNO IS NOT NULL
          AND trim(toString(seed.LOGICAL_BENACCOUNTNO)) <> ''
          AND seed.TRANSACTIONDATE IS NOT NULL
          AND trim(toString(seed.TRANSACTIONDATE)) <> ''

        WITH DISTINCT
            trim(toString(seed.LOGICAL_ACCOUNTNO)) AS seed_sender,
            trim(toString(seed.LOGICAL_BENACCOUNTNO)) AS seed_receiver,
            trim(toString(seed.TRANSACTIONDATE)) AS seed_day

        WHERE seed_sender <> seed_receiver
          AND NOT seed_sender IN $pt
          AND NOT seed_receiver IN $pt
        """

    match_a = f"""
    MATCH (a:{label})
    WHERE ({scope_clause_a})
      AND coalesce(a.IGNORE_LOGICAL, false) = false

      AND a.LOGICAL_ACCOUNTNO IS NOT NULL
      AND trim(toString(a.LOGICAL_ACCOUNTNO)) <> ''

      AND a.LOGICAL_BENACCOUNTNO IS NOT NULL
      AND trim(toString(a.LOGICAL_BENACCOUNTNO)) <> ''

      AND trim(toString(a.LOGICAL_ACCOUNTNO)) = seed_sender
      AND trim(toString(a.LOGICAL_BENACCOUNTNO)) = seed_receiver
      AND trim(toString(a.TRANSACTIONDATE)) = seed_day

      AND NOT trim(toString(a.LOGICAL_ACCOUNTNO)) IN $pt
      AND NOT trim(toString(a.LOGICAL_BENACCOUNTNO)) IN $pt
    """

    match_b = f"""
    MATCH (b:{label})
    WHERE ({scope_clause_b})
      AND coalesce(b.IGNORE_LOGICAL, false) = false

      AND b.LOGICAL_ACCOUNTNO IS NOT NULL
      AND trim(toString(b.LOGICAL_ACCOUNTNO)) <> ''

      AND b.LOGICAL_BENACCOUNTNO IS NOT NULL
      AND trim(toString(b.LOGICAL_BENACCOUNTNO)) <> ''

      AND trim(toString(b.LOGICAL_ACCOUNTNO)) = seed_receiver
      AND trim(toString(b.LOGICAL_BENACCOUNTNO)) = seed_sender
      AND trim(toString(b.TRANSACTIONDATE)) = seed_day

      AND NOT trim(toString(b.LOGICAL_ACCOUNTNO)) IN $pt
      AND NOT trim(toString(b.LOGICAL_BENACCOUNTNO)) IN $pt

      AND elementId(a) < elementId(b)
    """

    return f"""
    {seed_block}

    {match_a}

    {match_b}

    WITH
        a,
        b,

        trim(toString(a.LOGICAL_ACCOUNTNO)) AS sender_a,
        trim(toString(a.LOGICAL_BENACCOUNTNO)) AS receiver_a,

        trim(toString(b.LOGICAL_ACCOUNTNO)) AS sender_b,
        trim(toString(b.LOGICAL_BENACCOUNTNO)) AS receiver_b,

        coalesce(
            toFloat(a.AMOUNTINBIRR),
            toFloat(a.AMOUNT),
            toFloat(a.amount),
            toFloat(a.LOCAL_AMOUNT),
            0.0
        ) AS amt_a,

        coalesce(
            toFloat(b.AMOUNTINBIRR),
            toFloat(b.AMOUNT),
            toFloat(b.amount),
            toFloat(b.LOCAL_AMOUNT),
            0.0
        ) AS amt_b

    WHERE
        sender_a = receiver_b
        AND receiver_a = sender_b

        AND sender_a <> receiver_a

        AND amt_a > 0
        AND amt_b > 0

        AND abs(amt_a - amt_b)
            <= (
                CASE
                    WHEN amt_a < amt_b THEN amt_a
                    ELSE amt_b
                END * 0.05
            )

        AND {trusted_pair_clause}

    CALL (
        a,
        b,
        sender_a,
        receiver_a,
        sender_b,
        receiver_b,
        amt_a,
        amt_b
    ) {_OPEN}

        MERGE (a)-[r1:CIRCULAR_FLOW {_SID}]->(b)

        SET
            r1.is_evidence = true,
            r1.anomaly_score = 0.6,
            r1.bgcolor = '#e6e6e6',
            r1.provisional = {prov_str},

            r1.reason =
                CASE
                    WHEN coalesce(a.PASSTHROUGH_HOPS, 0) > 0
                      OR coalesce(b.PASSTHROUGH_HOPS, 0) > 0
                    THEN 'logical same-day reverse flow with pass-through intermediary'
                    ELSE 'same-day direct reverse transfer'
                END,

            r1.logical_sender = sender_a,
            r1.logical_receiver = receiver_a,

            r1.reverse_sender = sender_b,
            r1.reverse_receiver = receiver_b,

            r1.amount_a = amt_a,
            r1.amount_b = amt_b,

            r1.passthrough_a = coalesce(a.PASSTHROUGH_HOPS, 0),
            r1.passthrough_b = coalesce(b.PASSTHROUGH_HOPS, 0),

            r1.logical_path_a = coalesce(a.LOGICAL_PATH, ''),
            r1.logical_path_b = coalesce(b.LOGICAL_PATH, ''),

            r1.logical_transformation_a = coalesce(a.LOGICAL_TRANSFORMATION_REASON, 'NONE'),
            r1.logical_transformation_b = coalesce(b.LOGICAL_TRANSFORMATION_REASON, 'NONE'),

            r1.edge_semantic = 'DERIVED_LOGICAL_FLOW',
            r1.financial_flow = true,
            r1.directed_display = true


        MERGE (b)-[r2:CIRCULAR_FLOW {_SID}]->(a)

        SET
            r2.is_evidence = true,
            r2.anomaly_score = 0.6,
            r2.bgcolor = '#e6e6e6',
            r2.provisional = {prov_str},

            r2.reason =
                CASE
                    WHEN coalesce(a.PASSTHROUGH_HOPS, 0) > 0
                      OR coalesce(b.PASSTHROUGH_HOPS, 0) > 0
                    THEN 'logical same-day reverse flow with pass-through intermediary'
                    ELSE 'same-day direct reverse transfer'
                END,

            r2.logical_sender = receiver_a,
            r2.logical_receiver = sender_a,

            r2.reverse_sender = sender_a,
            r2.reverse_receiver = receiver_a,

            r2.amount_a = amt_a,
            r2.amount_b = amt_b,

            r2.passthrough_a = coalesce(a.PASSTHROUGH_HOPS, 0),
            r2.passthrough_b = coalesce(b.PASSTHROUGH_HOPS, 0),

            r2.logical_path_a = coalesce(a.LOGICAL_PATH, ''),
            r2.logical_path_b = coalesce(b.LOGICAL_PATH, ''),

            r2.logical_transformation_a = coalesce(a.LOGICAL_TRANSFORMATION_REASON, 'NONE'),
            r2.logical_transformation_b = coalesce(b.LOGICAL_TRANSFORMATION_REASON, 'NONE'),

            r2.edge_semantic = 'DERIVED_LOGICAL_FLOW',
            r2.financial_flow = true,
            r2.directed_display = true

    {_CLOSE} IN TRANSACTIONS OF 5000 ROWS
    """


def get_fund_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False, boundary_clause=None, **kwargs):
    prov_str = "true" if is_provisional else "false"
    boundary_str = f"AND ({boundary_clause})" if boundary_clause else ""
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count < 1000 AND NOT acc IN $pt

    MATCH (a:{label})
    WHERE ({scope_clause_a}) AND a.LOGICAL_BENACCOUNTNO = acc
    
    // Force planner to resolve 'a' before scanning 'b'
    WITH acc, a

    MATCH (b:{label})
    WHERE ({scope_clause_b}) AND b.LOGICAL_ACCOUNTNO = acc
      AND elementId(a) <> elementId(b)
      AND (
        coalesce(a.TRANSACTIONDATE, '') < coalesce(b.TRANSACTIONDATE, '')
        OR (
          coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
          AND coalesce(a.TRANSACTIONTIME, '') < coalesce(b.TRANSACTIONTIME, '')
        )
      )
      AND {trusted_pair_clause}
      {boundary_str}

    WITH a, b
    ORDER BY b.TRANSACTIONDATE ASC, b.TRANSACTIONTIME ASC
    WITH a, collect(b) AS downstream
    WITH a, downstream[..5] AS limited_downstream
    UNWIND limited_downstream AS b

    CALL {_OPEN}
      WITH a, b
      MERGE (a)-[r:FUND_FLOW {_SID}]->(b)
      SET r.is_evidence = true, r.anomaly_score = 0.5, r.bgcolor = '#d8a822', r.provisional = {prov_str},
          r.reason = 'beneficiary later acts as sender',
          r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    {_CLOSE} IN TRANSACTIONS OF 5000 ROWS
    """

def get_dormant_to_active_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = f"MATCH (t:{label}) WHERE t.batch_id = {incremental_batch_id} AND coalesce(t.IGNORE_LOGICAL, false) = false AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant' AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active' " if incremental_batch_id else f"MATCH (t:{label}) WHERE ({scope_clause_t}) AND coalesce(t.IGNORE_LOGICAL, false) = false AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant' AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active' "
    return f"""
    {seed_block}
    MERGE (t)-[r:DORMANT_TO_ACTIVE {_SID}]->(t)
    SET r.is_evidence = true, r.anomaly_score = 0.4, r.bgcolor = '#c20f0f', r.textcolor = '#eeeeee', r.provisional = {prov_str},
        r.reason = 'dormant source account transacts with active beneficiary',
        r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
    """

def get_abnormal_balance_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} WITH DISTINCT seed.LOGICAL_ACCOUNTNO AS acc, coalesce(seed.TRANSACTIONDATE, toString(date())) AS seed_day WHERE acc IS NOT NULL AND acc <> ''"
        match_filters = "AND t.LOGICAL_ACCOUNTNO = acc AND coalesce(t.TRANSACTIONDATE, '') >= toString(date(seed_day) - duration({days: $historical_baseline_days}))"
        
    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {match_filters if incremental_batch_id else "AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''"}
    WITH {"acc, t" if incremental_batch_id else "t.LOGICAL_ACCOUNTNO AS acc, t"}
    ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
    WITH acc, collect(t) AS txns
    WHERE size(txns) >= 5
    UNWIND range(3, size(txns)-1) AS i
    WITH txns[i] AS current, txns[i-1] AS previous, txns[0..i] AS history
    WITH current, previous,
         abs(coalesce(toFloat(current.SENDERPREVIOUSBALANCE), 0.0) - coalesce(toFloat(previous.SENDERPREVIOUSBALANCE), 0.0)) AS current_change,
         history
    WHERE current_change > 0
    WITH current, previous, current_change, history
    WITH current, previous, current_change,
         [idx IN range(1, size(history)-1) | abs(coalesce(toFloat(history[idx].SENDERPREVIOUSBALANCE), 0.0) - coalesce(toFloat(history[idx-1].SENDERPREVIOUSBALANCE), 0.0))] AS history_changes
    WITH current, previous, current_change,
         CASE WHEN size(history_changes) > 0 THEN reduce(s = 0.0, x IN history_changes | s + x) / size(history_changes) ELSE 0.0 END AS avg_change
    WHERE current_change > (avg_change * 3)
    MERGE (previous)-[r:ABNORMAL_BALANCE_CHANGE {_SID}]->(current)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#196e08', r.textcolor = '#eeeeee', r.provisional = {prov_str},
        r.reason = 'balance change exceeds recent account baseline',
        r.change = current_change, r.average_recent_change = avg_change, r.threshold_multiplier = 3,
        r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    """

def get_hub_and_spoke_out_query(label, scope_clause_t, trusted_pair_clause, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} WITH DISTINCT seed.LOGICAL_ACCOUNTNO AS hub, seed.TRANSACTIONDATE AS tx_day WHERE hub IS NOT NULL AND hub <> '' AND tx_day IS NOT NULL AND tx_day <> '' AND NOT hub IN $pt"
        match_filters = "AND t.LOGICAL_ACCOUNTNO = hub AND t.TRANSACTIONDATE = tx_day"
        
    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_BENACCOUNTNO IS NOT NULL AND t.LOGICAL_BENACCOUNTNO <> ''
      {match_filters if incremental_batch_id else "AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> ''"}
    WITH {"hub, tx_day," if incremental_batch_id else "t.LOGICAL_ACCOUNTNO AS hub, t.TRANSACTIONDATE AS tx_day,"} collect(t) AS txns, count(DISTINCT t.LOGICAL_BENACCOUNTNO) AS spoke_count
    WHERE {"spoke_count >= $hub_spoke_min_counterparties" if incremental_batch_id else "hub IS NOT NULL AND hub <> '' AND NOT hub IN $pt AND spoke_count >= $hub_spoke_min_counterparties"} AND size(txns) < 1000
    CALL (txns, hub, tx_day, spoke_count) {_OPEN}
      UNWIND range(0, size(txns)-2) AS i
      WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
      WHERE {trusted_pair_clause}
      MERGE (a)-[r:HUB_AND_SPOKE {_SID}]->(b)
      SET r.is_evidence = true, r.anomaly_score = 0.5, r.bgcolor = '#6f42c1', r.textcolor = '#eeeeee', r.provisional = {prov_str},
          r.reason = 'account connects with multiple counterparties on same day',
          r.hub_account = hub, r.direction = 'outgoing', r.tx_day = tx_day, r.spoke_count = spoke_count,
          r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
    {_CLOSE} IN TRANSACTIONS OF 1000 ROWS
    """

def get_hub_and_spoke_in_query(label, scope_clause_t, trusted_pair_clause, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} WITH DISTINCT seed.LOGICAL_BENACCOUNTNO AS hub, seed.TRANSACTIONDATE AS tx_day WHERE hub IS NOT NULL AND hub <> '' AND tx_day IS NOT NULL AND tx_day <> '' AND NOT hub IN $pt"
        match_filters = "AND t.LOGICAL_BENACCOUNTNO = hub AND t.TRANSACTIONDATE = tx_day"
        
    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
      {match_filters if incremental_batch_id else "AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> ''"}
    WITH {"hub, tx_day," if incremental_batch_id else "t.LOGICAL_BENACCOUNTNO AS hub, t.TRANSACTIONDATE AS tx_day,"} collect(t) AS txns, count(DISTINCT t.LOGICAL_ACCOUNTNO) AS spoke_count
    WHERE {"spoke_count >= $hub_spoke_min_counterparties" if incremental_batch_id else "hub IS NOT NULL AND hub <> '' AND NOT hub IN $pt AND spoke_count >= $hub_spoke_min_counterparties"} AND size(txns) < 1000
    CALL (txns, hub, tx_day, spoke_count) {_OPEN}
      UNWIND range(0, size(txns)-2) AS i
      WITH txns[i] AS a, txns[i+1] AS b, hub, tx_day, spoke_count
      WHERE {trusted_pair_clause}
      MERGE (a)-[r:HUB_AND_SPOKE {_SID}]->(b)
      SET r.is_evidence = true, r.anomaly_score = 0.5, r.bgcolor = '#6f42c1', r.textcolor = '#eeeeee', r.provisional = {prov_str},
          r.reason = 'account connects with multiple counterparties on same day',
          r.hub_account = hub, r.direction = 'incoming', r.tx_day = tx_day, r.spoke_count = spoke_count,
          r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
    {_CLOSE} IN TRANSACTIONS OF 1000 ROWS
    """

def get_shared_identifier_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"""
        MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} 
        WITH [{_CK_BIZMOB_SEED}, {_CK_BENTEL_SEED}] AS identifiers 
        UNWIND identifiers AS seed_identifier
        WITH DISTINCT seed_identifier.kind AS identifier_type, trim(toString(seed_identifier.value)) AS identifier_value
        WHERE identifier_value <> ''
        """
        match_filters = "AND (t.BUSINESSMOBILENO = identifier_value OR t.BENTELNO = identifier_value)"

    with_identifiers = "identifier_type, identifier_value" if incremental_batch_id else "[{kind:'BUSINESSMOBILENO', value:t.BUSINESSMOBILENO, account:t.LOGICAL_ACCOUNTNO}, {kind:'BENTELNO', value:t.BENTELNO, account:t.LOGICAL_BENACCOUNTNO}] AS identifiers"
    with_account = "WITH identifier_type, identifier_value, CASE WHEN t.BUSINESSMOBILENO = identifier_value THEN t.LOGICAL_ACCOUNTNO ELSE t.LOGICAL_BENACCOUNTNO END AS account, t" if incremental_batch_id else "UNWIND identifiers AS identifier WITH identifier.kind AS identifier_type, trim(toString(identifier.value)) AS identifier_value, identifier.account AS account, t"

    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {match_filters}
    WITH t, {with_identifiers}
    {with_account}
    WHERE identifier_value <> '' AND account IS NOT NULL AND account <> ''
    WITH identifier_type, identifier_value, collect(DISTINCT account) AS accounts, collect(DISTINCT t) AS txns
    WHERE size(accounts) >= 2 AND size(txns) < 1000
    CALL (txns, identifier_type, identifier_value, accounts) {_OPEN}
      UNWIND range(0, size(txns)-2) AS i
      WITH txns[i] AS a, txns[i+1] AS b, identifier_type, identifier_value, accounts
      MERGE (a)-[r:SHARED_IDENTIFIER {_SID}]->(b)
      SET r.is_evidence = true, r.anomaly_score = 0.8, r.bgcolor = '#0d898a', r.textcolor = '#eeeeee', r.provisional = {prov_str},
          r.reason = 'same identifier appears on multiple accounts',
          r.identifier_type = identifier_type, r.identifier_value = identifier_value,
          r.account_count = size(accounts),
          r.edge_semantic = 'GROUPING', r.financial_flow = false, r.directed_display = false
    {_CLOSE} IN TRANSACTIONS OF 1000 ROWS
    """

def get_rapid_withdrawal_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} WITH DISTINCT seed.LOGICAL_ACCOUNTNO AS acc, seed.TRANSACTIONDATE AS tx_day WHERE acc IS NOT NULL AND acc <> '' AND tx_day IS NOT NULL AND tx_day <> '' AND NOT acc IN $pt"
        match_filters = "AND (t.LOGICAL_ACCOUNTNO = acc OR t.LOGICAL_BENACCOUNTNO = acc) AND t.TRANSACTIONDATE = tx_day"

    where_filter = match_filters if incremental_batch_id else "AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> '' AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> '' AND NOT t.LOGICAL_ACCOUNTNO IN $pt"
    with_acc = "acc, tx_day," if incremental_batch_id else "t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day,"

    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {where_filter}
    WITH {with_acc} count(t) AS cnt
    WHERE cnt >= 1
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND (t.LOGICAL_ACCOUNTNO = acc OR t.LOGICAL_BENACCOUNTNO = acc)
      AND t.TRANSACTIONDATE = tx_day
    WITH acc, tx_day, collect(t) AS txns
    WITH acc, tx_day, [x IN txns WHERE x.LOGICAL_BENACCOUNTNO = acc] AS in_txns, [x IN txns WHERE x.LOGICAL_ACCOUNTNO = acc] AS out_txns
    WHERE size(in_txns) > 0 AND size(out_txns) > 0 AND (size(in_txns) + size(out_txns)) < 1000
    CALL (in_txns, out_txns, acc, tx_day) {_OPEN}
      UNWIND in_txns AS t1
      UNWIND out_txns AS t2
      WITH t1, t2, acc, tx_day
      WHERE coalesce(t1.TRANSACTIONTIME, '') <= coalesce(t2.TRANSACTIONTIME, '')
      WITH t1, t2, acc, tx_day,
           coalesce(toFloat(t1.AMOUNTINBIRR), toFloat(t1.AMOUNT), toFloat(t1.LOCAL_AMOUNT), 0.0) AS in_amt,
           coalesce(toFloat(t2.AMOUNTINBIRR), toFloat(t2.AMOUNT), toFloat(t2.LOCAL_AMOUNT), 0.0) AS out_amt
      WHERE in_amt > 0 AND out_amt > 0
        AND abs(in_amt - out_amt) <= (in_amt * $rapid_withdrawal_amount_tolerance)
      MERGE (t1)-[r:RAPID_WITHDRAWAL {_SID}]->(t2)
      SET r.is_evidence = true, r.anomaly_score = 0.4, r.bgcolor = '#e07624', r.textcolor = '#eeeeee', r.provisional = {prov_str},
          r.reason = 'funds rapidly withdrawn or passed through on same day',
          r.in_amount = in_amt, r.out_amount = out_amt,
          r.edge_semantic = 'OBSERVED_FLOW', r.financial_flow = true, r.directed_display = true
    {_CLOSE} IN TRANSACTIONS OF 1000 ROWS
    """

def get_account_activity_spike_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = ""
    match_filters = ""
    if incremental_batch_id:
        seed_block = f"MATCH (seed:{label}) WHERE seed.batch_id = {incremental_batch_id} WITH DISTINCT seed.LOGICAL_ACCOUNTNO AS acc, seed.TRANSACTIONDATE AS tx_day WHERE acc IS NOT NULL AND acc <> '' AND tx_day IS NOT NULL AND tx_day <> '' AND NOT acc IN $pt"
        match_filters = "AND t.LOGICAL_ACCOUNTNO = acc AND t.TRANSACTIONDATE = tx_day"
        
    where_filter = match_filters if incremental_batch_id else "AND t.TRANSACTIONDATE IS NOT NULL AND t.TRANSACTIONDATE <> '' AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> '' AND NOT t.LOGICAL_ACCOUNTNO IN $pt"
    with_acc = "acc, tx_day," if incremental_batch_id else "t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day,"

    return f"""
    {seed_block}
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {where_filter}
    WITH {with_acc} count(t) AS daily_count, collect(t) AS txns
    WHERE daily_count >= $activity_spike_min_daily_count AND daily_count < 2000
    WITH acc, tx_day, daily_count, txns
    MATCH (history:{label} {_CK_LOGACC})
    WHERE coalesce(history.IGNORE_LOGICAL, false) = false
      AND history.TRANSACTIONDATE IS NOT NULL AND history.TRANSACTIONDATE <> tx_day
      AND history.TRANSACTIONDATE >= toString(date(tx_day) - duration({_CK_DAYS}))
    WITH acc, tx_day, daily_count, txns, count(history) AS hist_count, count(DISTINCT history.TRANSACTIONDATE) AS hist_days
    WHERE hist_days > 0
    WITH acc, tx_day, daily_count, txns, (toFloat(hist_count) / hist_days) AS avg_daily
    WHERE daily_count > (avg_daily * $activity_spike_multiplier)
    CALL (txns, daily_count, avg_daily) {_OPEN}
      UNWIND txns AS t
      MERGE (t)-[r:ACCOUNT_ACTIVITY_SPIKE {_SID}]->(t)
      SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#99153c', r.textcolor = '#eeeeee', r.provisional = {prov_str},
          r.reason = 'unusually high transaction volume for this account on this day',
          r.daily_count = daily_count, r.avg_daily = avg_daily,
          r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
    {_CLOSE} IN TRANSACTIONS OF 1000 ROWS
    """

def get_high_risk_link_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_block = f"MATCH (t:{label}) WHERE t.batch_id = {incremental_batch_id} " if incremental_batch_id else f"MATCH (t:{label}) WHERE ({scope_clause_t}) "
    
    return f"""
    {seed_block}
      AND coalesce(t.IGNORE_LOGICAL, false) = false
    UNWIND $risk_entries AS risk_entity
    WITH t, risk_entity
    WHERE
       (risk_entity.account IS NOT NULL AND risk_entity.account <> '' AND (t.LOGICAL_ACCOUNTNO = risk_entity.account OR t.LOGICAL_BENACCOUNTNO = risk_entity.account)) OR
       (risk_entity.phone IS NOT NULL AND risk_entity.phone <> '' AND (t.BUSINESSMOBILENO = risk_entity.phone OR t.BENTELNO = risk_entity.phone)) OR
       (risk_entity.name IS NOT NULL AND risk_entity.name <> '' AND (toLower(t.SENDER_FULL_NAME) = toLower(risk_entity.name) OR toLower(t.RECEIVER_FULL_NAME) = toLower(risk_entity.name)))
    WITH t, collect(DISTINCT toUpper(risk_entity.category)) AS matched_categories
    WHERE size(matched_categories) > 0
    CALL (t, matched_categories) {_OPEN}
      UNWIND matched_categories AS cat
      FOREACH (ignore IN CASE WHEN NOT cat IN ['PEP', 'SANCTION', 'SANCTIONS', 'SANCTIONED'] THEN [1] ELSE [] END |
          MERGE (t)-[r:HIGH_RISK_LINK {_SID}]->(t)
          SET r.is_evidence = true, r.anomaly_score = 0.7, r.bgcolor = '#de7d07', r.provisional = {prov_str}, r.reason = 'Configured risk entity matched', r.risk_source = 'risk_entities', r.category = cat,
              r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
      )
    {_CLOSE} IN TRANSACTIONS OF 1000 ROWS
    """




def get_late_night_tx_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_filter = f"AND t.batch_id = {incremental_batch_id}" if incremental_batch_id else ""
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {seed_filter}
      AND t.TRANSACTIONTIME IS NOT NULL
      AND toString(t.TRANSACTIONTIME) <> ''
      AND {_trusted_node_clause('t')}
    WITH t, toInteger(substring(replace(toString(t.TRANSACTIONTIME), ':', ''), 0, 4)) AS t_time
    WHERE t_time >= $late_night_start OR t_time <= $late_night_end
    MERGE (t)-[r:LATE_NIGHT_TX {_SID}]->(t)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#00c1a2',
        r.provisional = {prov_str},
        r.reason = 'transaction occurred outside typical business hours',
        r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
    """

def get_just_below_threshold_query(label, scope_clause_t, is_provisional=False, incremental_batch_id=None):
    prov_str = "true" if is_provisional else "false"
    seed_filter = f"AND t.batch_id = {incremental_batch_id}" if incremental_batch_id else ""
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      {seed_filter}
      AND coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) > 0
      AND {_trusted_node_clause('t')}
    WITH t, coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0) AS amt
    WHERE amt >= ($single_tx_threshold * 0.9) AND amt < $single_tx_threshold
    MERGE (t)-[r:JUST_BELOW_THRESHOLD {_SID}]->(t)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#dba124',
        r.provisional = {prov_str},
        r.reason = 'transaction amount is suspiciously close to reporting threshold',
        r.amount = amt,
        r.threshold = $single_tx_threshold,
        r.edge_semantic = 'NODE_FLAG', r.financial_flow = false, r.directed_display = false
    """

def _create_transaction_indexes(session, label):
    index_prefix = _safe_index_name(label)
    safe_label = _safe_label(label)
    for prop in ["batch_id", "session_id", "ACCOUNTNO", "BENACCOUNTNO", "LOGICAL_ACCOUNTNO", "LOGICAL_BENACCOUNTNO", "TRANSACTIONDATE", "BUSINESSMOBILENO", "BENTELNO"]:
        index_name = _safe_index_name("idx", index_prefix, prop)
        session.run(f"CREATE INDEX {index_name} IF NOT EXISTS FOR (n:{safe_label}) ON (n.{prop})")


def _clear_transaction_relationships(session, session_id):
    session.run("""
    MATCH ()-[r]->()
    WHERE r.session_id = $session_id
      AND type(r) IN $relationship_types
    DELETE r
    """, session_id=str(session_id), relationship_types=TRANSACTION_RELATIONSHIPS)


def _count_transaction_relationships(session, session_id):
    result = session.run("""
    MATCH ()-[r]->()
    WHERE r.session_id = $session_id
      AND type(r) IN $relationship_types
    RETURN type(r) AS relationship_type, count(r) AS count
    """, session_id=str(session_id), relationship_types=TRANSACTION_RELATIONSHIPS)
    return {record["relationship_type"]: record["count"] for record in result}




def execute_effective_flow_rule(session, label, scope_clause_t, session_id, pass_through_accounts):
    """
    Python-accelerated central implementation of EFFECTIVE_FLOW.
    This prevents the Cartesian explosion that occurs in pure Cypher when pass-through nodes are extremely dense.
    """
    if not pass_through_accounts:
        return 0

    # 1. Fetch inbound
    inbound_res = session.run(
        f"MATCH (n:{label}) WHERE ({scope_clause_t}) AND n.BENACCOUNTNO IN $pt AND n.ACCOUNTNO IS NOT NULL AND n.ACCOUNTNO <> '' "
        f"RETURN elementId(n) AS id, n.ACCOUNTNO AS acc, n.BENACCOUNTNO AS ben, n.TRANSACTIONDATE AS date, n.TRANSACTIONTIME AS time, "
        f"coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt",
        session_id=session_id, pt=pass_through_accounts
    )
    inbounds = [dict(r) for r in inbound_res]
    
    # 2. Fetch outbound
    outbound_res = session.run(
        f"MATCH (n:{label}) WHERE ({scope_clause_t}) AND n.ACCOUNTNO IN $pt AND n.BENACCOUNTNO IS NOT NULL AND n.BENACCOUNTNO <> '' "
        f"RETURN elementId(n) AS id, n.ACCOUNTNO AS acc, n.BENACCOUNTNO AS ben, n.TRANSACTIONDATE AS date, n.TRANSACTIONTIME AS time, "
        f"coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt",
        session_id=session_id, pt=pass_through_accounts
    )
    outbounds = [dict(r) for r in outbound_res]
    
    from collections import defaultdict
    out_map = defaultdict(list)
    for o in outbounds:
        out_map[(o["acc"], o["date"])].append(o)
        
    for k in out_map:
        out_map[k].sort(key=lambda x: str(x["time"]))
        
    edges_to_create = []
    for i in inbounds:
        candidates = out_map.get((i["ben"], i["date"]), [])
        for o in candidates:
            if o["ben"] != i["acc"] and str(o["time"]) >= str(i["time"]):
                if i["amt"] > 0 and o["amt"] > 0 and abs(o["amt"] - i["amt"]) <= (i["amt"] * 0.1):
                    edges_to_create.append({
                        "in_id": i["id"],
                        "out_id": o["id"],
                        "intermediary": i["ben"],
                        "in_amt": i["amt"],
                        "out_amt": o["amt"],
                        "fee": i["amt"] - o["amt"],
                        "sender": i["acc"],
                        "receiver": o["ben"],
                        "date": i["date"]
                    })
                    break  # Prevent cartesian multiplication per inbound row
                    
    if edges_to_create:
        session.run(
            f"UNWIND $edges AS e "
            f"MATCH (inbound) WHERE elementId(inbound) = e.in_id "
            f"MATCH (outbound) WHERE elementId(outbound) = e.out_id "
            f"MERGE (inbound)-[r:EFFECTIVE_FLOW {_SID}]->(outbound) "
            f"SET r.intermediary = e.intermediary, r.hop_count = 2, r.in_amount = e.in_amt, r.out_amount = e.out_amt, "
            f"r.fee_delta = e.fee, r.effective_sender = e.sender, r.effective_receiver = e.receiver, r.tx_date = e.date, "
            f"r.bgcolor = '#9b59b6', r.textcolor = '#eeeeee', r.provisional = false, r.edge_semantic = 'EFFECTIVE_FLOW', "
            f"r.financial_flow = true, r.directed_display = true",
            session_id=session_id, edges=edges_to_create
        )
    return len(edges_to_create)


def execute_circular_flow_rule(
    session, label, scope_clause, session_id,
    pass_through_accounts=None, trusted_entries=None,
    thresholds=None, is_provisional=False,
    incremental_batch_id=None, boundary_batch_id=None,
):
    """
    Python-accelerated central implementation of CIRCULAR_FLOW.

    Detects same-day A→B / B→A round-trip transfers with amount matching
    within a configurable tolerance.  Instead of running an expensive Cypher
    self-join across hundreds of thousands of nodes, we:

      1. Fetch the relevant LOGICAL transaction data into Python memory.
      2. Build an O(1) hash-set keyed by (sender, receiver, date).
      3. For each transaction, check if a reverse-direction counterpart
         exists on the same day.
      4. Validate amounts within the configured tolerance.
      5. Write *only* the verified anomaly edges back to Neo4j.

    All thresholds are driven from the database-backed ``global_rule_thresholds``
    configuration (``circular_flow_amount_tolerance``).  Trusted-entity
    filtering is performed in Python *before* edge creation.

    Parameters
    ----------
    session : neo4j.Session
        Active Neo4j driver session.
    label : str
        Safe-escaped node label (e.g. 'bank_transactions_xvigilance-daemon').
    scope_clause : str
        Cypher WHERE fragment that scopes nodes (session_id / batch_id).
    session_id : str
        The session_id value to bind as ``$session_id`` in the scope clause.
    pass_through_accounts : list[str] | None
        Account numbers to exclude (pass-through intermediaries).
    trusted_entries : list[dict] | None
        Trusted entity entries from global_entity_classification.
    thresholds : dict | None
        Rule thresholds from ``fetch_rule_thresholds()``.
    is_provisional : bool
        Whether edges should be marked provisional (incremental batches).
    incremental_batch_id : str | None
        When set, only seed from nodes in this batch (incremental mode).
    boundary_batch_id : str | None
        When set, at least one side of the pair must belong to this batch.
    """
    if thresholds is None:
        thresholds = {}
    if pass_through_accounts is None:
        pass_through_accounts = []
    if trusted_entries is None:
        trusted_entries = []

    prov_str = "true" if is_provisional else "false"
    pt_set = set(str(a).strip() for a in pass_through_accounts if a)

    # --- Configuration-driven tolerance from database ---
    amount_tolerance = float(thresholds.get("circular_flow_amount_tolerance", 0.05))

    # ---- 1. Fetch transaction data into Python memory ----
    fetch_query = (
        f"MATCH (n:{label}) WHERE ({scope_clause}) "
        f"AND coalesce(n.IGNORE_LOGICAL, false) = false "
        f"AND n.LOGICAL_ACCOUNTNO IS NOT NULL AND n.LOGICAL_ACCOUNTNO <> '' "
        f"AND n.LOGICAL_BENACCOUNTNO IS NOT NULL AND n.LOGICAL_BENACCOUNTNO <> '' "
        f"AND n.TRANSACTIONDATE IS NOT NULL AND n.TRANSACTIONDATE <> '' "
        f"AND n.LOGICAL_ACCOUNTNO <> n.LOGICAL_BENACCOUNTNO "
        f"RETURN elementId(n) AS id, "
        f"n.LOGICAL_ACCOUNTNO AS acc, n.LOGICAL_BENACCOUNTNO AS ben, "
        f"n.TRANSACTIONDATE AS date, "
        f"coalesce(n.TRANSACTIONTIME, '') AS time, "
        f"coalesce(toFloat(n.AMOUNTINBIRR), toFloat(n.AMOUNT), toFloat(n.amount), toFloat(n.LOCAL_AMOUNT), 0.0) AS amt, "
        f"coalesce(n.PASSTHROUGH_HOPS, 0) AS pt_hops, "
        f"coalesce(n.LOGICAL_PATH, '') AS logical_path, "
        f"coalesce(n.LOGICAL_TRANSFORMATION_REASON, 'NONE') AS logical_transform, "
        f"coalesce(n.batch_id, '') AS batch_id"
    )
    params = {"session_id": session_id}
    if incremental_batch_id:
        params["incremental_batch_id"] = incremental_batch_id
    if boundary_batch_id:
        params["boundary_batch_id"] = boundary_batch_id

    result = session.run(fetch_query, **params)
    rows = [dict(r) for r in result]

    if not rows:
        return 0

    # ---- 2. Build trusted-entity lookup for Python-side filtering ----
    # Pre-compute which fields the trusted entries care about for fast checking
    def _is_trusted(node_dict):
        """Check if a node matches any trusted entity entry (Python equivalent
        of the Cypher _trusted_entry_match)."""
        if not trusted_entries:
            return False
        for entry in trusted_entries:
            match = True
            for k, v in entry.items():
                if k.lower() in ('category', 'type', 'reason', 'pass_through',
                                 'passthrough', 'classification', 'notes'):
                    continue
                node_val = str(node_dict.get(k, ""))
                if node_val != str(v):
                    match = False
                    break
            if match:
                return True
        return False

    # ---- 3. Index rows by (sender, receiver, date) for O(1) lookup ----
    from collections import defaultdict

    # key = (LOGICAL_ACCOUNTNO, LOGICAL_BENACCOUNTNO, TRANSACTIONDATE)
    forward_map = defaultdict(list)
    for row in rows:
        acc = str(row["acc"]).strip()
        ben = str(row["ben"]).strip()
        date = str(row["date"]).strip()
        # Skip pass-through accounts
        if acc in pt_set or ben in pt_set:
            continue
        forward_map[(acc, ben, date)].append(row)

    # ---- 4. Detect circular pairs via reverse-key hash lookup ----
    seen_pairs = set()  # Canonical pair dedup: frozenset({id_a, id_b})
    edges_to_create = []

    for (sender, receiver, date), a_rows in forward_map.items():
        # Look up the reverse direction: receiver→sender on the same date
        reverse_key = (receiver, sender, date)
        b_rows = forward_map.get(reverse_key)
        if not b_rows:
            continue

        for a in a_rows:
            amt_a = a["amt"]
            if amt_a is None or amt_a <= 0:
                continue

            for b in b_rows:
                # Deduplication: each pair only once
                pair_key = frozenset({a["id"], b["id"]})
                if pair_key in seen_pairs:
                    continue
                if a["id"] == b["id"]:
                    continue

                amt_b = b["amt"]
                if amt_b is None or amt_b <= 0:
                    continue

                # Amount tolerance check (configuration-driven)
                min_amt = min(amt_a, amt_b)
                if abs(amt_a - amt_b) > (min_amt * amount_tolerance):
                    continue

                # Trusted entity filter (Python-side)
                a_node = {"LOGICAL_ACCOUNTNO": sender, "LOGICAL_BENACCOUNTNO": receiver,
                          "ACCOUNTNO": sender, "BENACCOUNTNO": receiver}
                b_node = {"LOGICAL_ACCOUNTNO": receiver, "LOGICAL_BENACCOUNTNO": sender,
                          "ACCOUNTNO": receiver, "BENACCOUNTNO": sender}
                if _is_trusted(a_node) or _is_trusted(b_node):
                    continue

                # Boundary filter for incremental mode
                if boundary_batch_id:
                    if a["batch_id"] != boundary_batch_id and b["batch_id"] != boundary_batch_id:
                        continue

                seen_pairs.add(pair_key)

                # Determine pass-through context for the reason field
                has_passthrough = (a["pt_hops"] > 0) or (b["pt_hops"] > 0)
                reason = ('logical same-day reverse flow with pass-through intermediary'
                          if has_passthrough else 'same-day direct reverse transfer')

                edges_to_create.append({
                    "id_a": a["id"],
                    "id_b": b["id"],
                    "sender_a": sender,
                    "receiver_a": receiver,
                    "amt_a": amt_a,
                    "amt_b": amt_b,
                    "reason": reason,
                    "pt_hops_a": a["pt_hops"],
                    "pt_hops_b": b["pt_hops"],
                    "logical_path_a": a["logical_path"],
                    "logical_path_b": b["logical_path"],
                    "logical_transform_a": a["logical_transform"],
                    "logical_transform_b": b["logical_transform"],
                })

    # ---- 5. Write only the verified anomaly edges back to Neo4j ----
    if edges_to_create:
        session.run(
            f"UNWIND $edges AS e "
            f"MATCH (a) WHERE elementId(a) = e.id_a "
            f"MATCH (b) WHERE elementId(b) = e.id_b "
            # Forward edge: a → b
            f"MERGE (a)-[r1:CIRCULAR_FLOW {_SID}]->(b) "
            f"SET r1.is_evidence = true, r1.anomaly_score = 0.6, "
            f"r1.bgcolor = '#e6e6e6', r1.provisional = {prov_str}, "
            f"r1.reason = e.reason, "
            f"r1.logical_sender = e.sender_a, r1.logical_receiver = e.receiver_a, "
            f"r1.reverse_sender = e.receiver_a, r1.reverse_receiver = e.sender_a, "
            f"r1.amount_a = e.amt_a, r1.amount_b = e.amt_b, "
            f"r1.passthrough_a = e.pt_hops_a, r1.passthrough_b = e.pt_hops_b, "
            f"r1.logical_path_a = e.logical_path_a, r1.logical_path_b = e.logical_path_b, "
            f"r1.logical_transformation_a = e.logical_transform_a, r1.logical_transformation_b = e.logical_transform_b, "
            f"r1.edge_semantic = 'DERIVED_LOGICAL_FLOW', r1.financial_flow = true, r1.directed_display = true "
            # Reverse edge: b → a
            f"MERGE (b)-[r2:CIRCULAR_FLOW {_SID}]->(a) "
            f"SET r2.is_evidence = true, r2.anomaly_score = 0.6, "
            f"r2.bgcolor = '#e6e6e6', r2.provisional = {prov_str}, "
            f"r2.reason = e.reason, "
            f"r2.logical_sender = e.receiver_a, r2.logical_receiver = e.sender_a, "
            f"r2.reverse_sender = e.sender_a, r2.reverse_receiver = e.receiver_a, "
            f"r2.amount_a = e.amt_b, r2.amount_b = e.amt_a, "
            f"r2.passthrough_a = e.pt_hops_b, r2.passthrough_b = e.pt_hops_a, "
            f"r2.logical_path_a = e.logical_path_b, r2.logical_path_b = e.logical_path_a, "
            f"r2.logical_transformation_a = e.logical_transform_b, r2.logical_transformation_b = e.logical_transform_a, "
            f"r2.edge_semantic = 'DERIVED_LOGICAL_FLOW', r2.financial_flow = true, r2.directed_display = true",
            session_id=session_id, edges=edges_to_create,
        )

    return len(edges_to_create)


def get_logical_layer_query(label, scope_clause_t, apply_pass_through=False):
    q1 = f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
    SET t.LOGICAL_ACCOUNTNO = coalesce(t.ACCOUNTNO, ''),
        t.LOGICAL_BENACCOUNTNO = coalesce(t.BENACCOUNTNO, ''),
        t.RAW_SENDER = coalesce(t.ACCOUNTNO, ''),
        t.RAW_RECEIVER = coalesce(t.BENACCOUNTNO, ''),
        t.LOGICAL_TRANSFORMATION_REASON = 'NONE',
        t.PASSTHROUGH_HOPS = 0,
        t.IGNORE_LOGICAL = false
    """
    
    q2 = f"""
    MATCH (inbound:{label})-[r:EFFECTIVE_FLOW]->(outbound:{label})
    WHERE ({scope_clause_t.replace('t.', 'inbound.')})
    SET inbound.LOGICAL_BENACCOUNTNO = coalesce(outbound.BENACCOUNTNO, ''),
        outbound.IGNORE_LOGICAL = true,
        inbound.PASSTHROUGH_HOPS = 1,
        inbound.LOGICAL_TRANSFORMATION_REASON = 'EFFECTIVE_FLOW_COLLAPSE',
        inbound.LOGICAL_PATH = '[' + coalesce(inbound.ACCOUNTNO, '') + ', ' + coalesce(r.intermediary, '') + ', ' + coalesce(outbound.BENACCOUNTNO, '') + ']'
    
    MERGE (inbound)-[df:DERIVED_FLOW {_SID}]->(outbound)
    SET df.edge_semantic = 'DERIVED_EFFECTIVE_FLOW',
        df.raw_sender = coalesce(inbound.ACCOUNTNO, ''),
        df.logical_sender = coalesce(inbound.ACCOUNTNO, ''),
        df.raw_receiver = coalesce(outbound.BENACCOUNTNO, ''),
        df.passthrough_entity = coalesce(r.intermediary, ''),
        df.passthrough_hops = 1,
        df.financial_flow = false,
        df.bgcolor = '#3498db',
        df.directed_display = true
    """
    
    return [q1, q2] if apply_pass_through else [q1]

def batch_graph_analysis_transactions(
    driver,
    log_file,
    session_id=None,
    nodes_label="Transactions",
    high_risk_accounts=None,
    threshold_multiplier=3,
    single_tx_threshold=10000,
    total_threshold=30000,
    min_tx_count=3,
    trusted_entities=None,
    risk_entities=None,
):
    if high_risk_accounts is None:
        high_risk_accounts = []

    log_writer(log_file, f"[{datetime.now()}] [Info] Starting transactions analysis")
    label = _safe_label(nodes_label)
    thresholds = fetch_rule_thresholds()
    log_writer(log_file, f"[{datetime.now()}] [Info] Rule thresholds loaded: late_night_start={thresholds.get('late_night_start')}, late_night_end={thresholds.get('late_night_end')}, hub_spoke_min={thresholds.get('hub_spoke_min_counterparties')}")
    session_param = str(session_id) if session_id else ""
    trusted_entries = trusted_entities_cypher_entries(trusted_entities)
    risk_entries = risk_entities_cypher_entries(risk_entities)
    pass_through_accounts = _extract_pass_through_accounts(trusted_entities)

    with driver.session() as session:
        _create_transaction_indexes(session, nodes_label)
        if session_id:
            _clear_transaction_relationships(session, session_id)

        # ----------------------------
        # 0. EFFECTIVE_FLOW
        # ----------------------------
        if pass_through_accounts:
            try:
                log_writer(log_file, f"[{datetime.now()}] [Info] Starting EFFECTIVE_FLOW rule (Python accelerated)")
                edge_count = execute_effective_flow_rule(session, label, f"$session_id IS NULL OR {_session_scope_clause('n')}", session_param, pass_through_accounts)
                log_writer(log_file, f"[{datetime.now()}] [Info] EFFECTIVE_FLOW rule completed. Created {edge_count} edges.")
            except Exception as e:
                log_writer(log_file, f"[{datetime.now()}] [Error] EFFECTIVE_FLOW rule failed: {e}")

        # ----------------------------
        # 0.5 LOGICAL TRANSACTION LAYER
        # ----------------------------
        try:
            queries = get_logical_layer_query(label, f"$session_id IS NULL OR {_session_scope_clause('t')}", apply_pass_through=bool(pass_through_accounts))
            for q in queries:
                session.run(q, session_id=session_param)
            log_writer(log_file, f"[{datetime.now()}] [Info] Logical Layer initialized")
        except Exception as e:
            log_writer(log_file, f"[{datetime.now()}] [Error] Logical Layer failed: {e}")

        # ----------------------------
        # 1. SMURFING: repeated small transfers from one account to one beneficiary
        # ----------------------------
        query = get_smurfing_query(
            label=label,
            scope_clause_t="$session_id IS NULL OR t.session_id = $session_id OR t.batch_id STARTS WITH $session_id",
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=False
        )
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts,
             smurfing_single_tx_threshold=thresholds.get("smurfing_single_tx_threshold", 300000),
             smurfing_cumulative_threshold=thresholds.get("smurfing_cumulative_threshold", 900000),
             smurfing_min_tx_count=thresholds.get("smurfing_min_tx_count", 3))

        # ----------------------------
        # 2. CIRCULAR_FLOW (Python accelerated: in-memory hash matching)
        # ----------------------------
        try:
            edge_count = execute_circular_flow_rule(
                session=session,
                label=label,
                scope_clause="$session_id IS NULL OR n.session_id = $session_id OR n.batch_id STARTS WITH $session_id",
                session_id=session_param,
                pass_through_accounts=pass_through_accounts,
                trusted_entries=trusted_entries,
                thresholds=thresholds,
                is_provisional=False,
            )
            log_writer(log_file, f"[{datetime.now()}] [Info] CIRCULAR_FLOW rule completed (Python accelerated: {edge_count} pairs).")
        except Exception as e:
            log_writer(log_file, f"[{datetime.now()}] [Error] CIRCULAR_FLOW rule failed: {e}")

        # ----------------------------
        # 3. FUND_FLOW: beneficiary becomes sender in a later transaction
        # ----------------------------
        query = get_fund_flow_query(
            label=label,
            scope_clause_t="$session_id IS NULL OR t.session_id = $session_id OR t.batch_id STARTS WITH $session_id",
            scope_clause_a="$session_id IS NULL OR a.session_id = $session_id OR a.batch_id STARTS WITH $session_id",
            scope_clause_b="$session_id IS NULL OR b.session_id = $session_id OR b.batch_id STARTS WITH $session_id",
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=False
        )
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 4. DORMANT_TO_ACTIVE
        # ----------------------------
        scope_full = "$session_id IS NULL OR t.session_id = $session_id OR t.batch_id STARTS WITH $session_id"
        query = get_dormant_to_active_query(label=label, scope_clause_t=scope_full, is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 5. ABNORMAL_BALANCE_CHANGE
        # ----------------------------
        query = get_abnormal_balance_query(label=label, scope_clause_t=scope_full, is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, historical_baseline_days=30)

        # ----------------------------
        # 6. HUB_AND_SPOKE (outgoing)
        # ----------------------------
        query = get_hub_and_spoke_out_query(label=label, scope_clause_t=scope_full, trusted_pair_clause=_trusted_pair_clause('a', 'b'), is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties", 3))

        # ----------------------------
        # 7. HUB_AND_SPOKE (incoming)
        # ----------------------------
        query = get_hub_and_spoke_in_query(label=label, scope_clause_t=scope_full, trusted_pair_clause=_trusted_pair_clause('a', 'b'), is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties", 3))

        # ----------------------------
        # 8. SHARED_IDENTIFIER
        # ----------------------------
        query = get_shared_identifier_query(label=label, scope_clause_t=scope_full, is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # ----------------------------
        # 9. LATE_NIGHT_TX
        # ----------------------------
        query = get_late_night_tx_query(label=label, scope_clause_t=scope_full, is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, late_night_start=thresholds.get("late_night_start", 2300), late_night_end=thresholds.get("late_night_end", 400))

        # ----------------------------
        # 10. JUST_BELOW_THRESHOLD
        # ----------------------------
        query = get_just_below_threshold_query(label=label, scope_clause_t=scope_full, is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, single_tx_threshold=thresholds.get("reporting_threshold", 300000))

        # ----------------------------
        # 11. RAPID_WITHDRAWAL
        # ----------------------------
        query = get_rapid_withdrawal_query(label=label, scope_clause_t=scope_full, is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, rapid_withdrawal_amount_tolerance=thresholds.get("rapid_withdrawal_amount_tolerance", 0.1))

        # ----------------------------
        # 12. ACCOUNT_ACTIVITY_SPIKE
        # ----------------------------
        query = get_account_activity_spike_query(label=label, scope_clause_t=scope_full, is_provisional=False)
        session.run(query, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, activity_spike_min_daily_count=thresholds.get("activity_spike_min_daily_count", 10), activity_spike_multiplier=thresholds.get("activity_spike_multiplier", 3), historical_baseline_days=30)

        # ----------------------------
        # 13. HIGH_RISK_LINK
        # ----------------------------
        query = get_high_risk_link_query(label=label, scope_clause_t=scope_full, is_provisional=False)
        session.run(query, session_id=session_param, risk_entries=risk_entries)

        # ----------------------------
        # 14. FRAUD_AGGREGATOR
        # ----------------------------
        query = get_fraud_aggregator_query(label=label, scope_clause_t=scope_full, session_id=session_param)
        session.run(query, session_id=session_param)

        counts = _count_transaction_relationships(session, session_param) if session_param else {}
        _write_gds_metrics(session, f"{session_param}_transactions", nodes_label, session_param, TRANSACTION_RELATIONSHIPS, log_file)

    log_writer(log_file, f"[{datetime.now()}] [Success] Transactions analysis completed")
    return counts


def incremental_graph_analysis_transactions(
    driver,
    session_id,
    nodes_label,
    batch_id,
    log_file,
    high_risk_accounts=None,
    threshold_multiplier=3,
    single_tx_threshold=10000,
    total_threshold=30000,
    min_tx_count=3,
    trusted_entities=None,
    risk_entities=None,
):
    if high_risk_accounts is None:
        high_risk_accounts = []

    session_param = str(session_id)
    label = _safe_label(nodes_label)
    thresholds = fetch_rule_thresholds()
    trusted_entries = trusted_entities_cypher_entries(trusted_entities)
    risk_entries = risk_entities_cypher_entries(risk_entities)
    pass_through_accounts = _extract_pass_through_accounts(trusted_entities)
    log_writer(log_file, f"[{datetime.now()}] [Info] Running incremental transaction analysis for batch {batch_id}")

    with driver.session() as session:
        _create_transaction_indexes(session, nodes_label)

        # ----------------------------
        # 0. EFFECTIVE_FLOW (incremental)
        # ----------------------------
        if pass_through_accounts:
            try:
                log_writer(log_file, f"[{datetime.now()}] [Info] Starting EFFECTIVE_FLOW rule (Python accelerated incremental)")
                edge_count = execute_effective_flow_rule(session, label, "n.batch_id = $session_id", batch_id, pass_through_accounts)
                log_writer(log_file, f"[{datetime.now()}] [Info] EFFECTIVE_FLOW rule completed. Created {edge_count} edges.")
            except Exception as e:
                log_writer(log_file, f"[{datetime.now()}] [Error] EFFECTIVE_FLOW rule failed: {e}")

        # ----------------------------
        # 0.5 LOGICAL TRANSACTION LAYER
        # ----------------------------
        try:
            queries = get_logical_layer_query(label, "t.batch_id = $batch_id", apply_pass_through=bool(pass_through_accounts))
            for q in queries:
                session.run(q, session_id=session_param, batch_id=batch_id)
            log_writer(log_file, f"[{datetime.now()}] [Info] Logical Layer initialized")
        except Exception as e:
            log_writer(log_file, f"[{datetime.now()}] [Error] Logical Layer failed: {e}")

        # Smurfing: start from new rows, then inspect only matching account/beneficiary/day groups.
        query = get_smurfing_query(
            label=label,
            scope_clause_t=_session_scope_clause("t"),
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=True,
            incremental_batch_id="$batch_id"
        )
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts,
             smurfing_single_tx_threshold=thresholds.get("smurfing_single_tx_threshold", 300000),
             smurfing_cumulative_threshold=thresholds.get("smurfing_cumulative_threshold", 900000),
             smurfing_min_tx_count=thresholds.get("smurfing_min_tx_count", 3))

        # Circular flow: only pairs where the current batch is one side of the reversal.
        try:
            edge_count = execute_circular_flow_rule(
                session=session,
                label=label,
                scope_clause=_session_scope_clause("n"),
                session_id=session_param,
                pass_through_accounts=pass_through_accounts,
                trusted_entries=trusted_entries,
                thresholds=thresholds,
                is_provisional=True,
                boundary_batch_id=batch_id,
            )
            log_writer(log_file, f"[{datetime.now()}] [Info] CIRCULAR_FLOW incremental completed (Python accelerated: {edge_count} pairs).")
        except Exception as e:
            log_writer(log_file, f"[{datetime.now()}] [Error] CIRCULAR_FLOW incremental failed: {e}")

        # Fund flow: new nodes can either precede or complete a downstream flow.
        query = get_fund_flow_query(
            label=label,
            scope_clause_t=_session_scope_clause("t"),
            scope_clause_a=_session_scope_clause("a"),
            scope_clause_b=_session_scope_clause("b"),
            trusted_pair_clause=_trusted_pair_clause('a', 'b'),
            is_provisional=True,
            boundary_clause="(a.batch_id = $batch_id OR b.batch_id = $batch_id)"
        )
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # Cheap row-local flags: use central generators with incremental batch_id.
        scope_inc = _session_scope_clause("t")

        # 4. DORMANT_TO_ACTIVE
        query = get_dormant_to_active_query(label=label, scope_clause_t=scope_inc, is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # 5. ABNORMAL_BALANCE_CHANGE
        query = get_abnormal_balance_query(label=label, scope_clause_t=scope_inc, is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, historical_baseline_days=30, threshold=threshold_multiplier)

        # 6. HUB_AND_SPOKE (outgoing)
        query = get_hub_and_spoke_out_query(label=label, scope_clause_t=scope_inc, trusted_pair_clause=_trusted_pair_clause('a', 'b'), is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties", 3))

        # 7. HUB_AND_SPOKE (incoming)
        query = get_hub_and_spoke_in_query(label=label, scope_clause_t=scope_inc, trusted_pair_clause=_trusted_pair_clause('a', 'b'), is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties", 3))

        # 8. SHARED_IDENTIFIER
        query = get_shared_identifier_query(label=label, scope_clause_t=scope_inc, is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts)

        # 9. LATE_NIGHT_TX
        query = get_late_night_tx_query(label=label, scope_clause_t=scope_inc, is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, late_night_start=thresholds.get("late_night_start", 2300), late_night_end=thresholds.get("late_night_end", 400))

        # 10. JUST_BELOW_THRESHOLD
        query = get_just_below_threshold_query(label=label, scope_clause_t=scope_inc, is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, single_tx_threshold=thresholds.get("reporting_threshold", 300000))

        # 11. RAPID_WITHDRAWAL
        query = get_rapid_withdrawal_query(label=label, scope_clause_t=scope_inc, is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, rapid_withdrawal_amount_tolerance=thresholds.get("rapid_withdrawal_amount_tolerance", 0.1))

        # 12. ACCOUNT_ACTIVITY_SPIKE
        query = get_account_activity_spike_query(label=label, scope_clause_t=scope_inc, is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, trusted_entries=trusted_entries, pt=pass_through_accounts, activity_spike_min_daily_count=thresholds.get("activity_spike_min_daily_count", 10), activity_spike_multiplier=thresholds.get("activity_spike_multiplier", 3), historical_baseline_days=30)

        # 13. HIGH_RISK_LINK
        query = get_high_risk_link_query(label=label, scope_clause_t=scope_inc, is_provisional=True, incremental_batch_id="$batch_id")
        session.run(query, batch_id=batch_id, session_id=session_param, risk_entries=risk_entries)

        counts = _count_transaction_relationships(session, session_param)

    log_writer(log_file, f"[{datetime.now()}] [Info] Incremental analysis for batch {batch_id} flags: {counts}")
    return counts

# ====================================================
# Shared graph metrics
# ====================================================

def _cypher_string(value):
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def _write_gds_metrics(session, graph_name, label, session_id, relationship_types, log_file):
    if not session_id or not relationship_types:
        return

    escaped_session = _cypher_string(session_id)
    relationship_literal = "[" + ", ".join(f"'{_cypher_string(rel)}'" for rel in relationship_types) + "]"
    node_query = (
        "MATCH (n) "
        f"WHERE n.batch_id STARTS WITH '{escaped_session}' OR n.session_id = '{escaped_session}' "
        "RETURN id(n) AS id"
    )
    rel_query = (
        "MATCH (a)-[r]->(b) "
        f"WHERE r.session_id = '{escaped_session}' AND type(r) IN {relationship_literal} "
        "RETURN id(a) AS source, id(b) AS target"
    )

    try:
        log_writer(log_file, f"[{datetime.now()}] [Info] Starting GDS metrics for {graph_name}")
        session.run("CALL gds.graph.drop($graph_name, false) YIELD graphName RETURN graphName", graph_name=graph_name)
        session.run(
            """
            CALL gds.graph.project.cypher($graph_name, $node_query, $rel_query)
            YIELD graphName
            RETURN graphName
            """,
            graph_name=graph_name,
            node_query=node_query,
            rel_query=rel_query,
        )
        session.run("CALL gds.degree.write($graph_name, {writeProperty:'outDegree', orientation:'NATURAL'})", graph_name=graph_name)
        session.run("CALL gds.degree.write($graph_name, {writeProperty:'inDegree', orientation:'REVERSE'})", graph_name=graph_name)
        session.run("CALL gds.pageRank.write($graph_name, {writeProperty:'pagerank'})", graph_name=graph_name)
        session.run("CALL gds.betweenness.write($graph_name, {writeProperty:'betweenness'})", graph_name=graph_name)
        session.run("CALL gds.eigenvector.write($graph_name, {writeProperty:'eigenvector'})", graph_name=graph_name)
        session.run("CALL gds.wcc.write($graph_name, {writeProperty:'component_id'})", graph_name=graph_name)
        session.run("""
        MATCH (n)
        WHERE (n.batch_id STARTS WITH $session_id OR n.session_id = $session_id)
          AND (n.inDegree IS NOT NULL OR n.outDegree IS NOT NULL)
        SET n.degree = coalesce(n.inDegree, 0) + coalesce(n.outDegree, 0)
        """, session_id=session_id)
        log_writer(log_file, f"[{datetime.now()}] [Success] GDS metrics completed for {graph_name}")
    except Exception as exc:
        log_writer(log_file, f"[{datetime.now()}] [Warning] GDS metrics skipped for {graph_name}: {exc}")


# ====================================================
# Social Media Posts
# ====================================================

POST_RELATIONSHIPS = [
    "CREATED",
    "LOW_ENGAGEMENT",
    "INFLUENCER_POST",
    "NEGATIVE_CONTENT",
    "SUSPICIOUS_PATTERN",
    "SHARED_NEG_NET",
]


def _create_post_indexes(session, label):
    index_prefix = _safe_index_name(label)
    safe_label = _safe_label(label)
    for prop in ["batch_id", "session_id", "USERNAME", "LIKES", "RETWEETS", "POLARITY", "SENTIMENT"]:
        index_name = _safe_index_name("idx", index_prefix, prop)
        session.run(f"CREATE INDEX {index_name} IF NOT EXISTS FOR (n:{safe_label}) ON (n.{prop})")
    session.run("CREATE INDEX idx_linkx_user_session_username IF NOT EXISTS FOR (n:User) ON (n.session_id, n.Username)")


def _clear_post_relationships(session, session_id):
    session.run("""
    MATCH ()-[r]->()
    WHERE r.session_id = $session_id AND type(r) IN $relationship_types
    DELETE r
    """, session_id=str(session_id), relationship_types=POST_RELATIONSHIPS)
    session.run("""
    MATCH (n)
    WHERE n.session_id = $session_id
      AND n.generated_by = 'link_analysis'
      AND any(label IN labels(n) WHERE label IN ['User', 'LowEngagementCluster', 'NegativeSentiment', 'SuspiciousCluster'])
    DETACH DELETE n
    """, session_id=str(session_id))


def _count_post_relationships(session, session_id):
    result = session.run("""
    MATCH ()-[r]->()
    WHERE r.session_id = $session_id AND type(r) IN $relationship_types
    RETURN type(r) AS relationship_type, count(r) AS count
    """, session_id=str(session_id), relationship_types=POST_RELATIONSHIPS)
    return {record["relationship_type"]: record["count"] for record in result}


def _run_post_rules(session, label, session_id, provisional, batch_id=None):
    scope = "t.batch_id = $batch_id" if batch_id else _session_scope_clause("t")
    pair_scope = "(t1.batch_id = $batch_id OR t2.batch_id = $batch_id)" if batch_id else "true"

    session.run(f"""
    MATCH (t:{label})
    WHERE {scope}
      AND coalesce(t.USERNAME, t.Username, t.username, '') <> ''
    MERGE (u:User {_CK_USER})
    SET u.generated_by = 'link_analysis'
    MERGE (u)-[r:CREATED {_SID}]->(t)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#e6e6e6',
        r.provisional = $provisional,
        r.reason = 'user created post'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (t:{label})
    WHERE {scope}
      AND coalesce(toInteger(t.LIKES), toInteger(t.likes), 0) < 10
      AND coalesce(toInteger(t.RETWEETS), toInteger(t.retweets), 0) < 5
    MERGE (c:LowEngagementCluster {_CK_LOWENG})
    SET c.generated_by = 'link_analysis'
    MERGE (t)-[r:LOW_ENGAGEMENT {_SID}]->(c)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#e6e6e6',
        r.provisional = $provisional,
        r.reason = 'low likes and retweets'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (t:{label})
    WHERE {scope}
      AND (
        toLower(toString(coalesce(t.IS_INFLUENCER, t.is_influencer, 'false'))) IN ['true', '1', 'yes']
        OR coalesce(toInteger(t.FOLLOWERS), toInteger(t.followers), 0) >= 10000
      )
      AND coalesce(t.USERNAME, t.Username, t.username, '') <> ''
    MERGE (u:User {_CK_USER})
    SET u.generated_by = 'link_analysis'
    MERGE (t)-[r:INFLUENCER_POST {_SID}]->(u)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#363636',
        r.textcolor = '#eeeeee',
        r.provisional = $provisional,
        r.reason = 'post belongs to influencer account'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (t:{label})
    WHERE {scope}
      AND (
        coalesce(toFloat(t.POLARITY), toFloat(t.polarity), 0.0) < 0
        OR toLower(toString(coalesce(t.SENTIMENT, t.sentiment, ''))) CONTAINS 'negative'
      )
    MERGE (c:NegativeSentiment {_CK_NEGSENT})
    SET c.generated_by = 'link_analysis'
    MERGE (t)-[r:NEGATIVE_CONTENT {_SID}]->(c)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#dba124',
        r.provisional = $provisional,
        r.reason = 'negative post sentiment'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (t:{label})-[:LOW_ENGAGEMENT {_SID}]->(:LowEngagementCluster {_SID}),
          (t)-[:NEGATIVE_CONTENT {_SID}]->(:NegativeSentiment {_SID})
    WHERE {scope}
    MERGE (sc:SuspiciousCluster {_CK_SUSPCLUSTER})
    SET sc.generated_by = 'link_analysis'
    MERGE (t)-[r:SUSPICIOUS_PATTERN {_SID}]->(sc)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#d5d276',
        r.provisional = $provisional,
        r.reason = 'low engagement negative post'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (u1:User {_SID})-[:CREATED {_SID}]->(t1:{label})-[:NEGATIVE_CONTENT {_SID}]->(),
          (u2:User {_SID})-[:CREATED {_SID}]->(t2:{label})-[:NEGATIVE_CONTENT {_SID}]->()
    WHERE u1.Username < u2.Username
      AND {pair_scope}
    MERGE (u1)-[r:SHARED_NEG_NET {_SID}]->(u2)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#d5d276',
        r.provisional = $provisional,
        r.reason = 'users share negative post pattern'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)


def batch_graph_analysis_posts(driver, log_file, session_id=None, nodes_label="Tweet"):
    log_writer(log_file, f"[{datetime.now()}] [Info] Starting social media analysis")
    session_param = str(session_id) if session_id else ""
    label = _safe_label(nodes_label)
    thresholds = fetch_rule_thresholds()

    with driver.session() as session:
        _create_post_indexes(session, nodes_label)
        if session_param:
            _clear_post_relationships(session, session_param)
        _run_post_rules(session, label, session_param, provisional=False)
        counts = _count_post_relationships(session, session_param) if session_param else {}
        _write_gds_metrics(session, f"{session_param}_posts", nodes_label, session_param, POST_RELATIONSHIPS, log_file)

    log_writer(log_file, f"[{datetime.now()}] [Success] Social media analysis completed")
    return counts


def incremental_graph_analysis_posts(driver, session_id, nodes_label, batch_id, log_file):
    session_param = str(session_id)
    label = _safe_label(nodes_label)
    thresholds = fetch_rule_thresholds()
    log_writer(log_file, f"[{datetime.now()}] [Info] Running incremental social media analysis for batch {batch_id}")

    with driver.session() as session:
        _create_post_indexes(session, nodes_label)
        _run_post_rules(session, label, session_param, provisional=True, batch_id=batch_id)
        counts = _count_post_relationships(session, session_param)

    log_writer(log_file, f"[{datetime.now()}] [Info] Incremental social media analysis for batch {batch_id} flags: {counts}")
    return counts


# ====================================================
# Call Data Records
# ====================================================

CDR_RELATIONSHIPS = [
    "CALL_SEQUENCE",
    "CALLBACK_PATTERN",
    "FREQUENT_CONTACT",
    "SHORT_DURATION_BURST",
    "LONG_DURATION_CALL",
    "MISSED_CALL_SIGNAL",
    "CALL_RELAY",
    "STAR_PATTERN",
    "LOCATION_JUMP",
    "NIGHT_ACTIVITY",
    "HIGH_RISK_CONTACT",
    "FAN_OUT",
    "FAN_IN",
    "SIMULTANEOUS_CALL",
]


def _create_cdr_indexes(session, label):
    index_prefix = _safe_index_name(label)
    safe_label = _safe_label(label)
    for prop in ["batch_id", "session_id", "CALLING_NO", "CALLED_NO", "START_TIME", "LOCATION_ID"]:
        index_name = _safe_index_name("idx", index_prefix, prop)
        session.run(f"CREATE INDEX {index_name} IF NOT EXISTS FOR (n:{safe_label}) ON (n.{prop})")


def _clear_cdr_relationships(session, session_id):
    session.run("""
    MATCH ()-[r]->()
    WHERE r.session_id = $session_id AND type(r) IN $relationship_types
    DELETE r
    """, session_id=str(session_id), relationship_types=CDR_RELATIONSHIPS)


def _count_cdr_relationships(session, session_id):
    result = session.run("""
    MATCH ()-[r]->()
    WHERE r.session_id = $session_id AND type(r) IN $relationship_types
    RETURN type(r) AS relationship_type, count(r) AS count
    """, session_id=str(session_id), relationship_types=CDR_RELATIONSHIPS)
    return {record["relationship_type"]: record["count"] for record in result}


def _run_cdr_rules(session, label, session_id, high_risk_numbers, provisional, batch_id=None):
    scope = "c.batch_id = $batch_id" if batch_id else _session_scope_clause("c")
    pair_scope = "(a.batch_id = $batch_id OR b.batch_id = $batch_id)" if batch_id else "true"

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
      AND coalesce(c.CALLING_NO, '') <> ''
    WITH c.CALLING_NO AS caller, c
    ORDER BY coalesce(c.START_TIME, '')
    WITH caller, collect(c) AS calls
    WHERE size(calls) > 1
    UNWIND range(0, size(calls)-2) AS i
    WITH calls[i] AS a, calls[i+1] AS b
    MERGE (a)-[r:CALL_SEQUENCE {_SID}]->(b)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#c7c7ff',
        r.provisional = $provisional,
        r.reason = 'successive calls from same caller'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (a:{label}), (b:{label})
    WHERE {_session_scope_clause("a")}
      AND {_session_scope_clause("b")}
      AND {pair_scope}
      AND a.CALLING_NO = b.CALLED_NO
      AND a.CALLED_NO = b.CALLING_NO
      AND coalesce(a.CALLING_NO, '') <> ''
      AND elementId(a) <> elementId(b)
      AND coalesce(toString(b.START_TIME), '') > coalesce(toString(a.START_TIME), '')
    MERGE (a)-[r:CALLBACK_PATTERN {_SID}]->(b)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#ffb347',
        r.provisional = $provisional,
        r.reason = 'callee later calls the original caller'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {_session_scope_clause("c")}
      AND ({'c.batch_id = $batch_id AND' if batch_id else ''} true)
      AND coalesce(c.CALLING_NO, '') <> ''
      AND coalesce(c.CALLED_NO, '') <> ''
    WITH c.CALLING_NO AS caller, c.CALLED_NO AS callee, count(c) AS freq
    WHERE freq > 5
    MATCH (x:{label})
    WHERE {_session_scope_clause("x")}
      AND x.CALLING_NO = caller
      AND x.CALLED_NO = callee
    MERGE (x)-[r:FREQUENT_CONTACT {_SID}]->(x)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#00c1a2',
        r.provisional = $provisional,
        r.reason = 'frequent caller-callee pair',
        r.frequency = freq
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
    WITH c.CALLING_NO AS caller, count(c) AS short_calls
    WHERE caller IS NOT NULL
      AND caller <> ''
      AND short_calls > 5
    MATCH (x:{label})
    WHERE {_session_scope_clause("x")}
      AND x.CALLING_NO = caller
      AND coalesce(toInteger(x.DURATION_SECONDS), toInteger(x.DURATION), 0) < 20
    MERGE (x)-[r:SHORT_DURATION_BURST {_SID}]->(x)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#ff6f91',
        r.provisional = $provisional,
        r.reason = 'burst of short calls',
        r.short_calls = short_calls
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
      AND coalesce(toInteger(c.DURATION_SECONDS), toInteger(c.DURATION), 0) > 1800
    MERGE (c)-[r:LONG_DURATION_CALL {_SID}]->(c)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#7d3cff',
        r.textcolor = '#eeeeee',
        r.provisional = $provisional,
        r.reason = 'call duration exceeds 30 minutes'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
      AND coalesce(toInteger(c.DURATION_SECONDS), toInteger(c.DURATION), 0) = 0
    MERGE (c)-[r:MISSED_CALL_SIGNAL {_SID}]->(c)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#ffcc00',
        r.provisional = $provisional,
        r.reason = 'zero-duration call'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (a:{label}), (b:{label})
    WHERE {_session_scope_clause("a")}
      AND {_session_scope_clause("b")}
      AND {pair_scope}
      AND a.CALLED_NO = b.CALLING_NO
      AND coalesce(a.CALLED_NO, '') <> ''
      AND elementId(a) <> elementId(b)
      AND coalesce(toString(b.START_TIME), '') > coalesce(toString(a.START_TIME), '')
    MERGE (a)-[r:CALL_RELAY {_SID}]->(b)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#4caf50',
        r.provisional = $provisional,
        r.reason = 'called party later initiates another call'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {_session_scope_clause("c")}
      AND ({'c.batch_id = $batch_id AND' if batch_id else ''} true)
      AND coalesce(c.CALLING_NO, '') <> ''
    WITH c.CALLING_NO AS caller, count(DISTINCT c.CALLED_NO) AS targets
    WHERE targets > 10
    MATCH (x:{label})
    WHERE {_session_scope_clause("x")}
      AND x.CALLING_NO = caller
    MERGE (x)-[r:STAR_PATTERN {_SID}]->(x)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#0099ff',
        r.provisional = $provisional,
        r.reason = 'caller reaches many distinct targets',
        r.targets = targets
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
      AND coalesce(c.CALLING_NO, '') <> ''
    WITH c.CALLING_NO AS caller, c
    ORDER BY coalesce(c.START_TIME, '')
    WITH caller, collect(c) AS calls
    WHERE size(calls) > 1
    UNWIND range(0, size(calls)-2) AS i
    WITH calls[i] AS a, calls[i+1] AS b
    WHERE coalesce(a.LOCATION_ID, '') <> ''
      AND coalesce(b.LOCATION_ID, '') <> ''
      AND a.LOCATION_ID <> b.LOCATION_ID
    MERGE (a)-[r:LOCATION_JUMP {_SID}]->(b)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#ff3b3b',
        r.provisional = $provisional,
        r.reason = 'successive calls use different locations'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
      AND coalesce(toInteger(c.START_HOUR), toInteger(substring(toString(c.START_TIME), 11, 2)), 12) < 5
    MERGE (c)-[r:NIGHT_ACTIVITY {_SID}]->(c)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#1c1c54',
        r.textcolor = '#eeeeee',
        r.provisional = $provisional,
        r.reason = 'call starts between midnight and 05:00'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    UNWIND $nums AS num
    MATCH (c:{label})
    WHERE {scope}
      AND (c.CALLING_NO = num OR c.CALLED_NO = num)
    MERGE (c)-[r:HIGH_RISK_CONTACT {_SID}]->(c)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#de7d07',
        r.provisional = $provisional,
        r.reason = 'configured high-risk number appears in call',
        r.number = num
    """, nums=high_risk_numbers, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {_session_scope_clause("c")}
      AND ({'c.batch_id = $batch_id AND' if batch_id else ''} true)
      AND coalesce(c.CALLING_NO, '') <> ''
    WITH c.CALLING_NO AS caller, count(DISTINCT c.CALLED_NO) AS targets
    WHERE targets > 15
    MATCH (x:{label})
    WHERE {_session_scope_clause("x")}
      AND x.CALLING_NO = caller
    MERGE (x)-[r:FAN_OUT {_SID}]->(x)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#00ffaa',
        r.provisional = $provisional,
        r.reason = 'caller has high distinct outbound reach',
        r.targets = targets
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {_session_scope_clause("c")}
      AND ({'c.batch_id = $batch_id AND' if batch_id else ''} true)
      AND coalesce(c.CALLED_NO, '') <> ''
    WITH c.CALLED_NO AS callee, count(DISTINCT c.CALLING_NO) AS sources
    WHERE sources > 15
    MATCH (x:{label})
    WHERE {_session_scope_clause("x")}
      AND x.CALLED_NO = callee
    MERGE (x)-[r:FAN_IN {_SID}]->(x)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#ffaa00',
        r.provisional = $provisional,
        r.reason = 'callee has high distinct inbound reach',
        r.sources = sources
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (a:{label}), (b:{label})
    WHERE {_session_scope_clause("a")}
      AND {_session_scope_clause("b")}
      AND {pair_scope}
      AND a.CALLING_NO = b.CALLING_NO
      AND coalesce(a.CALLING_NO, '') <> ''
      AND elementId(a) < elementId(b)
      AND abs(coalesce(toInteger(a.START_EPOCH), 0) - coalesce(toInteger(b.START_EPOCH), 0)) < 10
      AND coalesce(toInteger(a.START_EPOCH), 0) > 0
    MERGE (a)-[r:SIMULTANEOUS_CALL {_SID}]->(b)
    SET r.is_evidence = true, r.anomaly_score = 0.3, r.bgcolor = '#ff66cc',
        r.provisional = $provisional,
        r.reason = 'same caller has near-simultaneous calls'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)


def batch_graph_analysis_cdr(driver, log_file, session_id=None, nodes_label="CallDataRecords", high_risk_numbers=None):
    if high_risk_numbers is None:
        high_risk_numbers = ["971503760906", "251946131995", "447911123456"]

    session_param = str(session_id) if session_id else ""
    label = _safe_label(nodes_label)
    thresholds = fetch_rule_thresholds()
    log_writer(log_file, f"[{datetime.now()}] [Info] Starting CDR analysis")

    with driver.session() as session:
        _create_cdr_indexes(session, nodes_label)
        if session_param:
            _clear_cdr_relationships(session, session_param)
        _run_cdr_rules(session, label, session_param, high_risk_numbers, provisional=False)
        counts = _count_cdr_relationships(session, session_param) if session_param else {}
        _write_gds_metrics(session, f"{session_param}_cdr", nodes_label, session_param, CDR_RELATIONSHIPS, log_file)

    log_writer(log_file, f"[{datetime.now()}] [Success] CDR analysis completed")
    return counts


def incremental_graph_analysis_cdr(driver, session_id, nodes_label, batch_id, log_file, high_risk_numbers=None):
    if high_risk_numbers is None:
        high_risk_numbers = ["971503760906", "251946131995", "447911123456"]

    session_param = str(session_id)
    label = _safe_label(nodes_label)
    thresholds = fetch_rule_thresholds()
    log_writer(log_file, f"[{datetime.now()}] [Info] Running incremental CDR analysis for batch {batch_id}")

    with driver.session() as session:
        _create_cdr_indexes(session, nodes_label)
        _run_cdr_rules(session, label, session_param, high_risk_numbers, provisional=True, batch_id=batch_id)
        counts = _count_cdr_relationships(session, session_param)

    log_writer(log_file, f"[{datetime.now()}] [Info] Incremental CDR analysis for batch {batch_id} flags: {counts}")
    return counts

def get_fraud_aggregator_query(label, scope_clause_t, session_id=None):
    return f'''
    MATCH (t:{label})-[r:SMURFING|CIRCULAR_FLOW|FUND_FLOW|DORMANT_TO_ACTIVE|ABNORMAL_BALANCE_CHANGE|HUB_AND_SPOKE|SHARED_IDENTIFIER|LATE_NIGHT_TX|JUST_BELOW_THRESHOLD|RAPID_WITHDRAWAL|ACCOUNT_ACTIVITY_SPIKE|HIGH_RISK_LINK|PEP_INVOLVED|SANCTIONED_ENTITY_MATCH]->()
    WHERE ({scope_clause_t}) 
      AND r.is_evidence = true
    WITH coalesce(t.LOGICAL_ACCOUNTNO, t.ACCOUNTNO) AS account_no,
         sum(r.anomaly_score) AS total_score,
         collect(distinct type(r)) AS evidence_types,
         count(r) AS evidence_count,
         collect(distinct t) AS involved_tx_nodes
    WHERE total_score >= 1.0
      AND account_no IS NOT NULL AND account_no <> ''
    
    CALL (account_no, total_score, evidence_types, evidence_count, involved_tx_nodes) {_OPEN}
      MERGE (a:AccountAlert {_CK_ACCTNO})
      SET a.total_score = total_score,
          a.evidence_types = evidence_types,
          a.evidence_count = evidence_count,
          a.created_at = datetime(),
          a.alert_type = 'FRAUD_ALERT_TARGET'
      
      WITH a, involved_tx_nodes
      UNWIND involved_tx_nodes AS t
      MERGE (a)-[fa:FRAUD_ALERT_TARGET {_SID}]->(t)
      SET fa.bgcolor = '#ff0000', fa.directed_display = true, fa.reason = 'Aggregated Fraud Evidence'
    {_CLOSE} IN TRANSACTIONS OF 100 ROWS
    '''
