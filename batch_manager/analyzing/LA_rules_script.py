from datetime import datetime, timedelta
from logger import log_writer
from textblob import TextBlob
import re


def _safe_label(label):
    return f"`{str(label).replace('`', '')}`"


def _session_scope_clause(alias="t"):
    return f"($session_id = '' OR {alias}.batch_id STARTS WITH $session_id OR {alias}.session_id = $session_id)"


def _safe_index_name(*parts):
    text = "_".join(str(part) for part in parts if part is not None)
    text = re.sub(r"[^A-Za-z0-9_]+", "_", text).strip("_").lower()
    return text or "idx"


TRANSACTION_RELATIONSHIPS = [
    "SMURFING",
    "CIRCULAR_FLOW",
    "FUND_FLOW",
    "DORMANT_TO_ACTIVE",
    "HIGH_RISK_LINK",
    "ABNORMAL_BALANCE_CHANGE",
]


def _create_transaction_indexes(session, label):
    index_prefix = _safe_index_name(label)
    safe_label = _safe_label(label)
    for prop in ["batch_id", "session_id", "ACCOUNTNO", "BENACCOUNTNO", "TRANSACTIONDATE"]:
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
):
    if high_risk_accounts is None:
        high_risk_accounts = ['ACC6970109','ACC4659300','ACC8482897','ACC5960522','ACC2976147','ACC6802190']

    log_writer(log_file, f"[{datetime.now()}] [Info] Starting transactions analysis")
    label = _safe_label(nodes_label)
    session_param = str(session_id) if session_id else ""

    with driver.session() as session:
        _create_transaction_indexes(session, nodes_label)
        if session_id:
            _clear_transaction_relationships(session, session_id)

        # ----------------------------
        # 1. SMURFING: repeated small transfers from one account to one beneficiary
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
        WITH
            t.ACCOUNTNO AS acc,
            t.BENACCOUNTNO AS beneficiary,
            t.TRANSACTIONDATE AS tx_day,
            t,
            coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount)) AS amount
        WHERE acc IS NOT NULL
          AND acc <> ''
          AND beneficiary IS NOT NULL
          AND beneficiary <> ''
          AND tx_day IS NOT NULL
          AND tx_day <> ''
          AND amount IS NOT NULL
          AND amount > 0
          AND amount < $single_tx_threshold
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, beneficiary, tx_day, collect(t) AS txns, sum(amount) AS total_amount, count(t) AS tx_count
        WHERE tx_count >= $min_tx_count
          AND total_amount >= $total_threshold
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, acc, beneficiary, tx_day, tx_count, total_amount
        MERGE (a)-[r:SMURFING {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d5d276',
            r.provisional = false,
            r.reason = 'multiple small same-day transfers below threshold',
            r.account = acc,
            r.beneficiary = beneficiary,
            r.tx_day = tx_day,
            r.tx_count = tx_count,
            r.total_amount = total_amount,
            r.single_tx_threshold = $single_tx_threshold,
            r.total_threshold = $total_threshold
        """, session_id=session_param,
             single_tx_threshold=single_tx_threshold,
             total_threshold=total_threshold,
             min_tx_count=min_tx_count)

        # ----------------------------
        # 2. CIRCULAR_FLOW: direct account-to-beneficiary reversal
        # ----------------------------
        session.run(f"""
        MATCH (a:{label}), (b:{label})
        WHERE ($session_id IS NULL OR ({_session_scope_clause("a")} AND {_session_scope_clause("b")}))
          AND a.ACCOUNTNO = b.BENACCOUNTNO
          AND a.BENACCOUNTNO = b.ACCOUNTNO
          AND a.ACCOUNTNO IS NOT NULL
          AND a.ACCOUNTNO <> ''
          AND a.BENACCOUNTNO IS NOT NULL
          AND a.BENACCOUNTNO <> ''
          AND id(a) < id(b)
          AND coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
        MERGE (a)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(b)
        SET r1.bgcolor = '#e6e6e6', r1.provisional = false, r1.reason = 'same-day reverse transfer pair'
        MERGE (b)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(a)
        SET r2.bgcolor = '#e6e6e6', r2.provisional = false, r2.reason = 'same-day reverse transfer pair'
        """, session_id=session_param)

        # ----------------------------
        # 3. FUND_FLOW: beneficiary becomes sender in a later transaction
        # ----------------------------
        session.run(f"""
        MATCH (a:{label}), (b:{label})
        WHERE ($session_id IS NULL OR ({_session_scope_clause("a")} AND {_session_scope_clause("b")}))
          AND a.BENACCOUNTNO = b.ACCOUNTNO
          AND a.BENACCOUNTNO IS NOT NULL
          AND a.BENACCOUNTNO <> ''
          AND id(a) <> id(b)
          AND (
            coalesce(a.TRANSACTIONDATE, '') < coalesce(b.TRANSACTIONDATE, '')
            OR (
              coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
              AND coalesce(a.TRANSACTIONTIME, '') < coalesce(b.TRANSACTIONTIME, '')
            )
          )
        WITH a, b
        ORDER BY a.TRANSACTIONDATE, a.TRANSACTIONTIME, b.TRANSACTIONDATE, b.TRANSACTIONTIME
        WITH a, collect(b)[0] AS b
        WHERE b IS NOT NULL
        MERGE (a)-[r:FUND_FLOW {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d8a822',
            r.provisional = false,
            r.reason = 'beneficiary later acts as sender'
        """, session_id=session_param)

        # ----------------------------
        # 4. DORMANT_TO_ACTIVE
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant'
          AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active'
        MERGE (t)-[r:DORMANT_TO_ACTIVE {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#c20f0f',
            r.textcolor = '#eeeeee',
            r.provisional = false,
            r.reason = 'dormant source account transacts with active beneficiary'
        """, session_id=session_param)

        # ----------------------------
        # 5. HIGH_RISK_LINK: configured risky account directly appears in transaction
        # ----------------------------
        session.run(f"""
        UNWIND $accounts AS acc
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
          AND (t.ACCOUNTNO = acc OR t.BENACCOUNTNO = acc)
        MERGE (t)-[r:HIGH_RISK_LINK {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#de7d07',
            r.provisional = false,
            r.reason = 'configured high-risk account appears in transaction',
            r.account = acc
        """, accounts=high_risk_accounts, session_id=session_param)

        # ----------------------------
        # 6. ABNORMAL_BALANCE_CHANGE: current balance move is an outlier for the account
        # ----------------------------
        session.run(f"""
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
        WITH t.ACCOUNTNO AS acc, t,
             coalesce(toFloat(t.BALANCEHELD), toFloat(t.BALANCE), toFloat(t.balance)) AS balance
        WHERE acc IS NOT NULL AND acc <> '' AND balance IS NOT NULL
        WITH t.ACCOUNTNO AS acc, t
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, collect(t) AS txns
        UNWIND range(1, size(txns)-1) AS i
        WITH txns[i] AS current,
             txns[i-1] AS previous,
             txns[CASE WHEN i-11 < 0 THEN 0 ELSE i-11 END .. i] AS history
        WITH current, previous,
             abs(
               coalesce(toFloat(current.BALANCEHELD), toFloat(current.BALANCE), toFloat(current.balance)) -
               coalesce(toFloat(previous.BALANCEHELD), toFloat(previous.BALANCE), toFloat(previous.balance))
             ) AS current_change,
             [j IN range(1, size(history)-1) |
               abs(
                 coalesce(toFloat(history[j].BALANCEHELD), toFloat(history[j].BALANCE), toFloat(history[j].balance)) -
                 coalesce(toFloat(history[j-1].BALANCEHELD), toFloat(history[j-1].BALANCE), toFloat(history[j-1].balance))
               )
             ] AS changes
        WITH current, previous, current_change, [c IN changes WHERE c IS NOT NULL AND c > 0] AS valid_changes
        WHERE size(valid_changes) >= 3
        WITH current, previous, current_change,
             reduce(s = 0.0, c IN valid_changes | s + c) / size(valid_changes) AS avg_change
        WHERE avg_change > 0 AND current_change >= avg_change * $threshold
        MERGE (previous)-[r:ABNORMAL_BALANCE_CHANGE {{session_id:$session_id}}]->(current)
        SET r.bgcolor = '#196e08',
            r.textcolor = '#eeeeee',
            r.provisional = false,
            r.reason = 'balance change exceeds recent account baseline',
            r.change = current_change,
            r.average_recent_change = avg_change,
            r.threshold_multiplier = $threshold
        """, threshold=threshold_multiplier, session_id=session_param)

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
):
    if high_risk_accounts is None:
        high_risk_accounts = ['ACC6970109','ACC4659300','ACC8482897','ACC5960522','ACC2976147','ACC6802190']

    session_param = str(session_id)
    label = _safe_label(nodes_label)
    log_writer(log_file, f"[{datetime.now()}] [Info] Running incremental transaction analysis for batch {batch_id}")

    with driver.session() as session:
        _create_transaction_indexes(session, nodes_label)

        # Smurfing: start from new rows, then inspect only matching account/beneficiary/day groups.
        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        WITH DISTINCT seed.ACCOUNTNO AS acc, seed.BENACCOUNTNO AS beneficiary, seed.TRANSACTIONDATE AS tx_day
        WHERE acc IS NOT NULL AND acc <> ''
          AND beneficiary IS NOT NULL AND beneficiary <> ''
          AND tx_day IS NOT NULL AND tx_day <> ''
        MATCH (t:{label})
        WHERE {_session_scope_clause("t")}
          AND t.ACCOUNTNO = acc
          AND t.BENACCOUNTNO = beneficiary
          AND t.TRANSACTIONDATE = tx_day
        WITH acc, beneficiary, tx_day, t,
             coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount)) AS amount
        WHERE amount IS NOT NULL
          AND amount > 0
          AND amount < $single_tx_threshold
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, beneficiary, tx_day, collect(t) AS txns, sum(amount) AS total_amount, count(t) AS tx_count
        WHERE tx_count >= $min_tx_count
          AND total_amount >= $total_threshold
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b, acc, beneficiary, tx_day, tx_count, total_amount
        MERGE (a)-[r:SMURFING {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d5d276',
            r.provisional = true,
            r.reason = 'multiple small same-day transfers below threshold',
            r.account = acc,
            r.beneficiary = beneficiary,
            r.tx_day = tx_day,
            r.tx_count = tx_count,
            r.total_amount = total_amount,
            r.single_tx_threshold = $single_tx_threshold,
            r.total_threshold = $total_threshold
        """, batch_id=batch_id,
             session_id=session_param,
             single_tx_threshold=single_tx_threshold,
             total_threshold=total_threshold,
             min_tx_count=min_tx_count)

        # Circular flow: only pairs where the current batch is one side of the reversal.
        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        MATCH (other:{label})
        WHERE {_session_scope_clause("other")}
          AND seed.ACCOUNTNO = other.BENACCOUNTNO
          AND seed.BENACCOUNTNO = other.ACCOUNTNO
          AND seed.ACCOUNTNO IS NOT NULL
          AND seed.ACCOUNTNO <> ''
          AND seed.BENACCOUNTNO IS NOT NULL
          AND seed.BENACCOUNTNO <> ''
          AND id(seed) <> id(other)
          AND coalesce(seed.TRANSACTIONDATE, '') = coalesce(other.TRANSACTIONDATE, '')
        MERGE (seed)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(other)
        SET r1.bgcolor = '#e6e6e6', r1.provisional = true, r1.reason = 'same-day reverse transfer pair'
        MERGE (other)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(seed)
        SET r2.bgcolor = '#e6e6e6', r2.provisional = true, r2.reason = 'same-day reverse transfer pair'
        """, batch_id=batch_id, session_id=session_param)

        # Fund flow: new nodes can either precede or complete a downstream flow.
        session.run(f"""
        MATCH (a:{label}), (b:{label})
        WHERE (a.batch_id = $batch_id OR b.batch_id = $batch_id)
          AND {_session_scope_clause("a")}
          AND {_session_scope_clause("b")}
          AND a.BENACCOUNTNO = b.ACCOUNTNO
          AND a.BENACCOUNTNO IS NOT NULL
          AND a.BENACCOUNTNO <> ''
          AND id(a) <> id(b)
          AND (
            coalesce(a.TRANSACTIONDATE, '') < coalesce(b.TRANSACTIONDATE, '')
            OR (
              coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
              AND coalesce(a.TRANSACTIONTIME, '') < coalesce(b.TRANSACTIONTIME, '')
            )
          )
        MERGE (a)-[r:FUND_FLOW {{session_id:$session_id}}]->(b)
        SET r.bgcolor = '#d8a822',
            r.provisional = true,
            r.reason = 'beneficiary later acts as sender'
        """, batch_id=batch_id, session_id=session_param)

        # Cheap row-local flags: only new batch rows.
        session.run(f"""
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
          AND toLower(coalesce(t.ACCOUNTSTATE, '')) = 'dormant'
          AND toLower(coalesce(t.BENACCOUNTSTATE, '')) = 'active'
        MERGE (t)-[r:DORMANT_TO_ACTIVE {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#c20f0f',
            r.textcolor = '#eeeeee',
            r.provisional = true,
            r.reason = 'dormant source account transacts with active beneficiary'
        """, batch_id=batch_id, session_id=session_param)

        session.run(f"""
        UNWIND $accounts AS acc
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
          AND (t.ACCOUNTNO = acc OR t.BENACCOUNTNO = acc)
        MERGE (t)-[r:HIGH_RISK_LINK {{session_id:$session_id}}]->(t)
        SET r.bgcolor = '#de7d07',
            r.provisional = true,
            r.reason = 'configured high-risk account appears in transaction',
            r.account = acc
        """, accounts=high_risk_accounts, batch_id=batch_id, session_id=session_param)

        # Balance outlier: recalculate only accounts touched by this batch.
        session.run(f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = $batch_id
        WITH collect(DISTINCT seed.ACCOUNTNO) AS affected_accounts
        MATCH (t:{label})
        WHERE {_session_scope_clause("t")}
          AND t.ACCOUNTNO IN affected_accounts
        WITH t.ACCOUNTNO AS acc, t,
             coalesce(toFloat(t.BALANCEHELD), toFloat(t.BALANCE), toFloat(t.balance)) AS balance
        WHERE acc IS NOT NULL AND acc <> '' AND balance IS NOT NULL
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, collect(t) AS txns
        UNWIND range(1, size(txns)-1) AS i
        WITH txns[i] AS current,
             txns[i-1] AS previous,
             txns[CASE WHEN i-11 < 0 THEN 0 ELSE i-11 END .. i] AS history
        WHERE current.batch_id = $batch_id OR previous.batch_id = $batch_id
        WITH current, previous,
             abs(
               coalesce(toFloat(current.BALANCEHELD), toFloat(current.BALANCE), toFloat(current.balance)) -
               coalesce(toFloat(previous.BALANCEHELD), toFloat(previous.BALANCE), toFloat(previous.balance))
             ) AS current_change,
             [j IN range(1, size(history)-1) |
               abs(
                 coalesce(toFloat(history[j].BALANCEHELD), toFloat(history[j].BALANCE), toFloat(history[j].balance)) -
                 coalesce(toFloat(history[j-1].BALANCEHELD), toFloat(history[j-1].BALANCE), toFloat(history[j-1].balance))
               )
             ] AS changes
        WITH current, previous, current_change, [c IN changes WHERE c IS NOT NULL AND c > 0] AS valid_changes
        WHERE size(valid_changes) >= 3
        WITH current, previous, current_change,
             reduce(s = 0.0, c IN valid_changes | s + c) / size(valid_changes) AS avg_change
        WHERE avg_change > 0 AND current_change >= avg_change * $threshold
        MERGE (previous)-[r:ABNORMAL_BALANCE_CHANGE {{session_id:$session_id}}]->(current)
        SET r.bgcolor = '#196e08',
            r.textcolor = '#eeeeee',
            r.provisional = true,
            r.reason = 'balance change exceeds recent account baseline',
            r.change = current_change,
            r.average_recent_change = avg_change,
            r.threshold_multiplier = $threshold
        """, batch_id=batch_id, session_id=session_param, threshold=threshold_multiplier)

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
    MERGE (u:User {{Username: coalesce(t.USERNAME, t.Username, t.username), session_id: $session_id}})
    SET u.generated_by = 'link_analysis'
    MERGE (u)-[r:CREATED {{session_id:$session_id}}]->(t)
    SET r.bgcolor = '#e6e6e6',
        r.provisional = $provisional,
        r.reason = 'user created post'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (t:{label})
    WHERE {scope}
      AND coalesce(toInteger(t.LIKES), toInteger(t.likes), 0) < 10
      AND coalesce(toInteger(t.RETWEETS), toInteger(t.retweets), 0) < 5
    MERGE (c:LowEngagementCluster {{flag:'LOW_ENG', session_id:$session_id}})
    SET c.generated_by = 'link_analysis'
    MERGE (t)-[r:LOW_ENGAGEMENT {{session_id:$session_id}}]->(c)
    SET r.bgcolor = '#e6e6e6',
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
    MERGE (u:User {{Username: coalesce(t.USERNAME, t.Username, t.username), session_id: $session_id}})
    SET u.generated_by = 'link_analysis'
    MERGE (t)-[r:INFLUENCER_POST {{session_id:$session_id}}]->(u)
    SET r.bgcolor = '#363636',
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
    MERGE (c:NegativeSentiment {{flag:'NEG_SENTIMENT', session_id:$session_id}})
    SET c.generated_by = 'link_analysis'
    MERGE (t)-[r:NEGATIVE_CONTENT {{session_id:$session_id}}]->(c)
    SET r.bgcolor = '#dba124',
        r.provisional = $provisional,
        r.reason = 'negative post sentiment'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (t:{label})-[:LOW_ENGAGEMENT {{session_id:$session_id}}]->(:LowEngagementCluster {{session_id:$session_id}}),
          (t)-[:NEGATIVE_CONTENT {{session_id:$session_id}}]->(:NegativeSentiment {{session_id:$session_id}})
    WHERE {scope}
    MERGE (sc:SuspiciousCluster {{type:'LOW_ENG_NEG_SENT', session_id:$session_id}})
    SET sc.generated_by = 'link_analysis'
    MERGE (t)-[r:SUSPICIOUS_PATTERN {{session_id:$session_id}}]->(sc)
    SET r.bgcolor = '#d5d276',
        r.provisional = $provisional,
        r.reason = 'low engagement negative post'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (u1:User {{session_id:$session_id}})-[:CREATED {{session_id:$session_id}}]->(t1:{label})-[:NEGATIVE_CONTENT {{session_id:$session_id}}]->(),
          (u2:User {{session_id:$session_id}})-[:CREATED {{session_id:$session_id}}]->(t2:{label})-[:NEGATIVE_CONTENT {{session_id:$session_id}}]->()
    WHERE u1.Username < u2.Username
      AND {pair_scope}
    MERGE (u1)-[r:SHARED_NEG_NET {{session_id:$session_id}}]->(u2)
    SET r.bgcolor = '#d5d276',
        r.provisional = $provisional,
        r.reason = 'users share negative post pattern'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)


def batch_graph_analysis_posts(driver, log_file, session_id=None, nodes_label="Tweet"):
    log_writer(log_file, f"[{datetime.now()}] [Info] Starting social media analysis")
    session_param = str(session_id) if session_id else ""
    label = _safe_label(nodes_label)

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
    MERGE (a)-[r:CALL_SEQUENCE {{session_id:$session_id}}]->(b)
    SET r.bgcolor = '#c7c7ff',
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
      AND id(a) <> id(b)
      AND coalesce(toString(b.START_TIME), '') > coalesce(toString(a.START_TIME), '')
    MERGE (a)-[r:CALLBACK_PATTERN {{session_id:$session_id}}]->(b)
    SET r.bgcolor = '#ffb347',
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
    MERGE (x)-[r:FREQUENT_CONTACT {{session_id:$session_id}}]->(x)
    SET r.bgcolor = '#00c1a2',
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
    MERGE (x)-[r:SHORT_DURATION_BURST {{session_id:$session_id}}]->(x)
    SET r.bgcolor = '#ff6f91',
        r.provisional = $provisional,
        r.reason = 'burst of short calls',
        r.short_calls = short_calls
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
      AND coalesce(toInteger(c.DURATION_SECONDS), toInteger(c.DURATION), 0) > 1800
    MERGE (c)-[r:LONG_DURATION_CALL {{session_id:$session_id}}]->(c)
    SET r.bgcolor = '#7d3cff',
        r.textcolor = '#eeeeee',
        r.provisional = $provisional,
        r.reason = 'call duration exceeds 30 minutes'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
      AND coalesce(toInteger(c.DURATION_SECONDS), toInteger(c.DURATION), 0) = 0
    MERGE (c)-[r:MISSED_CALL_SIGNAL {{session_id:$session_id}}]->(c)
    SET r.bgcolor = '#ffcc00',
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
      AND id(a) <> id(b)
      AND coalesce(toString(b.START_TIME), '') > coalesce(toString(a.START_TIME), '')
    MERGE (a)-[r:CALL_RELAY {{session_id:$session_id}}]->(b)
    SET r.bgcolor = '#4caf50',
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
    MERGE (x)-[r:STAR_PATTERN {{session_id:$session_id}}]->(x)
    SET r.bgcolor = '#0099ff',
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
    MERGE (a)-[r:LOCATION_JUMP {{session_id:$session_id}}]->(b)
    SET r.bgcolor = '#ff3b3b',
        r.provisional = $provisional,
        r.reason = 'successive calls use different locations'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    MATCH (c:{label})
    WHERE {scope}
      AND coalesce(toInteger(c.START_HOUR), toInteger(substring(toString(c.START_TIME), 11, 2)), 12) < 5
    MERGE (c)-[r:NIGHT_ACTIVITY {{session_id:$session_id}}]->(c)
    SET r.bgcolor = '#1c1c54',
        r.textcolor = '#eeeeee',
        r.provisional = $provisional,
        r.reason = 'call starts between midnight and 05:00'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)

    session.run(f"""
    UNWIND $nums AS num
    MATCH (c:{label})
    WHERE {scope}
      AND (c.CALLING_NO = num OR c.CALLED_NO = num)
    MERGE (c)-[r:HIGH_RISK_CONTACT {{session_id:$session_id}}]->(c)
    SET r.bgcolor = '#de7d07',
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
    MERGE (x)-[r:FAN_OUT {{session_id:$session_id}}]->(x)
    SET r.bgcolor = '#00ffaa',
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
    MERGE (x)-[r:FAN_IN {{session_id:$session_id}}]->(x)
    SET r.bgcolor = '#ffaa00',
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
      AND id(a) < id(b)
      AND abs(coalesce(toInteger(a.START_EPOCH), 0) - coalesce(toInteger(b.START_EPOCH), 0)) < 10
      AND coalesce(toInteger(a.START_EPOCH), 0) > 0
    MERGE (a)-[r:SIMULTANEOUS_CALL {{session_id:$session_id}}]->(b)
    SET r.bgcolor = '#ff66cc',
        r.provisional = $provisional,
        r.reason = 'same caller has near-simultaneous calls'
    """, session_id=session_id, batch_id=batch_id, provisional=provisional)


def batch_graph_analysis_cdr(driver, log_file, session_id=None, nodes_label="CallDataRecords", high_risk_numbers=None):
    if high_risk_numbers is None:
        high_risk_numbers = ["971503760906", "251946131995", "447911123456"]

    session_param = str(session_id) if session_id else ""
    label = _safe_label(nodes_label)
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
    log_writer(log_file, f"[{datetime.now()}] [Info] Running incremental CDR analysis for batch {batch_id}")

    with driver.session() as session:
        _create_cdr_indexes(session, nodes_label)
        _run_cdr_rules(session, label, session_param, high_risk_numbers, provisional=True, batch_id=batch_id)
        counts = _count_cdr_relationships(session, session_param)

    log_writer(log_file, f"[{datetime.now()}] [Info] Incremental CDR analysis for batch {batch_id} flags: {counts}")
    return counts
