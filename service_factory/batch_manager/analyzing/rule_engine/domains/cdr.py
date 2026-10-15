"""CDR domain rule module — call data records analysis.

Contains CDR_RELATIONSHIPS (14 typologies), Cypher query generators,
and batch/incremental entry points for telecom/CDR analysis.
"""

from datetime import datetime

try:
    from logger import log_writer
except ImportError:
    def log_writer(path, msg):
        print(msg, flush=True)

from batch_manager.analyzing.rule_engine.shared import (
    _safe_label, _session_scope_clause, _safe_index_name,
    _SID, _OPEN, _CLOSE,
)
from batch_manager.analyzing.rule_engine.thresholds import fetch_rule_thresholds
from batch_manager.analyzing.rule_engine.gds import _write_gds_metrics


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
