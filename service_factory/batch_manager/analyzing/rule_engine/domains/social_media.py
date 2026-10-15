"""Social media domain rule module — post/tweet analysis.

Contains POST_RELATIONSHIPS (6 typologies), Cypher query generators,
and batch/incremental entry points for social media analysis.
"""

from datetime import datetime

try:
    from logger import log_writer
except ImportError:
    def log_writer(path, msg):
        print(msg, flush=True)

from batch_manager.analyzing.rule_engine.shared import (
    _safe_label, _session_scope_clause, _safe_index_name,
    _CK_USER, _CK_LOWENG, _CK_NEGSENT, _CK_SUSPCLUSTER, _SID, _OPEN, _CLOSE,
)
from batch_manager.analyzing.rule_engine.gds import _write_gds_metrics


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

