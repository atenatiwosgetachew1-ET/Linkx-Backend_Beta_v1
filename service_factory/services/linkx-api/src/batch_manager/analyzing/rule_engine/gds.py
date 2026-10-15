"""Graph Data Science (GDS) metrics writer for the LinkX rule engine.

Projects Neo4j GDS graphs from anomaly evidence edges and computes
centrality scores (PageRank, betweenness, eigenvector, degree, WCC).
"""

from datetime import datetime

try:
    from logger import log_writer
except ImportError:
    def log_writer(path, msg):
        print(msg, flush=True)


def _cypher_string(value):
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def _write_gds_metrics(session, graph_name, label, session_id, relationship_types, log_file, anomaly_only=False):
    if not session_id or not relationship_types:
        return

    escaped_session = _cypher_string(session_id)
    relationship_literal = "[" + ", ".join(f"'{_cypher_string(rel)}'" for rel in relationship_types) + "]"

    if anomaly_only:
        # Project ONLY nodes that participate in at least one evidence edge.
        # This gives more meaningful centrality scores: PageRank reflects
        # importance within the suspicious subgraph, not diluted by clean traffic.
        node_query = (
            "MATCH (n)-[r]->() "
            f"WHERE r.session_id = '{escaped_session}' AND type(r) IN {relationship_literal} "
            "AND r.is_evidence = true "
            "RETURN DISTINCT id(n) AS id "
            "UNION "
            "MATCH ()-[r]->(n) "
            f"WHERE r.session_id = '{escaped_session}' AND type(r) IN {relationship_literal} "
            "AND r.is_evidence = true "
            "RETURN DISTINCT id(n) AS id"
        )
    else:
        # Full projection: all session nodes (used by Source-Target and other flows)
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
        if log_file:
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
        if log_file:
            log_writer(log_file, f"[{datetime.now()}] [Success] GDS metrics completed for {graph_name}")
    except Exception as exc:
        if log_file:
            log_writer(log_file, f"[{datetime.now()}] [Warning] GDS metrics skipped for {graph_name}: {exc}")
