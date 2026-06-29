import json
import os

from batch_manager.analyzing.LA_graphs_script import fetch_graph


def _database_url():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def _env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _insert_graph_chunk(cur, job_id, session_id, chunk):
    cur.execute(
        """
        INSERT INTO job_events (job_id, session_id, event_type, message, payload)
        VALUES (%s, %s, 'graph_chunk', %s, %s::jsonb)
        """,
        (str(job_id), str(session_id) if session_id is not None else None, f"Graph chunk {chunk.get('chunk_index')}", json.dumps(chunk)),
    )


def fetch_graph_result(payload):
    action = payload.get("id") or payload.get("action")
    source_id = payload.get("source_id") or payload.get("session_id")
    relationship = payload.get("relationship")

    if not source_id:
        return {
            "status": "failed",
            "message": "source_id_required",
            "source_id": source_id,
            "graph_session_id": source_id,
            "relationship": relationship,
            "nodes": [],
            "edges": [],
            "partial": False,
            "timed_out": False,
            "graph_limit": 0,
        }
    if action != "relationship":
        return {
            "status": "failed",
            "message": "unsupported_graph_action",
            "source_id": source_id,
            "graph_session_id": source_id,
            "relationship": relationship,
            "nodes": [],
            "edges": [],
            "partial": False,
            "timed_out": False,
            "graph_limit": 0,
        }

    job_id = payload.get("job_id")
    chunk_size = max(1, _env_int("LINKX_GRAPH_CHUNK_SIZE", 250))
    chunk_conn = None
    chunk_cur = None

    try:
        if job_id and _database_url():
            import psycopg
            chunk_conn = psycopg.connect(_database_url(), application_name="linkx-worker-graph-chunk")
            chunk_cur = chunk_conn.cursor()

        def emit_chunk(chunk):
            if not chunk_cur:
                return
            _insert_graph_chunk(chunk_cur, job_id, source_id, chunk)
            chunk_conn.commit()

        graph = fetch_graph(
            action,
            "generate",
            source_id,
            relationship,
            "html",
            chunk_callback=emit_chunk if chunk_cur else None,
            chunk_size=chunk_size,
        )
    finally:
        if chunk_cur:
            chunk_cur.close()
        if chunk_conn:
            chunk_conn.close()
    if isinstance(graph, tuple):
        return {
            "status": "failed",
            "message": "unexpected_graph_response",
            "source_id": source_id,
            "graph_session_id": source_id,
            "relationship": relationship,
            "nodes": [],
            "edges": [],
            "partial": False,
            "timed_out": False,
            "graph_limit": 0,
            "response": graph,
        }

    graph = dict(graph or {})
    graph.setdefault("nodes", [])
    graph.setdefault("edges", [])
    if graph.get("error"):
        graph.setdefault("status", "failed")
        graph.setdefault("message", graph["error"])
    else:
        graph.setdefault("status", "succeeded")
        graph.setdefault("message", "success")

    return {
        "status": graph.get("status"),
        "message": graph.get("message"),
        "source_id": source_id,
        "graph_session_id": source_id,
        "relationship": relationship,
        "nodes": graph.get("nodes", []),
        "edges": graph.get("edges", []),
        "partial": bool(graph.get("partial")),
        "timed_out": bool(graph.get("timed_out")),
        "graph_limit": graph.get("graph_limit"),
        "total_nodes": graph.get("total_nodes", len(graph.get("nodes", []))),
        "total_edges": graph.get("total_edges", len(graph.get("edges", []))),
        "chunk_count": graph.get("chunk_count", 0),
        "chunk_size": chunk_size if job_id else None,
        "chunked": bool(job_id),
    }
