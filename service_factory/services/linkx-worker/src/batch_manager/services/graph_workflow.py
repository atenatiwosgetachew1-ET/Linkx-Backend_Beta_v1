from batch_manager.analyzing.LA_graphs_script import fetch_graph


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

    graph = fetch_graph(action, "generate", source_id, relationship, "html")
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
    }
