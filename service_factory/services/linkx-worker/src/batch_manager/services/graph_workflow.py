from batch_manager.analyzing.LA_graphs_script import fetch_graph


def fetch_graph_result(payload):
    action = payload.get("id") or payload.get("action")
    source_id = payload.get("source_id") or payload.get("session_id")
    relationship = payload.get("relationship")

    if not source_id:
        return {"status": "failed", "message": "source_id_required", "nodes": [], "edges": []}
    if action != "relationship":
        return {"status": "failed", "message": "unsupported_graph_action", "nodes": [], "edges": []}

    graph = fetch_graph(action, "generate", source_id, relationship, "html")
    if isinstance(graph, tuple):
        return {"status": "failed", "message": "unexpected_graph_response", "response": graph}

    graph = dict(graph or {})
    graph["file"] = "graphs_template"
    if graph.get("error"):
        graph.setdefault("status", "failed")
        graph.setdefault("message", graph["error"])
    else:
        graph.setdefault("status", "succeeded")
        graph.setdefault("message", "success")

    return {
        **graph,
        "source_id": source_id,
        "graph_session_id": source_id,
        "relationship": relationship,
        "graph": graph,
    }
