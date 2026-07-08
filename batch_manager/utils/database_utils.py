import time
from globals import _session_store


def _session_batch_prefix(session_id):
    return f"{str(session_id or '')}_"


def _session_scope_clause(alias, include_run=False):
    run_clause = f" OR ($run_id IS NOT NULL AND {alias}.run_id = $run_id)" if include_run else ""
    return (
        f"({alias}.session_id = $session_id "
        f"OR coalesce({alias}.batch_id, '') STARTS WITH $batch_prefix"
        f"{run_clause})"
    )


def _current_session_run_id(driver, session_id):
    if not session_id:
        return None
    try:
        with driver.session() as session:
            record = session.run(
                "MATCH (s:Session {id:$session_id}) RETURN s.run_id AS run_id LIMIT 1",
                session_id=str(session_id),
            ).single()
        return str(record["run_id"]) if record and record.get("run_id") else None
    except Exception:
        return None

def get_graph_metadata(driver, session_id, tool_credentials=None):
    batch_prefix = _session_batch_prefix(session_id)
    run_id = _current_session_run_id(driver, session_id)
    with driver.session() as session:
        # Database info
        db_info_record = session.run("CALL db.info()").single()
        database_name = db_info_record["name"] if db_info_record else None

        # User
        username = tool_credentials.get("username") if tool_credentials else None

        # Nodes tied to this exact session ownership set.
        total_nodes_record = session.run(
            f"MATCH (n) WHERE {_session_scope_clause('n', include_run=True)} RETURN count(DISTINCT n) AS total_nodes",
            session_id=session_id,
            batch_prefix=batch_prefix,
            run_id=run_id,
        ).single()
        total_nodes = total_nodes_record["total_nodes"] if total_nodes_record else 0

        # Relationships tied to this exact session ownership set.
        total_relationships_record = session.run(
            f"MATCH ()-[r]->() WHERE {_session_scope_clause('r', include_run=True)} RETURN count(DISTINCT r) AS total_relationships",
            session_id=session_id,
            batch_prefix=batch_prefix,
            run_id=run_id,
        ).single()
        total_relationships = total_relationships_record["total_relationships"] if total_relationships_record else 0

        # Relationship labels
        relationship_labels_record = session.run(
            f"MATCH ()-[r]->() WHERE {_session_scope_clause('r', include_run=True)} RETURN COLLECT(DISTINCT type(r)) AS labels",
            session_id=session_id,
            batch_prefix=batch_prefix,
            run_id=run_id,
        ).single()
        relationship_labels = relationship_labels_record["labels"] if relationship_labels_record else []

        # Property keys tied to session
        property_keys = [
            record["key"]
            for record in session.run(
                f"MATCH (n) WHERE {_session_scope_clause('n', include_run=True)} WITH n LIMIT 500 UNWIND keys(n) AS key RETURN DISTINCT key ORDER BY key",
                session_id=session_id,
                batch_prefix=batch_prefix,
                run_id=run_id,
            )
        ]

        # Neo4j version
        version_record = session.run(
            """
            CALL dbms.components()
            YIELD name, versions
            WHERE name CONTAINS 'Neo4j'
            RETURN versions
            """
        ).single()
        version = version_record["versions"][0] if version_record else None

    return {
        "sourceId": session_id,
        "database": database_name,
        "user": username,
        "total_nodes": total_nodes,
        "total_relationships": total_relationships,
        "relationship_labels": relationship_labels,
        "property_keys": property_keys,
        "neo4j_version": version,
        "live_analysis": _session_store.get(session_id, {}).get("live_analysis"),
    }


def _fetch_relationship_graph(driver, session_id, relationship_type=None, limit=1000):
    rel_filter = "AND type(r) = $relationship_type" if relationship_type else ""
    query = f"""
        MATCH (a)-[r]->(b)
        WHERE r.session_id = $session_id {rel_filter}
        RETURN a, r, b
        LIMIT $limit
    """
    nodes = {}
    edges = []
    with driver.session() as session:
        for record in session.run(
            query,
            session_id=str(session_id),
            relationship_type=relationship_type,
            limit=int(limit),
        ):
            a = record["a"]
            b = record["b"]
            r = record["r"]
            a_id = getattr(a, "element_id", None) or str(a.id)
            b_id = getattr(b, "element_id", None) or str(b.id)
            r_id = getattr(r, "element_id", None) or str(r.id)
            nodes[a_id] = {
                "id": a_id,
                "label": a.get("account_number") or a.get("NodeId") or str(a_id),
                **dict(a),
            }
            nodes[b_id] = {
                "id": b_id,
                "label": b.get("account_number") or b.get("NodeId") or str(b_id),
                **dict(b),
            }
            edges.append({
                "id": r_id,
                "from": a_id,
                "to": b_id,
                "label": r.type,
                **dict(r),
            })
    return {"nodes": list(nodes.values()), "edges": edges}


def graph_status_stream(socketio, sid, session_id, registry_entry, node_label=None, primary_rel_type=None):
    """
    Stream metadata every 5 seconds and relationships/graph payloads when changed.
    """

    stop_event = registry_entry["stop_event"]
    driver = registry_entry["driver"]
    tool_credentials = registry_entry["tool_credentials"]
    registry_entry["latest_relationships"] = []
    last_rel_hash = None
    last_graph_hash = None

    def emit_metadata_once():
        metadata = get_graph_metadata(driver, session_id, tool_credentials)
        registry_entry["static_infos"] = metadata
        socketio.emit(
            "status",
            {"type": "metadata", "data": metadata, "session_id": session_id},
            to=sid,
        )

    def fetch_relationship_summaries():
        with driver.session() as session:
            result = session.run(
                """
                MATCH ()-[r]->()
                WHERE r.session_id = $session_id
                RETURN
                    type(r) AS type,
                    count(r) AS count,
                    coalesce(min(properties(r).color), "#333") AS color,
                    coalesce(min(properties(r).bgcolor), "#DDD") AS bgcolor
                ORDER BY type
                """,
                session_id=session_id,
            )
            return [{
                "id": r["type"],
                "type": r["type"],
                "count": r["count"],
                "color": r["color"] or "#333",
                "bgcolor": r["bgcolor"] or "#DDD",
            } for r in result]

    def emit_relationships_once():
        relationships = fetch_relationship_summaries()
        registry_entry["latest_relationships"] = relationships
        socketio.emit(
            "status",
            {"type": "relationships", "data": relationships, "session_id": session_id},
            to=sid,
        )
        socketio.emit(
            "status",
            {
                "type": "metadata",
                "data": {
                    "sourceId": session_id,
                    "user": tool_credentials.get("username") if tool_credentials else None,
                    "total_relationships": sum(int(r.get("count") or 0) for r in relationships),
                    "relationship_labels": [r.get("type") for r in relationships],
                    "partial": True,
                    "live_analysis": _session_store.get(session_id, {}).get("live_analysis"),
                },
                "session_id": session_id,
            },
            to=sid,
        )
        return relationships

    try:
        initial_relationships = emit_relationships_once()
        last_rel_hash = hash(tuple((r["id"], r["type"], r["color"], r["bgcolor"]) for r in initial_relationships))
    except Exception as e:
        socketio.emit("status", {"type": "error", "error": f"Relationships error: {e}", "session_id": session_id}, to=sid)

    try:
        emit_metadata_once()
    except Exception as e:
        socketio.emit("status", {"type": "error", "error": f"Metadata error: {e}", "session_id": session_id}, to=sid)

    # -------------------------
    # Metadata loop (every 5s)
    # -------------------------
    def emit_metadata():
        while not stop_event.is_set():
            try:
                # ALWAYS fetch fresh metadata
                metadata = get_graph_metadata(driver, session_id, tool_credentials)
                registry_entry["static_infos"] = metadata  # optional caching if needed
                if stop_event.is_set():
                    break
                socketio.emit(
                    "status",
                    {
                        "type": "metadata",
                        "data": metadata,
                        "session_id": session_id
                    },
                    to=sid
                )
            except Exception as e:
                socketio.emit(
                    "status",
                    {
                        "type": "error",
                        "error": f"Metadata error: {e}",
                        "session_id": session_id
                    },
                    to=sid
                )
            socketio.sleep(5)  # emits every 5 seconds

    # -------------------------
    # Relationships loop (on change)
    # -------------------------
    def emit_relationships():
        nonlocal last_rel_hash
        while not stop_event.is_set():
            try:
                relationships = fetch_relationship_summaries()

                # only emit if changed
                new_hash = hash(tuple((r["id"], r["type"], r["color"], r["bgcolor"]) for r in relationships))
                if new_hash != last_rel_hash:
                    if stop_event.is_set():
                        break
                    last_rel_hash = new_hash
                    registry_entry["latest_relationships"] = relationships
                    socketio.emit(
                        "status",
                        {
                            "type": "relationships",
                            "data": relationships,
                            "session_id": session_id
                        },
                        to=sid
                    )

            except Exception as e:
                socketio.emit(
                    "status",
                    {
                        "type": "error",
                        "error": f"Relationships error: {e}",
                        "session_id": session_id
                    },
                    to=sid
                )

            socketio.sleep(2)  # check every 2 seconds for updates

    # -------------------------
    # Relationship graph loop (on change)
    # -------------------------
    def emit_relationship_graph():
        nonlocal last_graph_hash
        while not stop_event.is_set():
            try:
                session_info = _session_store.get(session_id, {})
                relationship_type = primary_rel_type or session_info.get("primary_rel_type")
                graph = _fetch_relationship_graph(driver, session_id, relationship_type=relationship_type, limit=1000)
                new_hash = hash((
                    tuple(sorted(node.get("id") for node in graph["nodes"])),
                    tuple(sorted(edge.get("id") for edge in graph["edges"])),
                ))
                if new_hash != last_graph_hash:
                    if stop_event.is_set():
                        break
                    last_graph_hash = new_hash
                    socketio.emit(
                        "status",
                        {
                            "type": "relationship_graph",
                            "data": graph,
                            "relationship": relationship_type,
                            "session_id": session_id,
                        },
                        to=sid,
                    )
            except Exception as e:
                socketio.emit(
                    "status",
                    {
                        "type": "error",
                        "error": f"Relationship graph error: {e}",
                        "session_id": session_id,
                    },
                    to=sid,
                )
            socketio.sleep(3)

    # -------------------------
    # Start loops as background tasks
    # -------------------------
    socketio.start_background_task(emit_metadata)
    socketio.start_background_task(emit_relationships)
    socketio.start_background_task(emit_relationship_graph)
