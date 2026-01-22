import time

def get_graph_metadata(driver, session_id, tool_credentials=None):
    with driver.session() as session:
        # Database info
        db_info_record = session.run("CALL db.info()").single()
        database_name = db_info_record["name"] if db_info_record else None

        # User
        username = tool_credentials.get("username") if tool_credentials else None

        # Nodes tied to session
        total_nodes_record = session.run(
            "MATCH (n) WHERE n.batch_id STARTS WITH $session_id RETURN count(n) AS total_nodes",
            session_id=session_id
        ).single()
        total_nodes = total_nodes_record["total_nodes"] if total_nodes_record else 0

        # Relationships tied to session
        total_relationships_record = session.run(
            "MATCH ()-[r]->() WHERE r.session_id = $session_id RETURN count(r) AS total_relationships",
            session_id=session_id
        ).single()
        total_relationships = total_relationships_record["total_relationships"] if total_relationships_record else 0

        # Relationship labels
        relationship_labels_record = session.run(
            "MATCH ()-[r]->() WHERE r.session_id = $session_id RETURN COLLECT(DISTINCT type(r)) AS labels",
            session_id=session_id
        ).single()
        relationship_labels = relationship_labels_record["labels"] if relationship_labels_record else []

        # Property keys tied to session
        property_keys = [
            record["key"]
            for record in session.run(
                "MATCH (n) WHERE n.batch_id STARTS WITH $session_id UNWIND keys(n) AS key RETURN DISTINCT key",
                session_id=session_id
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
    }


def graph_status_stream(socketio, sid, session_id, registry_entry, node_label=None, primary_rel_type=None):
    """
    Stream metadata every 5 seconds and relationships only when changed.
    """

    stop_event = registry_entry["stop_event"]
    driver = registry_entry["driver"]
    tool_credentials = registry_entry["tool_credentials"]
    registry_entry["latest_relationships"] = []
    last_rel_hash = None

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
                with driver.session() as session:
                    result = session.run(
                        """
                        MATCH ()-[r]->()
                        WHERE r.session_id = $session_id
                        WITH
                            type(r) AS type,
                            collect(r) AS rels
                        WITH
                            type,
                            rels[0] AS rep
                        RETURN
                            type,
                            elementId(rep) AS id,
                            coalesce(rep.color, '#333') AS color,
                            coalesce(rep.bgcolor, '#DDD') AS bgcolor
                        ORDER BY type
                        """,
                        session_id=session_id
                    )

                    relationships = [{
                        "id": r["id"],
                        "type": r["type"],
                        "color": r["color"] or "#333",
                        "bgcolor": r["bgcolor"] or "#DDD",
                    } for r in result]

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
    # Start both loops as background tasks
    # -------------------------
    socketio.start_background_task(emit_metadata)
    socketio.start_background_task(emit_relationships)
