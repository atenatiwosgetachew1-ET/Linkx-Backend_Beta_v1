import os
import time
from globals import _session_store


def _env_float(name, default):
    try:
        return max(0.1, float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return float(default)


def _env_int(name, default):
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return int(default)


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
        "live_analysis": _session_store.get(session_id, {}).get("live_analysis"),
    }


def _fetch_relationship_graph(driver, session_id, relationship_type=None, limit=None):
    if limit is None:
        limit = _env_int("LINKX_GRAPH_STATUS_GRAPH_LIMIT", 5000)
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
    Stream graph metadata and lightweight relationship updates without competing too hard with ingestion.
    """

    metadata_interval = _env_float("LINKX_GRAPH_STATUS_METADATA_INTERVAL", 2)
    relationships_active_interval = _env_float("LINKX_GRAPH_STATUS_RELATIONSHIPS_ACTIVE_INTERVAL", 3)
    relationships_idle_interval = _env_float("LINKX_GRAPH_STATUS_RELATIONSHIPS_IDLE_INTERVAL", 10)
    relationships_idle_after_cycles = _env_int("LINKX_GRAPH_STATUS_RELATIONSHIPS_IDLE_AFTER_CYCLES", 2)

    stop_event = registry_entry["stop_event"]
    driver = registry_entry["driver"]
    tool_credentials = registry_entry["tool_credentials"]
    registry_entry["latest_relationships"] = []
    registry_entry["metadata_complete"] = False
    last_rel_hash = None
    unchanged_relationship_cycles = 0

    # -------------------------
    # Metadata loop
    # -------------------------
    def emit_metadata():
        while not stop_event.is_set() and not registry_entry.get("metadata_complete"):
            try:
                metadata = get_graph_metadata(driver, session_id, tool_credentials)
                registry_entry["static_infos"] = metadata
                if stop_event.is_set() or registry_entry.get("metadata_complete"):
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
            socketio.sleep(metadata_interval)

    # -------------------------
    # Relationships loop (on change)
    # -------------------------
    def emit_relationships():
        nonlocal last_rel_hash, unchanged_relationship_cycles
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
                        WITH type, rep, properties(rep) AS props
                        RETURN
                            type,
                            elementId(rep) AS id,
                            coalesce(props.color, '#333') AS color,
                            coalesce(props.bgcolor, '#DDD') AS bgcolor
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
                if relationships:
                    registry_entry["metadata_complete"] = True

                if new_hash != last_rel_hash:
                    if stop_event.is_set():
                        break
                    last_rel_hash = new_hash
                    unchanged_relationship_cycles = 0
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
                else:
                    unchanged_relationship_cycles += 1

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

            if unchanged_relationship_cycles >= relationships_idle_after_cycles:
                socketio.sleep(relationships_idle_interval)
            else:
                socketio.sleep(relationships_active_interval)

    # -------------------------
    # Start loops as background tasks
    # -------------------------
    socketio.start_background_task(emit_metadata)
    socketio.start_background_task(emit_relationships)
