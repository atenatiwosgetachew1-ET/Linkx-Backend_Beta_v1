import os
import time
from globals import _session_store
from batch_manager.utils.graph_status_events import has_active_graph_session_job, latest_graph_metadata_event


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


def _log_graph_status(stage, session_id, sid=None, **details):
    verbose = str(os.getenv("LINKX_GRAPH_STATUS_VERBOSE", "0")).lower() in {"1", "true", "yes", "on"}
    always_log = {
        "metadata_error",
        "metadata_complete",
        "metadata_max_polls_complete",
        "metadata_max_polls_active_reset",
    }
    if not verbose and stage not in always_log:
        return
    parts = [f"[graph_status] {stage}", f"session_id={session_id}"]
    if sid:
        parts.append(f"sid={sid}")
    for key, value in details.items():
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


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
                f"MATCH (n) WHERE {_session_scope_clause('n', include_run=True)} UNWIND keys(n) AS key RETURN DISTINCT key",
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
    metadata_max_cycles = _env_int("LINKX_GRAPH_STATUS_METADATA_MAX_CYCLES", 2)
    metadata_max_polls = _env_int("LINKX_GRAPH_STATUS_METADATA_MAX_POLLS", 60)
    metadata_slow_interval = _env_float("LINKX_GRAPH_STATUS_METADATA_SLOW_INTERVAL", 10)
    metadata_slow_after_changes = _env_int("LINKX_GRAPH_STATUS_METADATA_SLOW_AFTER_CHANGES", 5)
    metadata_event_check_interval = _env_float("LINKX_GRAPH_STATUS_METADATA_EVENT_CHECK_INTERVAL", 1)
    metadata_debounce_seconds = _env_float("LINKX_GRAPH_STATUS_METADATA_DEBOUNCE_SECONDS", 3)
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

    def _metadata_fingerprint(metadata):
        if not isinstance(metadata, dict):
            return metadata
        # Keep comparison stable so equivalent payloads do not cause extra emits.
        keys = (
            "sourceId",
            "database",
            "user",
            "total_nodes",
            "total_relationships",
            "relationship_labels",
            "property_keys",
            "neo4j_version",
            "live_analysis",
        )
        normalized = []
        for key in keys:
            value = metadata.get(key)
            if isinstance(value, list):
                value = tuple(value)
            elif isinstance(value, dict):
                value = tuple(sorted(value.items()))
            normalized.append((key, value))
        return tuple(normalized)

    # -------------------------
    # Metadata loop
    # -------------------------
    def emit_metadata():
        metadata_polls = 0
        unchanged_metadata_cycles = 0
        changed_metadata_emits = 0
        current_metadata_interval = metadata_interval
        last_metadata_fingerprint = None
        last_metadata_fetch_at = None
        last_graph_event_id = 0
        next_fallback_check = 0.0
        pending_graph_event = None
        pending_graph_event_at = None

        while not stop_event.is_set() and not registry_entry.get("metadata_complete"):
            graph_event = latest_graph_metadata_event(session_id, last_graph_event_id)
            if graph_event:
                last_graph_event_id = graph_event.get("event_id") or last_graph_event_id
                registry_entry["last_graph_metadata_event"] = graph_event
                pending_graph_event = graph_event
                pending_graph_event_at = time.monotonic()

            now = time.monotonic()
            event_is_hot = bool(pending_graph_event and pending_graph_event_at is not None and (now - pending_graph_event_at) < metadata_debounce_seconds)
            fetch_due_during_hot_stream = bool(
                last_metadata_fetch_at is None
                or (now - last_metadata_fetch_at) >= current_metadata_interval
            )
            if metadata_polls > 0 and event_is_hot and not fetch_due_during_hot_stream:
                _log_graph_status(
                    "metadata_debounce_wait",
                    session_id,
                    sid=sid,
                    poll=metadata_polls,
                    debounce_seconds=metadata_debounce_seconds,
                    event_id=pending_graph_event.get("event_id") if pending_graph_event else None,
                    remaining_seconds=round(metadata_debounce_seconds - (now - pending_graph_event_at), 2),
                    next_forced_refresh_in=round(current_metadata_interval - (now - last_metadata_fetch_at), 2) if last_metadata_fetch_at is not None else 0,
                )
                socketio.sleep(metadata_event_check_interval)
                continue

            should_fetch_metadata = metadata_polls == 0 or bool(pending_graph_event) or now >= next_fallback_check
            if not should_fetch_metadata:
                socketio.sleep(metadata_event_check_interval)
                continue

            try:
                metadata = get_graph_metadata(driver, session_id, tool_credentials)
                metadata_polls += 1
                last_metadata_fetch_at = time.monotonic()
                registry_entry["static_infos"] = metadata
                fingerprint = _metadata_fingerprint(metadata)
                if stop_event.is_set() or registry_entry.get("metadata_complete"):
                    break

                event_payload = (pending_graph_event or {}).get("payload") or {}
                if pending_graph_event:
                    _log_graph_status(
                        "metadata_event",
                        session_id,
                        sid=sid,
                        poll=metadata_polls,
                        event_id=pending_graph_event.get("event_id"),
                        phase=event_payload.get("phase"),
                    )
                    pending_graph_event = None
                    pending_graph_event_at = None

                if fingerprint != last_metadata_fingerprint:
                    unchanged_metadata_cycles = 0
                    last_metadata_fingerprint = fingerprint
                    changed_metadata_emits += 1
                    if changed_metadata_emits >= metadata_slow_after_changes:
                        current_metadata_interval = metadata_slow_interval
                    _log_graph_status(
                        "metadata_emit",
                        session_id,
                        sid=sid,
                        poll=metadata_polls,
                        total_nodes=metadata.get("total_nodes"),
                        total_relationships=metadata.get("total_relationships"),
                        relationship_labels=len(metadata.get("relationship_labels") or []),
                        changed_emits=changed_metadata_emits,
                        next_interval=current_metadata_interval,
                        event_id=(graph_event or {}).get("event_id"),
                    )
                    socketio.emit(
                        "status",
                        {
                            "type": "metadata",
                            "data": metadata,
                            "session_id": session_id
                        },
                        to=sid
                    )
                elif graph_event:
                    unchanged_metadata_cycles = 0
                    _log_graph_status(
                        "metadata_event_no_change",
                        session_id,
                        sid=sid,
                        poll=metadata_polls,
                        event_id=graph_event.get("event_id"),
                        phase=event_payload.get("phase"),
                        total_nodes=metadata.get("total_nodes"),
                        total_relationships=metadata.get("total_relationships"),
                        changed_emits=changed_metadata_emits,
                    )
                else:
                    unchanged_metadata_cycles += 1
                    active_job = has_active_graph_session_job(session_id)
                    _log_graph_status(
                        "metadata_unchanged",
                        session_id,
                        sid=sid,
                        poll=metadata_polls,
                        unchanged_cycles=unchanged_metadata_cycles,
                        total_nodes=metadata.get("total_nodes"),
                        total_relationships=metadata.get("total_relationships"),
                        changed_emits=changed_metadata_emits,
                        next_interval=current_metadata_interval,
                        active_job=active_job,
                    )
                    if unchanged_metadata_cycles >= metadata_max_cycles:
                        if active_job:
                            unchanged_metadata_cycles = 0
                            _log_graph_status(
                                "metadata_wait_active_job",
                                session_id,
                                sid=sid,
                                poll=metadata_polls,
                                changed_emits=changed_metadata_emits,
                                next_interval=current_metadata_interval,
                            )
                        else:
                            registry_entry["metadata_complete"] = True
                            _log_graph_status(
                                "metadata_complete",
                                session_id,
                                sid=sid,
                                poll=metadata_polls,
                                unchanged_cycles=unchanged_metadata_cycles,
                                changed_emits=changed_metadata_emits,
                            )
                            break

                next_fallback_check = time.monotonic() + current_metadata_interval
            except Exception as e:
                metadata_polls += 1
                _log_graph_status("metadata_error", session_id, sid=sid, error=str(e))
                socketio.emit(
                    "status",
                    {
                        "type": "error",
                        "error": f"Metadata error: {e}",
                        "session_id": session_id
                    },
                    to=sid
                )
                next_fallback_check = time.monotonic() + current_metadata_interval

            if not registry_entry.get("metadata_complete") and metadata_polls >= metadata_max_polls:
                active_job = has_active_graph_session_job(session_id)
                if active_job:
                    _log_graph_status(
                        "metadata_max_polls_active_reset",
                        session_id,
                        sid=sid,
                        poll=metadata_polls,
                        changed_emits=changed_metadata_emits,
                    )
                    metadata_polls = 0
                else:
                    registry_entry["metadata_complete"] = True
                    _log_graph_status(
                        "metadata_max_polls_complete",
                        session_id,
                        sid=sid,
                        poll=metadata_polls,
                        changed_emits=changed_metadata_emits,
                    )
                    break

            if not registry_entry.get("metadata_complete") and not stop_event.is_set():
                socketio.sleep(metadata_event_check_interval)
        registry_entry["metadata_complete"] = True

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

                if new_hash != last_rel_hash:
                    if stop_event.is_set():
                        break
                    last_rel_hash = new_hash
                    unchanged_relationship_cycles = 0
                    registry_entry["latest_relationships"] = relationships
                    _log_graph_status(
                        "relationships_emit",
                        session_id,
                        sid=sid,
                        relationship_count=len(relationships),
                        unchanged_cycles=unchanged_relationship_cycles,
                    )
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
