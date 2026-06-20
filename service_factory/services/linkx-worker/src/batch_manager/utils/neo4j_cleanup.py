from datetime import datetime
import time

from logger import log_writer


def _parent_session_id(session_id):
    raw = str(session_id or "")
    if "_" not in raw:
        return None
    _, parent = raw.split("_", 1)
    return parent or None


def _session_scope(session_id):
    raw = str(session_id or "")
    candidates = [raw] if raw else []
    parent = _parent_session_id(raw)
    if parent:
        candidates.append(parent)
    return list(dict.fromkeys(candidates))


def clean_existing_session(driver, session_id, log_file=None, batch_size=10000, run_id=None, database=None):
    if not session_id:
        return {
            "status": "skipped_missing_session",
            "deleted_relationships": 0,
            "deleted_nodes": 0,
            "remaining_relationships": 0,
            "remaining_nodes": 0,
        }

    session_ids = _session_scope(session_id)
    batch_prefixes = [f"{sid}_" for sid in session_ids]
    scope = f"run '{run_id}'" if run_id else f"session scope {session_ids}"

    if run_id:
        rel_filter = "r.run_id = $run_id OR a.run_id = $run_id OR b.run_id = $run_id"
        node_filter = "n.run_id = $run_id"
        session_filter = "s.run_id = $run_id OR s.id IN $session_ids OR s.parent_session_id IN $session_ids"
    else:
        rel_filter = """
            r.session_id IN $session_ids
            OR r.parent_session_id IN $session_ids
            OR a.session_id IN $session_ids
            OR b.session_id IN $session_ids
            OR a.parent_session_id IN $session_ids
            OR b.parent_session_id IN $session_ids
        """
        node_filter = """
            n.session_id IN $session_ids
            OR n.parent_session_id IN $session_ids
            OR any(prefix IN $batch_prefixes WHERE coalesce(n.batch_id, '') STARTS WITH prefix)
        """
        session_filter = "s.id IN $session_ids OR s.parent_session_id IN $session_ids"

    if log_file:
        log_writer(log_file, f"[{datetime.now()}] [Info] - Cleaning existing Neo4j data for {scope}")

    def run_count(query):
        with driver.session() as session:
            record = session.run(
                query,
                session_id=str(session_id),
                session_ids=session_ids,
                batch_prefixes=batch_prefixes,
                run_id=run_id,
                limit=int(batch_size),
            ).single()
        return int((record or {}).get("count") or 0)

    def delete_in_batches(query, label):
        total_deleted = 0
        while True:
            deleted = run_count(query)
            if deleted == 0:
                break
            total_deleted += deleted
            if log_file:
                log_writer(
                    log_file,
                    f"[{datetime.now()}] [Info] - Cleaned {total_deleted} existing {label} for {scope}",
                )
        return total_deleted

    rel_delete_query = f"""
        MATCH (a)-[r]->(b)
        WHERE {rel_filter}
        WITH r LIMIT $limit
        DELETE r
        RETURN count(r) AS count
        """
    node_delete_query = f"""
        MATCH (n)
        WHERE {node_filter}
        WITH n LIMIT $limit
        DETACH DELETE n
        RETURN count(n) AS count
        """
    rel_count_query = f"""
        MATCH (a)-[r]->(b)
        WHERE {rel_filter}
        RETURN count(r) AS count
        """
    node_count_query = f"""
        MATCH (n)
        WHERE {node_filter}
        RETURN count(n) AS count
        """

    deleted_relationships = 0
    deleted_nodes = 0
    cleanup_passes = 0
    max_passes = 4
    while True:
        cleanup_passes += 1
        deleted_relationships += delete_in_batches(rel_delete_query, "relationships")
        deleted_nodes += delete_in_batches(node_delete_query, "nodes")

        remaining_relationships = run_count(rel_count_query)
        remaining_nodes = run_count(node_count_query)
        if remaining_relationships == 0 and remaining_nodes == 0:
            break
        if cleanup_passes >= max_passes:
            break
        if log_file:
            log_writer(
                log_file,
                f"[{datetime.now()}] [Info] - Cleanup pass {cleanup_passes} left "
                f"{remaining_relationships} relationships and {remaining_nodes} nodes for {scope}; retrying",
            )
        time.sleep(1)

    with driver.session() as session:
        session.run(
            f"""
            MATCH (s:Session)
            WHERE {session_filter}
            DETACH DELETE s
            """,
            session_id=str(session_id),
            session_ids=session_ids,
            run_id=run_id,
        )

    result = {
        "status": "cleaned" if remaining_nodes == 0 and remaining_relationships == 0 else "residue_detected",
        "session_id": str(session_id),
        "session_ids": session_ids,
        "run_id": run_id,
        "deleted_relationships": deleted_relationships,
        "deleted_nodes": deleted_nodes,
        "remaining_relationships": remaining_relationships,
        "remaining_nodes": remaining_nodes,
        "cleanup_passes": cleanup_passes,
    }

    if log_file:
        log_writer(
            log_file,
            f"[{datetime.now()}] [Info] - Existing Neo4j data for {scope} cleaned "
            f"({deleted_relationships} relationships, {deleted_nodes} nodes; "
            f"remaining: {remaining_relationships} relationships, {remaining_nodes} nodes)",
        )
    return result
