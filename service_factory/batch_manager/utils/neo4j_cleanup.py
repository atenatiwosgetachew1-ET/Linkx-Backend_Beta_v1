from datetime import datetime

from logger import log_writer


def clean_existing_session(driver, session_id, log_file=None, batch_size=10000, run_id=None):
    if not session_id:
        return

    scope = f"run '{run_id}'" if run_id else f"session '{session_id}'"
    rel_match = "MATCH (a)-[r]->(b)" if run_id else "MATCH ()-[r]->()"
    rel_filter = "r.run_id = $run_id OR a.run_id = $run_id OR b.run_id = $run_id" if run_id else "r.session_id = $session_id"
    node_filter = "n.run_id = $run_id" if run_id else "n.session_id = $session_id OR n.batch_id STARTS WITH $batch_prefix"
    session_filter = "s.id = $session_id AND s.run_id = $run_id" if run_id else "s.id = $session_id"

    if log_file:
        log_writer(log_file, f"[{datetime.now()}] [Info] - Cleaning existing Neo4j data for {scope}")

    def delete_in_batches(query, count_key, label):
        total_deleted = 0
        while True:
            with driver.session() as session:
                record = session.run(
                    query,
                    session_id=session_id,
                    batch_prefix=f"{session_id}_",
                    run_id=run_id,
                    limit=int(batch_size),
                ).single()
            deleted = int(record[count_key] or 0) if record else 0
            if deleted == 0:
                break
            total_deleted += deleted
            if log_file:
                log_writer(
                    log_file,
                    f"[{datetime.now()}] [Info] - Cleaned {total_deleted} existing {label} for {scope}",
                )
        return total_deleted

    deleted_relationships = delete_in_batches(
        f"""
        {rel_match}
        WHERE {rel_filter}
        WITH r LIMIT $limit
        DELETE r
        RETURN count(r) AS deleted
        """,
        "deleted",
        "relationships",
    )
    deleted_nodes = delete_in_batches(
        f"""
        MATCH (n)
        WHERE {node_filter}
        WITH n LIMIT $limit
        DETACH DELETE n
        RETURN count(n) AS deleted
        """,
        "deleted",
        "nodes",
    )

    with driver.session() as session:
        session.run(
            f"""
            MATCH (s:Session)
            WHERE {session_filter}
            DETACH DELETE s
            """,
            session_id=session_id,
            run_id=run_id,
        )

    if log_file:
        log_writer(
            log_file,
            f"[{datetime.now()}] [Info] - Existing Neo4j data for {scope} cleaned "
            f"({deleted_relationships} relationships, {deleted_nodes} nodes)",
        )
