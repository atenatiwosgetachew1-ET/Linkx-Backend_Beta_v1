import json
import os
from datetime import datetime, timezone

from batch_manager.utils.neo4j_cleanup import clean_existing_session
from batch_manager.utils.neo4j_utils import create_neo4j_driver, neo4j_database_name
from linkx_cleanup.artifacts import delete_filesystem_artifact
from linkx_cleanup.db import connect


def _json(value):
    return json.dumps(value or {})


def _neo4j_credentials(payload=None):
    payload = payload or {}
    return {
        "url": payload.get("neo4j_url") or payload.get("url") or os.getenv("LINKX_NEO4J_URL") or os.getenv("LINKX_CLEANUP_NEO4J_URL"),
        "username": payload.get("neo4j_username") or payload.get("username") or os.getenv("LINKX_NEO4J_USERNAME") or os.getenv("LINKX_CLEANUP_NEO4J_USERNAME"),
        "password": payload.get("neo4j_password") or payload.get("password") or os.getenv("LINKX_NEO4J_PASSWORD") or os.getenv("LINKX_CLEANUP_NEO4J_PASSWORD"),
        "database": payload.get("neo4j_database") or payload.get("database") or os.getenv("LINKX_NEO4J_DATABASE") or os.getenv("LINKX_CLEANUP_NEO4J_DATABASE"),
    }


def cleanup_neo4j_session(session_id, run_id=None, batch_size=10000, dry_run=False, payload=None):
    if dry_run:
        return {"neo4j": "dry_run", "session_id": session_id, "run_id": run_id}
    creds = _neo4j_credentials(payload)
    if not creds["url"] or not creds["username"] or not creds["password"]:
        return {"neo4j": "skipped_missing_credentials", "session_id": session_id, "run_id": run_id}
    driver = create_neo4j_driver(creds)
    try:
        clean_existing_session(driver, session_id, batch_size=batch_size, run_id=run_id)
    finally:
        driver.close()
    return {"neo4j": "cleaned", "session_id": session_id, "run_id": run_id, "database": neo4j_database_name(creds)}


def cleanup_artifacts(session_id=None, artifact_ids=None, expired_only=False, dry_run=False, limit=500):
    artifact_ids = artifact_ids or []
    deleted = []
    skipped = []
    with connect() as conn:
        with conn.cursor() as cur:
            filters = ["delete_status = 'active'"]
            params = []
            if session_id:
                filters.append("session_id = %s")
                params.append(str(session_id))
            if artifact_ids:
                filters.append("id = ANY(%s::uuid[])")
                params.append(artifact_ids)
            if expired_only:
                filters.append("expires_at IS NOT NULL AND expires_at <= NOW()")
            where = " AND ".join(filters)
            cur.execute(
                f"""
                SELECT id::text, storage_backend, storage_uri
                FROM artifacts
                WHERE {where}
                ORDER BY created_at ASC
                LIMIT %s
                """,
                [*params, int(limit)],
            )
            rows = cur.fetchall()
            for artifact_id, backend, uri in rows:
                try:
                    if backend in {"filesystem", "local", "nfs"}:
                        result = delete_filesystem_artifact(uri, dry_run=dry_run)
                    else:
                        result = {"backend": backend, "uri": uri, "deleted": False, "reason": "unsupported_backend"}
                    deleted.append({"artifact_id": artifact_id, **result})
                    if not dry_run:
                        cur.execute(
                            "UPDATE artifacts SET delete_status = 'deleted', deleted_at = NOW() WHERE id = %s",
                            (artifact_id,),
                        )
                except Exception as exc:
                    skipped.append({"artifact_id": artifact_id, "error": str(exc)})
                    cur.execute(
                        "UPDATE artifacts SET delete_status = 'failed' WHERE id = %s",
                        (artifact_id,),
                    )
            conn.commit()
    return {"deleted": deleted, "skipped": skipped, "count": len(deleted), "dry_run": dry_run}


def prune_cleaned_metadata(retention_days=30, dry_run=False):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM artifacts WHERE delete_status = 'deleted' AND deleted_at < NOW() - (%s || ' days')::interval",
                (int(retention_days),),
            )
            artifact_count = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM jobs WHERE status IN ('succeeded','failed','cancelled') AND finished_at < NOW() - (%s || ' days')::interval",
                (int(retention_days),),
            )
            job_count = cur.fetchone()[0]
            if not dry_run:
                cur.execute("DELETE FROM artifacts WHERE delete_status = 'deleted' AND deleted_at < NOW() - (%s || ' days')::interval", (int(retention_days),))
                cur.execute("DELETE FROM jobs WHERE status IN ('succeeded','failed','cancelled') AND finished_at < NOW() - (%s || ' days')::interval", (int(retention_days),))
            conn.commit()
    return {"artifacts": artifact_count, "jobs": job_count, "dry_run": dry_run}


def _child_sessions(parent_session_id):
    if not parent_session_id:
        return []
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT session_id
                FROM analysis_sessions
                WHERE parent_session_id = %s
                ORDER BY created_at ASC
                """,
                (str(parent_session_id),),
            )
            return [row[0] for row in cur.fetchall()]


def cleanup_session(session_id, run_id=None, dry_run=False, payload=None, mark_status="cleaned"):
    results = {"session_id": session_id, "run_id": run_id, "dry_run": dry_run}
    results["artifacts"] = cleanup_artifacts(session_id=session_id, dry_run=dry_run)
    results["neo4j"] = cleanup_neo4j_session(session_id, run_id=run_id, dry_run=dry_run, payload=payload)
    if not dry_run:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE analysis_sessions SET status = %s, ended_at = COALESCE(ended_at, NOW()) WHERE session_id = %s",
                    (mark_status, str(session_id)),
                )
            conn.commit()
    return results


def cleanup_session_tree(session_id, dry_run=False, payload=None):
    session_ids = [str(session_id), *_child_sessions(session_id)]
    return {
        "session_id": session_id,
        "children": session_ids[1:],
        "results": [cleanup_session(sid, dry_run=dry_run, payload=payload) for sid in session_ids],
        "dry_run": dry_run,
    }


def cleanup_run(session_id, run_id, dry_run=False, payload=None):
    results = {"session_id": session_id, "run_id": run_id, "dry_run": dry_run}
    results["neo4j"] = cleanup_neo4j_session(session_id, run_id=run_id, dry_run=dry_run, payload=payload)
    return results


def cleanup_abandoned(retention_minutes=360, dry_run=False, payload=None):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT session_id
                FROM analysis_sessions
                WHERE status IN ('active', 'cancel_requested')
                  AND parent_session_id IS NULL
                  AND last_seen_at < NOW() - (%s || ' minutes')::interval
                ORDER BY last_seen_at ASC
                LIMIT %s
                """,
                (int(retention_minutes), int((payload or {}).get("limit", 100))),
            )
            stale_sessions = [row[0] for row in cur.fetchall()]
    return {
        "retention_minutes": int(retention_minutes),
        "sessions": stale_sessions,
        "results": [cleanup_session_tree(sid, dry_run=dry_run, payload=payload) for sid in stale_sessions],
        "dry_run": dry_run,
    }


def run_cleanup(cleanup_type, payload=None, dry_run=False):
    payload = payload or {}
    if cleanup_type in {"session", "window"}:
        return cleanup_session(payload.get("session_id"), run_id=payload.get("run_id"), dry_run=dry_run, payload=payload)
    if cleanup_type == "session_tree":
        return cleanup_session_tree(payload.get("session_id"), dry_run=dry_run, payload=payload)
    if cleanup_type == "run":
        return cleanup_run(payload.get("session_id"), payload.get("run_id"), dry_run=dry_run, payload=payload)
    if cleanup_type == "abandoned_sessions":
        return cleanup_abandoned(retention_minutes=int(payload.get("retention_minutes", 360)), dry_run=dry_run, payload=payload)
    if cleanup_type == "artifacts_expired":
        return cleanup_artifacts(expired_only=True, dry_run=dry_run, limit=int(payload.get("limit", 500)))
    if cleanup_type == "artifacts_session":
        return cleanup_artifacts(session_id=payload.get("session_id"), dry_run=dry_run, limit=int(payload.get("limit", 500)))
    if cleanup_type == "neo4j_session":
        return cleanup_neo4j_session(payload.get("session_id"), run_id=payload.get("run_id"), batch_size=int(payload.get("batch_size", 10000)), dry_run=dry_run, payload=payload)
    if cleanup_type == "metadata_prune":
        return prune_cleaned_metadata(retention_days=int(payload.get("retention_days", 30)), dry_run=dry_run)
    raise ValueError(f"unsupported_cleanup_type:{cleanup_type}")
