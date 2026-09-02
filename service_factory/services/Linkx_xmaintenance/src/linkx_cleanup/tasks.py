import json
import os
import time
from datetime import datetime, timezone

from batch_manager.utils.neo4j_cleanup import clean_existing_session
from batch_manager.utils.neo4j_utils import (
    Neo4jCredentialConfigError,
    create_neo4j_driver,
    neo4j_database_name,
    neo4j_credential_source,
    resolve_neo4j_credentials,
)
from linkx_cleanup.artifacts import artifact_root, delete_filesystem_artifact
from linkx_cleanup.db import connect


def _json(value):
    return json.dumps(value or {})


def _neo4j_credentials(payload=None):
    payload = payload or {}
    return {
        "url": payload.get("neo4j_url") or payload.get("url") or os.getenv("LINKX_NEO4J_URL") or os.getenv("LINKX_CLEANUP_NEO4J_URL"),
        "username": payload.get("neo4j_username") or payload.get("username") or os.getenv("LINKX_NEO4J_USERNAME") or os.getenv("LINKX_CLEANUP_NEO4J_USERNAME"),
        "password": payload.get("neo4j_password") or payload.get("password") or os.getenv("LINKX_NEO4J_PASSWORD") or os.getenv("LINKX_CLEANUP_NEO4J_PASSWORD"),
        "password_ref": payload.get("neo4j_password_ref") or payload.get("password_ref"),
        "database": payload.get("neo4j_database") or payload.get("database") or os.getenv("LINKX_NEO4J_DATABASE") or os.getenv("LINKX_CLEANUP_NEO4J_DATABASE"),
    }


def _is_retryable_neo4j_connect_error(exc):
    code = getattr(exc, "code", None) or getattr(exc, "neo4j_code", None)
    text = f"{code or ''} {exc}"
    non_retryable_markers = (
        "AuthenticationRateLimit",
        "Authentication",
        "Unauthorized",
        "Security.",
    )
    if any(marker in text for marker in non_retryable_markers):
        return False
    retryable_markers = (
        "ServiceUnavailable",
        "SessionExpired",
        "DatabaseUnavailable",
        "Connection refused",
        "Connection reset",
        "timed out",
        "Temporary",
        "TransientError",
    )
    return any(marker in text for marker in retryable_markers)


def _create_neo4j_driver_with_retry(creds, payload=None):
    payload = payload or {}
    attempts = int(payload.get("neo4j_retry_attempts") or os.getenv("LINKX_NEO4J_RETRY_ATTEMPTS", "6"))
    delay = float(payload.get("neo4j_retry_delay_seconds") or os.getenv("LINKX_NEO4J_RETRY_DELAY_SECONDS", "5"))
    resolved = resolve_neo4j_credentials(creds)
    print(
        "[cleanup] Neo4j credential source "
        f"source={neo4j_credential_source(resolved)} "
        f"database={neo4j_database_name(resolved) or 'default'} "
        f"password_ref={'present' if resolved.get('password_ref') or resolved.get('neo4j_password_ref') else 'missing'}",
        flush=True,
    )
    last_error = None
    for attempt in range(1, max(1, attempts) + 1):
        driver = create_neo4j_driver(resolved)
        try:
            with driver.session(database=neo4j_database_name(resolved)) as session:
                session.run("RETURN 1 AS ok").single()
            return driver
        except Exception as exc:
            last_error = exc
            driver.close()
            if attempt >= attempts or not _is_retryable_neo4j_connect_error(exc):
                break
            print(f"[cleanup] Neo4j not ready attempt={attempt}/{attempts}: {exc}", flush=True)
            time.sleep(delay)
    raise last_error


def cleanup_neo4j_session(session_id, run_id=None, batch_size=10000, dry_run=False, payload=None):
    if dry_run:
        return {"neo4j": "dry_run", "session_id": session_id, "run_id": run_id}
    creds = _neo4j_credentials(payload)
    if not creds["url"] or not creds["username"] or not (creds.get("password") or creds.get("password_ref")):
        return {"neo4j": "skipped_missing_credentials", "session_id": session_id, "run_id": run_id}
    try:
        driver = _create_neo4j_driver_with_retry(creds, payload=payload)
    except Neo4jCredentialConfigError as exc:
        return {"neo4j": "invalid_credentials", "session_id": session_id, "run_id": run_id, "error": str(exc)}
    try:
        result = clean_existing_session(driver, session_id, batch_size=batch_size, run_id=run_id)
    finally:
        driver.close()
    return {"neo4j": result.get("status", "cleaned"), "database": neo4j_database_name(creds), **result}


def _split_window_session(session_id):
    raw = str(session_id or "")
    if "_" not in raw:
        return raw, ""
    window_id, base_session = raw.split("_", 1)
    return base_session or raw, window_id


def cleanup_filesystem_footprint(session_id, dry_run=False):
    root = artifact_root()
    session_id = str(session_id or "")
    candidates = [
        root / "dfparts" / f"merged_dfpart_{session_id}",
        root / "uploads" / session_id,
        root / "rules" / session_id,
        root / "graphs" / session_id,
        root / "reports" / session_id,
        root / "configs" / f"{session_id}_temp_config.json",
        root / "configs" / session_id,
    ]
    candidates.extend((root / "logs").glob(f"logfile_{session_id}_*.log"))
    candidates.extend((root / "logs").glob(f"logfile_{session_id}_[*].log"))

    deleted = []
    skipped = []
    seen = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            if candidate.exists():
                deleted.append(delete_filesystem_artifact(str(candidate), dry_run=dry_run))
            else:
                skipped.append({"path": str(candidate), "exists": False})
        except Exception as exc:
            skipped.append({"path": str(candidate), "error": str(exc)})
    return {"deleted": deleted, "skipped": skipped, "count": len(deleted), "dry_run": dry_run}


def cleanup_session_config(session_id, dry_run=False):
    base_session, window_id = _split_window_session(session_id)
    if dry_run:
        return {"session_id": str(session_id), "base_session": base_session, "window_id": window_id, "dry_run": True}
    with connect() as conn:
        with conn.cursor() as cur:
            if window_id:
                cur.execute(
                    "DELETE FROM session_configs WHERE (session_id = %s AND window_id = %s) OR session_id = %s",
                    (base_session, window_id, str(session_id)),
                )
            else:
                cur.execute(
                    """
                    UPDATE session_configs
                    SET config = config
                        - 'active_source_type'
                        - 'active_source_mode'
                        - 'active_REST_API'
                        - 'active_kafka_adress'
                        - 'active_kafka_topic'
                        - 'active_storage_address'
                        - 'tool_credentials'
                        - 'tool'
                        || '{"dataframe_ready": false}'::jsonb,
                        updated_at = NOW()
                    WHERE session_id = %s
                    """,
                    (str(session_id),),
                )
            affected = cur.rowcount
        conn.commit()
    return {"session_id": str(session_id), "base_session": base_session, "window_id": window_id, "rows_changed": affected}


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


def _mark_session_status(session_id, status):
    if not session_id:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE analysis_sessions SET status = %s, last_seen_at = NOW() WHERE session_id = %s",
                (status, str(session_id)),
            )
            affected = cur.rowcount
        conn.commit()
    return affected


def cleanup_session(session_id, run_id=None, dry_run=False, payload=None, mark_status="cleaned"):
    results = {"session_id": session_id, "run_id": run_id, "dry_run": dry_run}
    if not dry_run:
        results["status_before_cleanup"] = _mark_session_status(session_id, "cancelling")
    results["artifacts"] = cleanup_artifacts(session_id=session_id, dry_run=dry_run)
    results["filesystem"] = cleanup_filesystem_footprint(session_id, dry_run=dry_run)
    results["neo4j"] = cleanup_neo4j_session(session_id, run_id=run_id, dry_run=dry_run, payload=payload)
    results["session_config"] = cleanup_session_config(session_id, dry_run=dry_run)
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


def scan_neo4j_residue(payload=None, dry_run=False):
    payload = payload or {}
    limit = int(payload.get("sample_limit", 25))
    include_unmanaged = str(payload.get("include_unmanaged", "true")).lower() in {"1", "true", "yes", "on"}
    creds = _neo4j_credentials(payload)
    if not creds["url"] or not creds["username"] or not creds["password"]:
        return {"neo4j": "skipped_missing_credentials", "dry_run": dry_run}

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT session_id
                FROM analysis_sessions
                WHERE status IN ('cleaned', 'cancelled', 'expired', 'failed')
                ORDER BY COALESCE(ended_at, last_seen_at, created_at) DESC
                LIMIT %s
                """,
                (int(payload.get("cleaned_session_limit", 500)),),
            )
            inactive_sessions = [row[0] for row in cur.fetchall()]

    driver = _create_neo4j_driver_with_retry(creds, payload=payload)
    try:
        with driver.session(database=neo4j_database_name(creds)) as session:
            unmanaged_nodes = session.run(
                """
                MATCH (n)
                WHERE coalesce(n.linkx_managed, false) = false
                  AND n.session_id IS NULL
                  AND n.parent_session_id IS NULL
                  AND n.run_id IS NULL
                  AND n.batch_id IS NULL
                  AND NOT n:Session
                RETURN count(n) AS count
                """
            ).single()["count"] if include_unmanaged else 0
            unmanaged_relationships = session.run(
                """
                MATCH ()-[r]->()
                WHERE coalesce(r.linkx_managed, false) = false
                  AND r.session_id IS NULL
                  AND r.parent_session_id IS NULL
                  AND r.run_id IS NULL
                  AND r.batch_id IS NULL
                RETURN count(r) AS count
                """
            ).single()["count"] if include_unmanaged else 0
            active_residue_nodes = session.run(
                """
                MATCH (n)
                WHERE coalesce(n.linkx_managed, false) = true
                  AND n.session_id IN $inactive_sessions
                RETURN count(n) AS count
                """,
                inactive_sessions=inactive_sessions,
            ).single()["count"] if inactive_sessions else 0
            active_residue_relationships = session.run(
                """
                MATCH ()-[r]->()
                WHERE coalesce(r.linkx_managed, false) = true
                  AND r.session_id IN $inactive_sessions
                RETURN count(r) AS count
                """,
                inactive_sessions=inactive_sessions,
            ).single()["count"] if inactive_sessions else 0
            unmanaged_node_samples = session.run(
                """
                MATCH (n)
                WHERE coalesce(n.linkx_managed, false) = false
                  AND n.session_id IS NULL
                  AND n.parent_session_id IS NULL
                  AND n.run_id IS NULL
                  AND n.batch_id IS NULL
                  AND NOT n:Session
                RETURN labels(n) AS labels, properties(n) AS properties
                LIMIT $limit
                """,
                limit=limit,
            ).data() if include_unmanaged and limit > 0 else []
            inactive_session_samples = session.run(
                """
                MATCH (n)
                WHERE coalesce(n.linkx_managed, false) = true
                  AND n.session_id IN $inactive_sessions
                RETURN n.session_id AS session_id, labels(n) AS labels, properties(n) AS properties
                LIMIT $limit
                """,
                inactive_sessions=inactive_sessions,
                limit=limit,
            ).data() if inactive_sessions and limit > 0 else []
    finally:
        driver.close()

    return {
        "dry_run": dry_run,
        "database": neo4j_database_name(creds),
        "inactive_sessions_checked": len(inactive_sessions),
        "inactive_sessions": inactive_sessions[:limit],
        "unmanaged": {
            "nodes": int(unmanaged_nodes),
            "relationships": int(unmanaged_relationships),
            "samples": unmanaged_node_samples,
        },
        "inactive_session_residue": {
            "nodes": int(active_residue_nodes),
            "relationships": int(active_residue_relationships),
            "samples": inactive_session_samples,
        },
        "status": "residue_detected" if any([unmanaged_nodes, unmanaged_relationships, active_residue_nodes, active_residue_relationships]) else "clean",
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
        # Session-scoped graph cleanup must ignore run_id. Non-final terminate/cancel
        # uses this path to remove Store Data, Source/Target, and Link Analysis
        # graph residue while preserving dfparts/uploads/session config.
        return cleanup_neo4j_session(payload.get("session_id"), run_id=None, batch_size=int(payload.get("batch_size", 10000)), dry_run=dry_run, payload=payload)
    if cleanup_type == "metadata_prune":
        return prune_cleaned_metadata(retention_days=int(payload.get("retention_days", 30)), dry_run=dry_run)
    if cleanup_type == "neo4j_residue_scan":
        return scan_neo4j_residue(payload=payload, dry_run=dry_run)
    raise ValueError(f"unsupported_cleanup_type:{cleanup_type}")
