import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path


LEGACY_DIRS = {
    "uploads": Path("public/temp_uploads"),
    "dfparts": Path("public/temp_dfParts"),
    "logs": Path("public/temp_logs"),
    "rules": Path("public/temp_rules"),
    "configs": Path("public/temp_config"),
    "graphs": Path("public/temp_graphs"),
    "reports": Path("public/temp_reports"),
}

RETENTION_ENV = {
    "upload": "LINKX_UPLOAD_RETENTION_DAYS",
    "uploads": "LINKX_UPLOAD_RETENTION_DAYS",
    "dfpart": "LINKX_DFPART_RETENTION_DAYS",
    "dfparts": "LINKX_DFPART_RETENTION_DAYS",
    "log": "LINKX_LOG_RETENTION_DAYS",
    "logs": "LINKX_LOG_RETENTION_DAYS",
    "rule": "LINKX_CONFIG_RETENTION_DAYS",
    "rules": "LINKX_CONFIG_RETENTION_DAYS",
    "config": "LINKX_CONFIG_RETENTION_DAYS",
    "configs": "LINKX_CONFIG_RETENTION_DAYS",
    "graph": "LINKX_GRAPH_RETENTION_DAYS",
    "graphs": "LINKX_GRAPH_RETENTION_DAYS",
    "report": "LINKX_REPORT_RETENTION_DAYS",
    "reports": "LINKX_REPORT_RETENTION_DAYS",
}

DEFAULT_RETENTION_DAYS = {
    "upload": 7,
    "uploads": 7,
    "dfpart": 3,
    "dfparts": 3,
    "log": 30,
    "logs": 30,
    "rule": 7,
    "rules": 7,
    "config": 7,
    "configs": 7,
    "graph": 14,
    "graphs": 14,
    "report": 30,
    "reports": 30,
}


def configured_artifact_root():
    root = os.getenv("LINKX_ARTIFACT_ROOT")
    return Path(root).resolve() if root else None


def artifact_base(kind):
    kind = str(kind)
    root = configured_artifact_root()
    if root:
        return root / kind
    return LEGACY_DIRS.get(kind, Path("public") / f"temp_{kind}")


def ensure_artifact_dir(kind, *parts):
    path = artifact_base(kind)
    for part in parts:
        if part is not None and str(part) != "":
            path = path / str(part)
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def session_artifact_dir(kind, session_id=None):
    return ensure_artifact_dir(kind, session_id) if session_id else ensure_artifact_dir(kind)


def artifact_retention_days(artifact_type):
    artifact_type = str(artifact_type or "")
    env_name = RETENTION_ENV.get(artifact_type)
    if env_name and os.getenv(env_name):
        return int(os.getenv(env_name))
    return int(DEFAULT_RETENTION_DAYS.get(artifact_type, 7))


def artifact_expires_at(artifact_type):
    days = artifact_retention_days(artifact_type)
    if days <= 0:
        return None
    return datetime.now(timezone.utc) + timedelta(days=days)


def _database_url():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def register_artifact(path, artifact_type, session_id=None, job_id=None, filename=None, metadata=None, expires_at=None):
    root = configured_artifact_root()
    if not root:
        return None

    dsn = _database_url()
    if not dsn:
        return None

    artifact_path = Path(path).resolve()
    try:
        artifact_path.relative_to(root)
    except ValueError:
        return None

    if expires_at is None:
        expires_at = artifact_expires_at(artifact_type)

    try:
        import psycopg
        size_bytes = artifact_path.stat().st_size if artifact_path.is_file() else None
        with psycopg.connect(dsn, application_name="linkx-artifact-register") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO artifacts (
                        session_id, job_id, artifact_type, storage_backend, storage_uri,
                        filename, size_bytes, expires_at, metadata
                    )
                    VALUES (%s, %s, %s, 'filesystem', %s, %s, %s, %s, %s::jsonb)
                    RETURNING id::text
                    """,
                    (
                        str(session_id) if session_id else None,
                        job_id,
                        artifact_type,
                        str(artifact_path),
                        filename or artifact_path.name,
                        size_bytes,
                        expires_at,
                        json.dumps(metadata or {}),
                    ),
                )
                artifact_id = cur.fetchone()[0]
            conn.commit()
        return artifact_id
    except Exception as exc:
        print(f"[artifact] registration skipped for {artifact_path}: {exc}")
        return None


def register_artifact_dir(path, artifact_type, session_id=None, job_id=None, metadata=None, expires_at=None):
    return register_artifact(
        path,
        artifact_type,
        session_id=session_id,
        job_id=job_id,
        filename=Path(path).name,
        metadata=metadata,
        expires_at=expires_at,
    )
