import json
import os
from datetime import datetime


def _dsn():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def db_enabled():
    return bool(_dsn())


def _connect(application_name="linkx-config-store"):
    import psycopg

    dsn = _dsn()
    if not dsn:
        raise RuntimeError("DATABASE_URL or LINKX_POSTGRES_DSN is required")
    return psycopg.connect(dsn, application_name=application_name)


def ensure_schema():
    if not db_enabled():
        return False
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_configs (
                    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                    config JSONB NOT NULL DEFAULT '{}'::jsonb,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS session_configs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    session_id TEXT NOT NULL REFERENCES analysis_sessions(session_id) ON DELETE CASCADE,
                    user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
                    service_account_id BIGINT REFERENCES service_accounts(id) ON DELETE SET NULL,
                    window_id TEXT NOT NULL DEFAULT '',
                    config JSONB NOT NULL DEFAULT '{}'::jsonb,
                    source_config_id UUID REFERENCES session_configs(id) ON DELETE SET NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (session_id, window_id)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_session_configs_user ON session_configs(user_id, updated_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_session_configs_session ON session_configs(session_id, window_id)")
        conn.commit()
    return True


def _actor_ids(actor=None):
    actor = actor or {}
    if actor.get("actor_type") == "service":
        return None, actor.get("id")
    return actor.get("id"), None


def get_user_config(user_id, default_config=None):
    if not user_id or not db_enabled():
        return default_config or {}
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT config FROM user_configs WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            if row:
                return row[0] or {}
            if default_config is None:
                return {}
            cur.execute(
                """
                INSERT INTO user_configs (user_id, config)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (user_id) DO NOTHING
                RETURNING config
                """,
                (user_id, json.dumps(default_config or {})),
            )
            inserted = cur.fetchone()
        conn.commit()
    return inserted[0] if inserted else default_config or {}


def save_user_config(user_id, config):
    if not user_id or not db_enabled():
        return False
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_configs (user_id, config)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (user_id) DO UPDATE
                SET config = EXCLUDED.config,
                    version = user_configs.version + 1,
                    updated_at = NOW()
                """,
                (user_id, json.dumps(config or {})),
            )
        conn.commit()
    return True


def create_session_config(session_id, actor=None, default_config=None, existing_session_id=None):
    if not session_id or not db_enabled():
        return default_config or {}
    ensure_schema()
    user_id, service_id = _actor_ids(actor)
    config = None
    source_config_id = None
    with _connect() as conn:
        with conn.cursor() as cur:
            if existing_session_id:
                cur.execute(
                    "SELECT id, config FROM session_configs WHERE session_id = %s AND window_id = ''",
                    (str(existing_session_id),),
                )
                row = cur.fetchone()
                if row:
                    source_config_id, config = row[0], row[1]
            if config is None and user_id:
                config = get_user_config(user_id, default_config=default_config)
            if config is None:
                config = default_config or {}
            cur.execute(
                """
                INSERT INTO session_configs (
                    session_id, user_id, service_account_id, window_id, config, source_config_id
                )
                VALUES (%s, %s, %s, '', %s::jsonb, %s)
                ON CONFLICT (session_id, window_id) DO UPDATE
                SET config = EXCLUDED.config,
                    user_id = COALESCE(session_configs.user_id, EXCLUDED.user_id),
                    service_account_id = COALESCE(session_configs.service_account_id, EXCLUDED.service_account_id),
                    updated_at = NOW()
                RETURNING config
                """,
                (str(session_id), user_id, service_id, json.dumps(config), source_config_id),
            )
            row = cur.fetchone()
        conn.commit()
    return row[0] if row else config


def duplicate_window_config(session_id, window_id):
    if not session_id or not window_id or not db_enabled():
        return None
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, config, user_id, service_account_id FROM session_configs WHERE session_id = %s AND window_id = ''",
                (str(session_id),),
            )
            base = cur.fetchone()
            if not base:
                return None
            source_id, config, user_id, service_id = base
            cur.execute(
                """
                INSERT INTO session_configs (
                    session_id, user_id, service_account_id, window_id, config, source_config_id
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (session_id, window_id) DO UPDATE
                SET updated_at = NOW()
                RETURNING config
                """,
                (str(session_id), user_id, service_id, str(window_id), json.dumps(config or {}), source_id),
            )
            row = cur.fetchone()
        conn.commit()
    return row[0] if row else config


def _split_config_id(session_id):
    raw = str(session_id or "")
    if "_" not in raw:
        return raw, ""
    window_id, base_session = raw.split("_", 1)
    if base_session:
        return base_session, window_id
    return raw, ""


def load_session_config(session_id, window_id=None):
    if not session_id or not db_enabled():
        return None
    ensure_schema()
    base_session, inferred_window = _split_config_id(session_id)
    target_window = str(window_id if window_id is not None else inferred_window)
    with _connect() as conn:
        with conn.cursor() as cur:
            base_config = None
            if target_window:
                cur.execute(
                    "SELECT config FROM session_configs WHERE session_id = %s AND window_id = ''",
                    (str(base_session),),
                )
                base_row = cur.fetchone()
                if base_row:
                    base_config = base_row[0] or {}
            cur.execute(
                "SELECT config FROM session_configs WHERE session_id = %s AND window_id = %s",
                (str(base_session), target_window),
            )
            row = cur.fetchone()
            if row:
                if target_window and base_config is not None:
                    return {**base_config, **(row[0] or {})}
                return row[0] or {}
            if target_window:
                return base_config
    return None


def save_session_config(session_id, config, window_id=None, merge=True):
    if not session_id or not db_enabled():
        return False
    ensure_schema()
    base_session, inferred_window = _split_config_id(session_id)
    target_window = str(window_id if window_id is not None else inferred_window)
    incoming = dict(config or {})
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT config FROM session_configs WHERE session_id = %s AND window_id = %s",
                (str(base_session), target_window),
            )
            row = cur.fetchone()
            current = row[0] if row else {}
            new_config = {**(current or {}), **incoming} if merge else incoming
            if row:
                cur.execute(
                    """
                    UPDATE session_configs
                    SET config = %s::jsonb, updated_at = NOW()
                    WHERE session_id = %s AND window_id = %s
                    """,
                    (json.dumps(new_config), str(base_session), target_window),
                )
            else:
                cur.execute(
                    "SELECT owner_user_id, owner_service_id FROM analysis_sessions WHERE session_id = %s",
                    (str(base_session),),
                )
                owner = cur.fetchone() or (None, None)
                cur.execute(
                    """
                    INSERT INTO session_configs (session_id, user_id, service_account_id, window_id, config)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    """,
                    (str(base_session), owner[0], owner[1], target_window, json.dumps(new_config)),
                )
        conn.commit()
    return True


def response_config(config):
    return {"data": config or {}, "Last modified": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
