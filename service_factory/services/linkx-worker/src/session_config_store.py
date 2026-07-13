import json
import os
from datetime import datetime

from security.secret_store import MASKED_SECRET, decrypt_secret, encrypt_secret, is_sensitive_key, should_store_secret


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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_secrets (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    scope_type TEXT NOT NULL,
                    scope_id TEXT NOT NULL,
                    secret_type TEXT NOT NULL,
                    ciphertext TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    rotated_at TIMESTAMPTZ,
                    expires_at TIMESTAMPTZ,
                    deleted_at TIMESTAMPTZ,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_managed_secrets_scope ON managed_secrets(scope_type, scope_id, secret_type, created_at DESC)")
        conn.commit()
    return True



def _secret_ref_key(key):
    return f"{key}_ref"


def _store_managed_secret(cur, scope_type, scope_id, secret_type, value):
    cur.execute(
        """
        INSERT INTO managed_secrets (scope_type, scope_id, secret_type, ciphertext)
        VALUES (%s, %s, %s, %s)
        RETURNING id::text
        """,
        (str(scope_type), str(scope_id), str(secret_type), encrypt_secret(value)),
    )
    return cur.fetchone()[0]


def _load_managed_secret(cur, secret_id):
    if not secret_id:
        return None
    cur.execute(
        "SELECT ciphertext FROM managed_secrets WHERE id = %s AND deleted_at IS NULL",
        (str(secret_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return decrypt_secret(row[0])


def _protect_config_secrets(value, cur, scope_type, scope_id, prefix=""):
    if isinstance(value, dict):
        protected = {}
        for key, item in value.items():
            key_text = str(key or "")
            if key_text.endswith("_ref"):
                protected.setdefault(key, item)
                continue
            path = f"{prefix}.{key_text}" if prefix else key_text
            ref_key = _secret_ref_key(key_text)
            if is_sensitive_key(key_text) and should_store_secret(item):
                protected[key] = MASKED_SECRET
                protected[ref_key] = _store_managed_secret(cur, scope_type, scope_id, path, item)
            else:
                protected[key] = _protect_config_secrets(item, cur, scope_type, scope_id, path)
        return protected
    if isinstance(value, list):
        return [_protect_config_secrets(item, cur, scope_type, scope_id, f"{prefix}[{idx}]") for idx, item in enumerate(value)]
    return value


def _merge_config(current, incoming):
    if not isinstance(current, dict) or not isinstance(incoming, dict):
        return incoming
    merged = dict(current or {})
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_config(merged.get(key) or {}, value)
        else:
            merged[key] = value
    return merged


PARENT_SCOPED_CONFIG_KEYS = {
    "kafka_addresses",
    "REST APIs",
    "storage_addresses",
    "storage_path",
    "storage_databases",
    "storage_tables",
    "active_storage_address",
    "active_storage_host",
    "storage_hdfs_user",
    "active_storage_database",
    "active_storage_tables",
    "storage_webhdfs_port",
    "storage_webhdfs_url",
    "storage_hdfs_uri",
    "hdfs_rpc_port",
    "hadoop_rcp_port",
    "hadoop_web_port",
    "spark_port",
    "thrift_port",
    "hive_metastore_uri",
    "hive_server_host",
    "hive_port",
    "elastic_api_base_url",
    "api_port",
    "search_api_endpoint_es_fuzzy",
    "search_api_endpoint_es_strict",
    "search_api_endpoint_hive_fuzzy",
    "search_api_endpoint_hive_strict",
    "search_columns_strict",
    "search_columns_fuzzy",
    "fetch_columns",
    "date_column",
    "default_source_col",
    "default_target_col",
    "default_relationship",
    "dataframes_limit",
    "large_search_backend",
    "elastic_scroll_enabled",
    "elastic_scroll_limit",
    "elastic_scroll_batch_size",
    "tools",
    "active_tool",
    "active_tool_protocol",
    "active_tool_username",
    "active_tool_password",
    "active_tool_password_ref",
    "active_tool_database",
    "active_tool_tables",
    "tool_protocol_port",
    "tool_web_port",
    "tool_credentials",
    "rule_names",
    "rule_file_names",
    "active_rule",
    "trusted_entities",
    "risk_entities",
    "automation",
    "remote",
}


def _split_parent_scoped_config(config):
    parent_config = {}
    window_config = {}
    for key, value in dict(config or {}).items():
        if key in PARENT_SCOPED_CONFIG_KEYS:
            parent_config[key] = value
        else:
            window_config[key] = value
    return parent_config, window_config


def _merge_window_config(base_config, window_config):
    merged = _merge_config(base_config or {}, window_config or {})
    for key in PARENT_SCOPED_CONFIG_KEYS:
        if isinstance(base_config, dict) and key in base_config:
            merged[key] = base_config[key]
    return merged


def _resolve_config_secrets(value, cur):
    if isinstance(value, dict):
        resolved = {}
        for key, item in value.items():
            key_text = str(key or "")
            if key_text.endswith("_ref"):
                resolved[key] = item
                continue
            ref_key = _secret_ref_key(key_text)
            if item == MASKED_SECRET and value.get(ref_key):
                secret = _load_managed_secret(cur, value.get(ref_key))
                resolved[key] = secret if secret is not None else item
            else:
                resolved[key] = _resolve_config_secrets(item, cur)
        return resolved
    if isinstance(value, list):
        return [_resolve_config_secrets(item, cur) for item in value]
    return value

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
                return _resolve_config_secrets(row[0] or {}, cur)
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
                (user_id, json.dumps(_protect_config_secrets(config or {}, cur, "user", user_id))),
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
            config = _protect_config_secrets(config or {}, cur, "session", f"{session_id}:")
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
                (str(session_id), user_id, service_id, str(window_id), json.dumps({}), source_id),
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
                    return _resolve_config_secrets(_merge_window_config(base_config or {}, row[0] or {}), cur)
                return _resolve_config_secrets(row[0] or {}, cur)
            if target_window:
                return _resolve_config_secrets(base_config, cur)
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
            if target_window:
                parent_incoming, incoming = _split_parent_scoped_config(incoming)
                if parent_incoming:
                    cur.execute(
                        "SELECT config FROM session_configs WHERE session_id = %s AND window_id = ''",
                        (str(base_session),),
                    )
                    parent_row = cur.fetchone()
                    parent_current = parent_row[0] if parent_row else {}
                    parent_config = _merge_config(parent_current or {}, parent_incoming) if merge else parent_incoming
                    parent_config = _protect_config_secrets(parent_config, cur, "session", f"{base_session}:")
                    if parent_row:
                        cur.execute(
                            """
                            UPDATE session_configs
                            SET config = %s::jsonb, updated_at = NOW()
                            WHERE session_id = %s AND window_id = ''
                            """,
                            (json.dumps(parent_config), str(base_session)),
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
                            VALUES (%s, %s, %s, '', %s::jsonb)
                            """,
                            (str(base_session), owner[0], owner[1], json.dumps(parent_config)),
                        )
                if not incoming:
                    conn.commit()
                    return True
            cur.execute(
                "SELECT config FROM session_configs WHERE session_id = %s AND window_id = %s",
                (str(base_session), target_window),
            )
            row = cur.fetchone()
            current = row[0] if row else {}
            new_config = _merge_config(current or {}, incoming) if merge else incoming
            new_config = _protect_config_secrets(new_config, cur, "session", f"{base_session}:{target_window}")
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


def migrate_existing_config_secrets():
    if not db_enabled():
        return {"user_configs": 0, "session_configs": 0}
    ensure_schema()
    counts = {"user_configs": 0, "session_configs": 0}
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, config FROM user_configs")
            for user_id, config in cur.fetchall():
                protected = _protect_config_secrets(config or {}, cur, "user", user_id)
                if protected != (config or {}):
                    cur.execute(
                        "UPDATE user_configs SET config = %s::jsonb, updated_at = NOW() WHERE user_id = %s",
                        (json.dumps(protected), user_id),
                    )
                    counts["user_configs"] += 1

            cur.execute("SELECT session_id, window_id, config FROM session_configs")
            for session_id, window_id, config in cur.fetchall():
                protected = _protect_config_secrets(config or {}, cur, "session", f"{session_id}:{window_id or ''}")
                if protected != (config or {}):
                    cur.execute(
                        "UPDATE session_configs SET config = %s::jsonb, updated_at = NOW() WHERE session_id = %s AND window_id = %s",
                        (json.dumps(protected), session_id, window_id or ""),
                    )
                    counts["session_configs"] += 1
        conn.commit()
    return counts


def response_config(config):
    return {"data": config or {}, "Last modified": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
