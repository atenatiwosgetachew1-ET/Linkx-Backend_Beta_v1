import json
import os
import secrets

from werkzeug.security import check_password_hash, generate_password_hash

from batch_manager.utils.postgres_utils import get_postgres_connection


ROLE_ALIASES = {
    "team_leader": "manager",
    "analyst": "viewer",
    "viewer": "viewer",
    "SUPER_ADMIN": "admin",
    "ADMIN": "admin",
    "HIGHER_OFFICIAL": "admin",
    "DIRECTOR": "manager",
    "TEAM_LEADER": "manager",
    "ANALYST": "viewer",
    "DATA_ENCODER": "viewer",
    "VIEWER": "viewer",
}

DEFAULT_ROLE_PERMISSIONS = {
    "superuser": [
        "superuser:manage",
        "users:manage",
        "config:read",
        "config:write",
        "source:create",
        "source:connect",
        "source:disconnect",
        "graph:create",
        "graph:read",
        "graph:link",
        "batch:upload",
        "batch:query",
        "analysis:run",
        "reports:read",
        "session:create",
        "session:read",
    ],
    "admin": [
        "config:read",
        "config:write",
        "source:create",
        "source:connect",
        "source:disconnect",
        "graph:create",
        "graph:read",
        "graph:link",
        "batch:upload",
        "batch:query",
        "analysis:run",
        "reports:read",
        "users:manage",
        "session:create",
        "session:read",
    ],
    "manager": [
        "config:read",
        "source:create",
        "source:connect",
        "source:disconnect",
        "graph:create",
        "graph:read",
        "graph:link",
        "batch:upload",
        "batch:query",
        "analysis:run",
        "reports:read",
        "session:create",
        "session:read",
    ],
    "analyst": [
        "config:read",
        "source:create",
        "source:connect",
        "source:disconnect",
        "graph:create",
        "graph:read",
        "graph:link",
        "batch:upload",
        "batch:query",
        "analysis:run",
        "reports:read",
        "session:create",
        "session:read",
    ],
    "viewer": [
        "config:read",
        "graph:read",
        "reports:read",
        "session:read",
    ],
}

DEFAULT_SERVICE_PERMISSIONS = {
    "parent_gateway_service": [
        "auth:verify",
        "session:create",
        "session:read",
        "graph:read",
        "reports:read",
    ],
    "reporting_service": [
        "session:read",
        "graph:read",
        "reports:read",
    ],
    "ai": [
        "ai:read",
        "session:read",
        "graph:read",
        "reports:read",
    ],
}

_AUTH_SCHEMA_READY = False


def ensure_auth_schema():
    global _AUTH_SCHEMA_READY
    if _AUTH_SCHEMA_READY:
        return

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '2s'")
            cur.execute("SET LOCAL statement_timeout = '15s'")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                display_name TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS roles (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                description TEXT
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS permissions (
                id BIGSERIAL PRIMARY KEY,
                key TEXT NOT NULL UNIQUE,
                description TEXT
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS user_roles (
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                role_id BIGINT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
                PRIMARY KEY (user_id, role_id)
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS role_permissions (
                role_id BIGINT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
                permission_id BIGINT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
                PRIMARY KEY (role_id, permission_id)
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS service_accounts (
                id BIGSERIAL PRIMARY KEY,
                client_id TEXT NOT NULL UNIQUE,
                secret_hash TEXT NOT NULL,
                display_name TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS service_account_permissions (
                service_account_id BIGINT NOT NULL REFERENCES service_accounts(id) ON DELETE CASCADE,
                permission_id BIGINT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
                PRIMARY KEY (service_account_id, permission_id)
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS analysis_sessions (
                session_id TEXT PRIMARY KEY,
                owner_user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                owner_service_id BIGINT REFERENCES service_accounts(id) ON DELETE CASCADE,
                created_by_type TEXT,
                created_by_id BIGINT,
                parent_session_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)
            _migrate_analysis_sessions(cur)
            _seed_roles_permissions(cur)
            _bootstrap_superuser(cur)
            _bootstrap_admin(cur)
            _bootstrap_service_accounts(cur)
        conn.commit()
    _AUTH_SCHEMA_READY = True


def _migrate_analysis_sessions(cur):
    if _column_is_not_nullable(cur, "analysis_sessions", "owner_user_id"):
        cur.execute("ALTER TABLE analysis_sessions ALTER COLUMN owner_user_id DROP NOT NULL")

    _add_column_if_missing(
        cur,
        "analysis_sessions",
        "owner_service_id",
        "owner_service_id BIGINT REFERENCES service_accounts(id) ON DELETE CASCADE",
    )
    _add_column_if_missing(cur, "analysis_sessions", "created_by_type", "created_by_type TEXT")
    _add_column_if_missing(cur, "analysis_sessions", "created_by_id", "created_by_id BIGINT")
    _add_column_if_missing(cur, "analysis_sessions", "parent_session_id", "parent_session_id TEXT")
    _add_column_if_missing(cur, "analysis_sessions", "status", "status TEXT NOT NULL DEFAULT 'active'")
    _add_column_if_missing(cur, "analysis_sessions", "ended_at", "ended_at TIMESTAMPTZ")
    _add_column_if_missing(
        cur,
        "analysis_sessions",
        "cancellation_requested_at",
        "cancellation_requested_at TIMESTAMPTZ",
    )
    _add_column_if_missing(cur, "analysis_sessions", "cancellation_reason", "cancellation_reason TEXT")
    _add_column_if_missing(cur, "analysis_sessions", "cancel_requested_by", "cancel_requested_by TEXT")


def _column_exists(cur, table_name, column_name):
    cur.execute("""
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = %s
      AND column_name = %s
    """, (table_name, column_name))
    return cur.fetchone() is not None


def _column_is_not_nullable(cur, table_name, column_name):
    cur.execute("""
    SELECT is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = %s
      AND column_name = %s
    """, (table_name, column_name))
    row = cur.fetchone()
    return bool(row and row[0] == "NO")


def _add_column_if_missing(cur, table_name, column_name, column_ddl):
    if not _column_exists(cur, table_name, column_name):
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_ddl}")


def _seed_roles_permissions(cur):
    permission_keys = set()
    for keys in DEFAULT_ROLE_PERMISSIONS.values():
        permission_keys.update(keys)
    for keys in DEFAULT_SERVICE_PERMISSIONS.values():
        permission_keys.update(keys)

    for permission_key in sorted(permission_keys):
        cur.execute(
            "INSERT INTO permissions (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
            (permission_key,),
        )

    for role_name, permission_keys in DEFAULT_ROLE_PERMISSIONS.items():
        cur.execute(
            "INSERT INTO roles (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
            (role_name,),
        )
        for permission_key in permission_keys:
            cur.execute("""
            INSERT INTO role_permissions (role_id, permission_id)
            SELECT r.id, p.id
            FROM roles r, permissions p
            WHERE r.name = %s AND p.key = %s
            ON CONFLICT DO NOTHING
            """, (role_name, permission_key))


def _bootstrap_superuser(cur):
    username = os.getenv("LINKX_BOOTSTRAP_SUPERUSER_USERNAME")
    password = os.getenv("LINKX_BOOTSTRAP_SUPERUSER_PASSWORD")
    if not username or not password:
        return

    cur.execute("""
    INSERT INTO users (username, password_hash, display_name)
    VALUES (%s, %s, %s)
    ON CONFLICT (username) DO NOTHING
    RETURNING id
    """, (username, generate_password_hash(password), username))
    inserted = cur.fetchone()
    if not inserted:
        return

    cur.execute("""
    INSERT INTO user_roles (user_id, role_id)
    SELECT %s, id FROM roles WHERE name = 'superuser'
    ON CONFLICT DO NOTHING
    """, (inserted[0],))


def _bootstrap_admin(cur):
    username = os.getenv("LINKX_BOOTSTRAP_ADMIN_USERNAME") or os.getenv("Linkx_Admin")
    password = os.getenv("LINKX_BOOTSTRAP_ADMIN_PASSWORD")
    if not username or not password:
        return

    cur.execute("""
    INSERT INTO users (username, password_hash, display_name)
    VALUES (%s, %s, %s)
    ON CONFLICT (username) DO NOTHING
    RETURNING id
    """, (username, generate_password_hash(password), username))
    inserted = cur.fetchone()
    if not inserted:
        return

    cur.execute("""
    INSERT INTO user_roles (user_id, role_id)
    SELECT %s, id FROM roles WHERE name = 'admin'
    ON CONFLICT DO NOTHING
    """, (inserted[0],))


def _bootstrap_service_accounts(cur):
    raw = os.getenv("LINKX_BOOTSTRAP_SERVICE_ACCOUNTS")
    if not raw:
        return

    try:
        accounts = json.loads(raw)
    except json.JSONDecodeError:
        print("[auth] LINKX_BOOTSTRAP_SERVICE_ACCOUNTS is not valid JSON")
        return

    if not isinstance(accounts, list):
        print("[auth] LINKX_BOOTSTRAP_SERVICE_ACCOUNTS must be a JSON list")
        return

    for account in accounts:
        if not isinstance(account, dict):
            continue
        client_id = str(account.get("client_id") or "").strip()
        client_secret = str(account.get("client_secret") or "")
        permissions = account.get("permissions") or DEFAULT_SERVICE_PERMISSIONS.get(client_id) or []
        display_name = account.get("display_name") or client_id
        if client_id and client_secret:
            _upsert_service_account(cur, client_id, client_secret, permissions, display_name)


def create_or_update_service_account(client_id, client_secret, permissions=None, display_name=None):
    ensure_auth_schema()
    permissions = permissions or DEFAULT_SERVICE_PERMISSIONS.get(client_id, [])
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            service_id = _upsert_service_account(cur, client_id, client_secret, permissions, display_name or client_id)
        conn.commit()
    return get_service_account_by_id(service_id)


def list_service_accounts():
    ensure_auth_schema()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, client_id, secret_hash, display_name, is_active
            FROM service_accounts
            ORDER BY client_id
            """)
            rows = cur.fetchall()
    return [hydrate_service_account(_service_from_row(row)) for row in rows]


def update_service_account(service_id, client_secret=None, permissions=None, display_name=None, is_active=None):
    ensure_auth_schema()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, client_id, secret_hash, display_name, is_active
            FROM service_accounts
            WHERE id = %s
            """, (service_id,))
            row = cur.fetchone()
            if not row:
                return None

            updates = []
            params = []
            if client_secret:
                updates.append("secret_hash = %s")
                params.append(generate_password_hash(client_secret))
            if display_name is not None:
                updates.append("display_name = %s")
                params.append(display_name)
            if is_active is not None:
                updates.append("is_active = %s")
                params.append(bool(is_active))
            if updates:
                params.append(service_id)
                cur.execute(
                    f"UPDATE service_accounts SET {', '.join(updates)} WHERE id = %s",
                    params,
                )

            if permissions is not None:
                cur.execute("DELETE FROM service_account_permissions WHERE service_account_id = %s", (service_id,))
                for permission_key in permissions:
                    cur.execute(
                        "INSERT INTO permissions (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
                        (permission_key,),
                    )
                    cur.execute("""
                    INSERT INTO service_account_permissions (service_account_id, permission_id)
                    SELECT %s, id FROM permissions WHERE key = %s
                    ON CONFLICT DO NOTHING
                    """, (service_id, permission_key))
        conn.commit()
    return get_service_account_by_id(service_id)


def delete_service_account(service_id):
    ensure_auth_schema()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM service_accounts WHERE id = %s RETURNING id", (service_id,))
            deleted = cur.fetchone()
        conn.commit()
    return deleted is not None


def _upsert_service_account(cur, client_id, client_secret, permissions, display_name):
    cur.execute("""
    INSERT INTO service_accounts (client_id, secret_hash, display_name, is_active)
    VALUES (%s, %s, %s, TRUE)
    ON CONFLICT (client_id) DO UPDATE
    SET secret_hash = EXCLUDED.secret_hash,
        display_name = EXCLUDED.display_name,
        is_active = TRUE
    RETURNING id
    """, (client_id, generate_password_hash(client_secret), display_name))
    service_id = cur.fetchone()[0]

    cur.execute("DELETE FROM service_account_permissions WHERE service_account_id = %s", (service_id,))
    for permission_key in permissions:
        cur.execute(
            "INSERT INTO permissions (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
            (permission_key,),
        )
        cur.execute("""
        INSERT INTO service_account_permissions (service_account_id, permission_id)
        SELECT %s, id FROM permissions WHERE key = %s
        ON CONFLICT DO NOTHING
        """, (service_id, permission_key))
    return service_id


def authenticate_user(username, password):
    ensure_auth_schema()
    user = get_user_by_username(username)
    if not user or not user["is_active"]:
        return None
    if not check_password_hash(user["password_hash"], password):
        return None
    return hydrate_user(user)


def authenticate_service_account(client_id, client_secret):
    ensure_auth_schema()
    service = get_service_account_by_client_id(client_id)
    if not service or not service["is_active"]:
        return None
    if not check_password_hash(service["secret_hash"], client_secret):
        return None
    return hydrate_service_account(service)


def normalize_role_names(role_names):
    if isinstance(role_names, str):
        role_names = [role_names]
    role_names = role_names or []
    normalized = []
    for role in role_names:
        role_name = normalize_parent_role(str(role).strip())
        if role_name in DEFAULT_ROLE_PERMISSIONS and role_name not in normalized:
            normalized.append(role_name)
    return normalized


def actor_is_superuser(actor):
    return actor_has_permission(actor, "superuser:manage") or "superuser" in set(actor.get("roles") or [])


def actor_can_manage_roles(actor, target_roles):
    if actor_is_superuser(actor):
        return True
    allowed = {"analyst", "viewer"}
    return set(target_roles or []).issubset(allowed)


def list_users():
    ensure_auth_schema()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, username, password_hash, display_name, is_active
            FROM users
            ORDER BY username
            """)
            rows = cur.fetchall()
    return [hydrate_user(_user_from_row(row)) for row in rows]


def create_or_update_user(username, password=None, roles=None, display_name=None, is_active=True):
    ensure_auth_schema()
    roles = normalize_role_names(roles) or ["viewer"]
    if not password:
        password = secrets.token_urlsafe(32)

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO users (username, password_hash, display_name, is_active)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (username) DO UPDATE
            SET display_name = EXCLUDED.display_name,
                is_active = EXCLUDED.is_active
            RETURNING id
            """, (username, generate_password_hash(password), display_name or username, bool(is_active)))
            user_id = cur.fetchone()[0]
            _set_user_roles(cur, user_id, roles)
        conn.commit()
    return get_user_by_id(user_id)


def update_user(user_id, password=None, roles=None, display_name=None, is_active=None):
    ensure_auth_schema()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, username, password_hash, display_name, is_active
            FROM users
            WHERE id = %s
            """, (user_id,))
            row = cur.fetchone()
            if not row:
                return None

            updates = []
            params = []
            if password:
                updates.append("password_hash = %s")
                params.append(generate_password_hash(password))
            if display_name is not None:
                updates.append("display_name = %s")
                params.append(display_name)
            if is_active is not None:
                updates.append("is_active = %s")
                params.append(bool(is_active))
            if updates:
                params.append(user_id)
                cur.execute(f"UPDATE users SET {', '.join(updates)} WHERE id = %s", params)
            if roles is not None:
                _set_user_roles(cur, user_id, normalize_role_names(roles) or ["viewer"])
        conn.commit()
    return get_user_by_id(user_id)


def delete_user(user_id):
    ensure_auth_schema()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = %s RETURNING id", (user_id,))
            deleted = cur.fetchone()
        conn.commit()
    return deleted is not None


def _set_user_roles(cur, user_id, roles):
    cur.execute("DELETE FROM user_roles WHERE user_id = %s", (user_id,))
    for role_name in roles:
        cur.execute("""
        INSERT INTO user_roles (user_id, role_id)
        SELECT %s, id FROM roles WHERE name = %s
        ON CONFLICT DO NOTHING
        """, (user_id, role_name))


def upsert_external_user(username, display_name=None, parent_roles=None):
    ensure_auth_schema()
    parent_roles = parent_roles or []
    if isinstance(parent_roles, str):
        parent_roles = [parent_roles]

    mapped_roles = []
    for role in parent_roles:
        mapped = normalize_parent_role(str(role).strip())
        if mapped in DEFAULT_ROLE_PERMISSIONS and mapped not in mapped_roles:
            mapped_roles.append(mapped)
    if not mapped_roles:
        mapped_roles = ["viewer"]

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO users (username, password_hash, display_name, is_active)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (username) DO UPDATE
            SET display_name = EXCLUDED.display_name,
                is_active = TRUE
            RETURNING id
            """, (
                username,
                generate_password_hash(secrets.token_urlsafe(32)),
                display_name or username,
            ))
            user_id = cur.fetchone()[0]
            cur.execute("DELETE FROM user_roles WHERE user_id = %s", (user_id,))
            for role_name in mapped_roles:
                cur.execute("""
                INSERT INTO user_roles (user_id, role_id)
                SELECT %s, id FROM roles WHERE name = %s
                ON CONFLICT DO NOTHING
                """, (user_id, role_name))
        conn.commit()
    return get_user_by_id(user_id)


def get_user_by_username(username):
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, username, password_hash, display_name, is_active
            FROM users
            WHERE username = %s
            """, (username,))
            row = cur.fetchone()
    return _user_from_row(row)


def get_user_by_id(user_id):
    ensure_auth_schema()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, username, password_hash, display_name, is_active
            FROM users
            WHERE id = %s
            """, (user_id,))
            row = cur.fetchone()
    user = _user_from_row(row)
    if not user or not user["is_active"]:
        return None
    return hydrate_user(user)


def get_service_account_by_client_id(client_id):
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, client_id, secret_hash, display_name, is_active
            FROM service_accounts
            WHERE client_id = %s
            """, (client_id,))
            row = cur.fetchone()
    return _service_from_row(row)


def get_service_account_by_id(service_id):
    ensure_auth_schema()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, client_id, secret_hash, display_name, is_active
            FROM service_accounts
            WHERE id = %s
            """, (service_id,))
            row = cur.fetchone()
    service = _service_from_row(row)
    if not service or not service["is_active"]:
        return None
    return hydrate_service_account(service)


def hydrate_user(user):
    roles = []
    permissions = []
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT r.name
            FROM roles r
            JOIN user_roles ur ON ur.role_id = r.id
            WHERE ur.user_id = %s
            ORDER BY r.name
            """, (user["id"],))
            roles = [row[0] for row in cur.fetchall()]
            cur.execute("""
            SELECT DISTINCT p.key
            FROM permissions p
            JOIN role_permissions rp ON rp.permission_id = p.id
            JOIN user_roles ur ON ur.role_id = rp.role_id
            WHERE ur.user_id = %s
            ORDER BY p.key
            """, (user["id"],))
            permissions = [row[0] for row in cur.fetchall()]

    user = dict(user)
    user["actor_type"] = "user"
    user["roles"] = roles
    user["permissions"] = permissions
    return user


def hydrate_service_account(service):
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT DISTINCT p.key
            FROM permissions p
            JOIN service_account_permissions sap ON sap.permission_id = p.id
            WHERE sap.service_account_id = %s
            ORDER BY p.key
            """, (service["id"],))
            permissions = [row[0] for row in cur.fetchall()]

    service = dict(service)
    service["actor_type"] = "service"
    service["roles"] = ["service_account"]
    service["permissions"] = permissions
    return service


def public_user(user):
    if not user:
        return None
    return {
        "id": user["id"],
        "actor_type": "user",
        "username": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "roles": user.get("roles", []),
        "permissions": user.get("permissions", []),
    }


def public_service_account(service):
    if not service:
        return None
    return {
        "id": service["id"],
        "actor_type": "service",
        "client_id": service["client_id"],
        "display_name": service.get("display_name") or service["client_id"],
        "roles": service.get("roles", ["service_account"]),
        "permissions": service.get("permissions", []),
    }


def public_actor(actor):
    if not actor:
        return None
    if actor.get("actor_type") == "service" or "client_id" in actor:
        return public_service_account(actor)
    return public_user(actor)


def normalize_parent_role(role):
    value = str(role or "").strip()
    return ROLE_ALIASES.get(value) or ROLE_ALIASES.get(value.upper()) or ROLE_ALIASES.get(value.lower()) or value


def user_has_permission(user, permission):
    return actor_has_permission(user, permission)


def actor_has_permission(actor, permission):
    if not actor:
        return False
    return permission in set(actor.get("permissions") or [])


def bind_analysis_session(session_id, user_id):
    return bind_analysis_session_actor(session_id, {"actor_type": "user", "id": user_id})


def bind_analysis_session_actor(session_id, actor, parent_session_id=None):
    ensure_auth_schema()
    actor_type = actor.get("actor_type")
    actor_id = actor.get("id")
    owner_user_id = actor_id if actor_type == "user" else None
    owner_service_id = actor_id if actor_type == "service" else None

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO analysis_sessions (
                session_id,
                owner_user_id,
                owner_service_id,
                created_by_type,
                created_by_id,
                parent_session_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO UPDATE
            SET last_seen_at = NOW()
            WHERE
                (analysis_sessions.owner_user_id IS NOT DISTINCT FROM EXCLUDED.owner_user_id)
                AND (analysis_sessions.owner_service_id IS NOT DISTINCT FROM EXCLUDED.owner_service_id)
            RETURNING session_id
            """, (
                str(session_id),
                owner_user_id,
                owner_service_id,
                actor_type,
                actor_id,
                parent_session_id,
            ))
            row = cur.fetchone()
        conn.commit()
    return bool(row)


def can_access_analysis_session(session_id, user):
    return can_access_analysis_session_actor(session_id, user)


def can_access_analysis_session_actor(session_id, actor):
    if not session_id or not actor:
        return False
    if actor_has_permission(actor, "users:manage"):
        return True

    ensure_auth_schema()
    actor_type = actor.get("actor_type")
    actor_id = actor.get("id")
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            if actor_type == "service":
                cur.execute("""
                SELECT 1
                FROM analysis_sessions
                WHERE session_id = %s AND owner_service_id = %s
                """, (str(session_id), actor_id))
            else:
                cur.execute("""
                SELECT 1
                FROM analysis_sessions
                WHERE session_id = %s AND owner_user_id = %s
                """, (str(session_id), actor_id))
            return cur.fetchone() is not None


def _user_from_row(row):
    if not row:
        return None
    return {
        "id": row[0],
        "username": row[1],
        "password_hash": row[2],
        "display_name": row[3],
        "is_active": row[4],
    }


def _service_from_row(row):
    if not row:
        return None
    return {
        "id": row[0],
        "client_id": row[1],
        "secret_hash": row[2],
        "display_name": row[3],
        "is_active": row[4],
    }
