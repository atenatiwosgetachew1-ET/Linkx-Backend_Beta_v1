import os

from neo4j import GraphDatabase

from linkx_cleanup.db import connect
from security.secret_store import MASKED_SECRET, decrypt_secret


class Neo4jCredentialConfigError(ValueError):
    pass


def neo4j_database_name(credentials=None):
    credentials = credentials or {}
    database = (
        credentials.get("database")
        or credentials.get("db")
        or credentials.get("neo4j_database")
        or credentials.get("active_tool_database")
    )
    database = str(database).strip() if database is not None else ""
    return database or None


class DatabaseScopedDriver:
    def __init__(self, driver, database=None):
        self._driver = driver
        self.database = database

    def session(self, *args, **kwargs):
        if self.database and "database" not in kwargs:
            kwargs["database"] = self.database
        return self._driver.session(*args, **kwargs)

    def close(self):
        return self._driver.close()

    def __getattr__(self, name):
        return getattr(self._driver, name)


def neo4j_credential_source(credentials=None):
    credentials = credentials or {}
    source = credentials.get("_credential_source")
    if source:
        return str(source)
    if credentials.get("password_ref") or credentials.get("neo4j_password_ref"):
        return "managed_secret"
    if credentials.get("neo4j_password"):
        return "cleanup_payload"
    if credentials.get("password"):
        return "payload"
    return "env"


def redacted_neo4j_credentials(credentials=None):
    credentials = credentials or {}
    return {
        "url": credentials.get("url") or credentials.get("neo4j_url"),
        "username": credentials.get("username") or credentials.get("neo4j_username"),
        "database": neo4j_database_name(credentials),
        "password": MASKED_SECRET if credentials.get("password") or credentials.get("neo4j_password") else None,
        "password_ref": "present" if credentials.get("password_ref") or credentials.get("neo4j_password_ref") else "missing",
        "source": neo4j_credential_source(credentials),
    }


def _load_managed_secret(secret_id):
    if not secret_id:
        return None
    with connect(application_name="linkx-cleanup-neo4j-secret-resolve") as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ciphertext FROM managed_secrets WHERE id = %s AND deleted_at IS NULL",
                (str(secret_id),),
            )
            row = cur.fetchone()
    if not row:
        return None
    return decrypt_secret(row[0])


def resolve_neo4j_credentials(credentials):
    if not credentials:
        raise Neo4jCredentialConfigError("Neo4j credentials are required")
    resolved = dict(credentials)
    resolved["url"] = resolved.get("url") or resolved.get("neo4j_url")
    resolved["username"] = resolved.get("username") or resolved.get("neo4j_username")
    password = resolved.get("password") or resolved.get("neo4j_password")
    secret_ref = resolved.get("password_ref") or resolved.get("neo4j_password_ref")
    if password == MASKED_SECRET:
        decrypted = _load_managed_secret(secret_ref) if secret_ref else None
        if decrypted:
            password = decrypted
            resolved["_credential_source"] = "managed_secret"
        elif secret_ref:
            raise Neo4jCredentialConfigError("Neo4j password remained masked and managed secret decryption failed")
        else:
            raise Neo4jCredentialConfigError("Neo4j password is masked but no password_ref is available")
    elif secret_ref and not password:
        decrypted = _load_managed_secret(secret_ref)
        if decrypted:
            password = decrypted
            resolved["_credential_source"] = "managed_secret"
        else:
            raise Neo4jCredentialConfigError("Neo4j managed secret decryption failed for cleanup")
    elif secret_ref:
        resolved["_credential_source"] = "managed_secret"
    elif resolved.get("neo4j_password"):
        resolved["_credential_source"] = "cleanup_payload"
    elif resolved.get("password"):
        resolved["_credential_source"] = "payload"
    else:
        resolved["_credential_source"] = "env"
    resolved["password"] = password
    missing = [key for key in ("url", "username", "password") if not resolved.get(key)]
    if missing:
        raise Neo4jCredentialConfigError(
            f"Neo4j credentials missing required field(s): {', '.join(missing)}"
        )
    return resolved


def create_neo4j_driver(credentials):
    credentials = resolve_neo4j_credentials(credentials)
    driver = GraphDatabase.driver(
        credentials["url"],
        auth=(credentials["username"], credentials["password"]),
    )
    return DatabaseScopedDriver(driver, neo4j_database_name(credentials))


def credentials_for_cleanup(credentials=None):
    credentials = dict(credentials or {})
    database = neo4j_database_name(credentials)
    payload = {
        "neo4j_url": credentials.get("url") or credentials.get("neo4j_url"),
        "neo4j_username": credentials.get("username") or credentials.get("neo4j_username"),
        "neo4j_password_ref": credentials.get("password_ref") or credentials.get("neo4j_password_ref"),
    }
    if database:
        payload["neo4j_database"] = database

    if str(os.getenv("LINKX_STORE_CLEANUP_CREDENTIALS", "false")).lower() not in {"1", "true", "yes", "on"}:
        return {key: value for key, value in payload.items() if value}

    resolved = resolve_neo4j_credentials(credentials or {})
    payload["neo4j_password"] = resolved.get("password") or resolved.get("neo4j_password")
    return {key: value for key, value in payload.items() if value}
