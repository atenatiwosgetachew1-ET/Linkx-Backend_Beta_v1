import os
from neo4j import GraphDatabase

from security.secret_store import MASKED_SECRET
from session_config_store import _connect, _load_managed_secret, db_enabled


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
    if credentials.get("neo4j_url") or credentials.get("neo4j_username") or credentials.get("neo4j_password"):
        return "env"
    return "payload"


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


def _managed_secret_password(credentials):
    secret_ref = credentials.get("password_ref") or credentials.get("neo4j_password_ref")
    if not secret_ref or not db_enabled():
        return None
    with _connect(application_name="linkx-neo4j-secret-resolve") as conn:
        with conn.cursor() as cur:
            return _load_managed_secret(cur, secret_ref)


def resolve_neo4j_credentials(credentials):
    if not credentials:
        raise Neo4jCredentialConfigError("Neo4j credentials are required")
    resolved = dict(credentials)
    resolved["url"] = resolved.get("url") or resolved.get("neo4j_url")
    resolved["username"] = resolved.get("username") or resolved.get("neo4j_username")
    password = resolved.get("password") or resolved.get("neo4j_password")
    secret_ref = resolved.get("password_ref") or resolved.get("neo4j_password_ref")
    if password == MASKED_SECRET:
        decrypted = _managed_secret_password(resolved)
        if decrypted:
            password = decrypted
            resolved["_credential_source"] = "managed_secret"
        elif secret_ref:
            raise Neo4jCredentialConfigError(
                "Neo4j password remained masked and managed secret decryption failed"
            )
        else:
            raise Neo4jCredentialConfigError(
                "Neo4j password is masked but no password_ref is available"
            )
    elif secret_ref:
        resolved["_credential_source"] = "managed_secret"
    elif resolved.get("neo4j_url") or resolved.get("neo4j_username") or resolved.get("neo4j_password"):
        resolved["_credential_source"] = "env"
    else:
        resolved["_credential_source"] = "payload"
    resolved["password"] = password
    missing = [key for key in ("url", "username", "password") if not resolved.get(key)]
    if missing:
        raise Neo4jCredentialConfigError(
            f"Neo4j credentials missing required field(s): {', '.join(missing)}"
        )
    return resolved


def create_neo4j_driver(credentials):
    credentials = resolve_neo4j_credentials(credentials)
    print("neo4j_credentials:", redacted_neo4j_credentials(credentials))
    driver = GraphDatabase.driver(
        credentials["url"],
        auth=(credentials["username"], credentials["password"]),
    )
    return DatabaseScopedDriver(driver, neo4j_database_name(credentials))


def credentials_for_cleanup(credentials=None):
    if str(os.getenv("LINKX_STORE_CLEANUP_CREDENTIALS", "false")).lower() not in {"1", "true", "yes", "on"}:
        credentials = dict(credentials or {})
        database = neo4j_database_name(credentials)
        return {"neo4j_database": database} if database else {}

    resolved = resolve_neo4j_credentials(credentials or {})
    database = neo4j_database_name(resolved)
    payload = {
        "neo4j_url": resolved.get("url") or resolved.get("neo4j_url"),
        "neo4j_username": resolved.get("username") or resolved.get("neo4j_username"),
        "neo4j_password": resolved.get("password") or resolved.get("neo4j_password"),
    }
    if database:
        payload["neo4j_database"] = database
    return {key: value for key, value in payload.items() if value}
