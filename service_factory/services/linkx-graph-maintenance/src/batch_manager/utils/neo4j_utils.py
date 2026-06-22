import os
from neo4j import GraphDatabase


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


def create_neo4j_driver(credentials):
    if not credentials:
        raise ValueError("Neo4j credentials are required")
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

    credentials = dict(credentials or {})
    database = neo4j_database_name(credentials)
    payload = {
        "neo4j_url": credentials.get("url") or credentials.get("neo4j_url"),
        "neo4j_username": credentials.get("username") or credentials.get("neo4j_username"),
        "neo4j_password": credentials.get("password") or credentials.get("neo4j_password"),
    }
    if database:
        payload["neo4j_database"] = database
    return {key: value for key, value in payload.items() if value}
