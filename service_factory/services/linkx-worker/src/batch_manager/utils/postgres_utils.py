import os
from contextlib import contextmanager


DEFAULT_DATABASE_URL = "postgresql:///linkx"
DEFAULT_CONNECT_TIMEOUT = 5


def get_database_url():
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


def get_connect_timeout():
    try:
        return int(os.getenv("DATABASE_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT))
    except ValueError:
        return DEFAULT_CONNECT_TIMEOUT


@contextmanager
def get_postgres_connection():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL driver is not installed. Install requirements.txt to add psycopg."
        ) from exc

    with psycopg.connect(
        get_database_url(),
        connect_timeout=get_connect_timeout(),
        application_name=os.getenv("DATABASE_APPLICATION_NAME", "linkx-backend"),
    ) as conn:
        yield conn


def check_postgres_connection():
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

    return True
