import os


def get_database_url():
    return os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")


def connect(application_name="linkx-cleanup"):
    import psycopg

    dsn = get_database_url()
    if not dsn:
        raise RuntimeError("DATABASE_URL or LINKX_POSTGRES_DSN is required")
    return psycopg.connect(dsn, application_name=application_name)
