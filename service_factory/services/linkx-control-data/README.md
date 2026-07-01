# LinkX Control Data Service

This is the Server 2 bundle for PostgreSQL + Redis. It is the shared coordination layer for the split backend.

## Owns

- PostgreSQL database for auth/RBAC, sessions, jobs, job events, artifact metadata, and cleanup metadata
- Redis for queues, locks, short-lived progress, and worker coordination

## Does Not Own

- Flask API execution
- analysis execution
- Neo4j graph storage
- artifact file bytes

Artifact file bytes should live on shared storage. PostgreSQL stores only artifact metadata and storage URIs.

## Local Run

    cp .env.example .env
    docker compose up -d

PostgreSQL defaults:

    host: 127.0.0.1
    port: 5432
    database: linkx
    user: linkx

Redis defaults:

    host: 127.0.0.1
    port: 6379
    auth: required through REDIS_PASSWORD

## Application Connection Strings

API and workers should use:

    DATABASE_URL=postgresql://linkx:<password>@<control-data-host>:5432/linkx
    LINKX_REDIS_URL=redis://:<redis-password>@<control-data-host>:6379/0

The current app reads DATABASE_URL for PostgreSQL. LINKX_POSTGRES_DSN is kept in env examples as the future explicit service contract.

## Schema

Initialization SQL lives in postgres/initdb. It creates:

- users
- roles
- permissions
- user_roles
- role_permissions
- service_accounts
- service_account_permissions
- analysis_sessions
- jobs
- job_events
- artifacts
- cleanup_runs

The official PostgreSQL Docker image runs initdb SQL only on first database creation. For existing data, use explicit migrations.

## Operational Notes

- Change all default passwords before deployment.
- Back up PostgreSQL regularly.
- Redis is coordination state, but appendonly is enabled for safer restarts.
- Keep Redis bound to the Server 2 private interface with REDIS_BIND_ADDR and restrict 6379 to approved backend servers.
- Do not store large files in PostgreSQL. Store file bytes in shared artifact storage and store metadata here.
