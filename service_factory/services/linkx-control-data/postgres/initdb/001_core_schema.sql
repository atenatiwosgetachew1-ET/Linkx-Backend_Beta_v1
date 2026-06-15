CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- LinkX control-data core schema.
-- Matches current auth repository tables and adds the service-split foundations.

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS roles (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS permissions (
    id BIGSERIAL PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id BIGINT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id BIGINT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id BIGINT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE IF NOT EXISTS service_accounts (
    id BIGSERIAL PRIMARY KEY,
    client_id TEXT NOT NULL UNIQUE,
    secret_hash TEXT NOT NULL,
    display_name TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS service_account_permissions (
    service_account_id BIGINT NOT NULL REFERENCES service_accounts(id) ON DELETE CASCADE,
    permission_id BIGINT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (service_account_id, permission_id)
);

CREATE TABLE IF NOT EXISTS analysis_sessions (
    session_id TEXT PRIMARY KEY,
    owner_user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    owner_service_id BIGINT REFERENCES service_accounts(id) ON DELETE CASCADE,
    created_by_type TEXT,
    created_by_id BIGINT,
    parent_session_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    cancellation_requested_at TIMESTAMPTZ,
    cancellation_reason TEXT,
    cancel_requested_by TEXT
);

CREATE TABLE IF NOT EXISTS user_configs (
    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT REFERENCES analysis_sessions(session_id) ON DELETE SET NULL,
    run_id TEXT,
    job_type TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'created',
    priority INTEGER NOT NULL DEFAULT 100,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    cancellation_requested_at TIMESTAMPTZ,
    cancellation_reason TEXT
);

CREATE TABLE IF NOT EXISTS job_events (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID REFERENCES jobs(id) ON DELETE CASCADE,
    session_id TEXT REFERENCES analysis_sessions(session_id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    message TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS artifacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id TEXT REFERENCES analysis_sessions(session_id) ON DELETE SET NULL,
    job_id UUID REFERENCES jobs(id) ON DELETE SET NULL,
    artifact_type TEXT NOT NULL,
    storage_backend TEXT NOT NULL DEFAULT 'filesystem',
    storage_uri TEXT NOT NULL,
    filename TEXT,
    size_bytes BIGINT,
    checksum TEXT,
    delete_status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS cleanup_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cleanup_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'created',
    session_id TEXT REFERENCES analysis_sessions(session_id) ON DELETE SET NULL,
    job_id UUID REFERENCES jobs(id) ON DELETE SET NULL,
    dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analysis_sessions_owner_user ON analysis_sessions(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_analysis_sessions_owner_service ON analysis_sessions(owner_service_id);
CREATE INDEX IF NOT EXISTS idx_analysis_sessions_status ON analysis_sessions(status, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_session_configs_user ON session_configs(user_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_session_configs_session ON session_configs(session_id, window_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status_queue ON jobs(status, queue_name, priority, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_jobs_cancel_requested ON jobs(status, cancellation_requested_at);
CREATE INDEX IF NOT EXISTS idx_jobs_session ON jobs(session_id);
CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events(job_id, created_at);
CREATE INDEX IF NOT EXISTS idx_job_events_session ON job_events(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_artifacts_session ON artifacts(session_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_expiry ON artifacts(delete_status, expires_at);
CREATE INDEX IF NOT EXISTS idx_cleanup_runs_status ON cleanup_runs(status, cleanup_type, created_at);
