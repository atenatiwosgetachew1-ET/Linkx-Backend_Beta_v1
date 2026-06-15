-- User default config plus per-session/per-window config store.
-- Safe to run multiple times.

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

CREATE INDEX IF NOT EXISTS idx_session_configs_user ON session_configs(user_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_session_configs_session ON session_configs(session_id, window_id);
