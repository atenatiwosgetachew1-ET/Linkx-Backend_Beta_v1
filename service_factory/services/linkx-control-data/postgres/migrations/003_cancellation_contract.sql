-- Adds explicit cancellation fields for worker/API coordination.
-- Safe to run multiple times on an existing control-data database.

ALTER TABLE analysis_sessions
    ADD COLUMN IF NOT EXISTS cancellation_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancellation_reason TEXT,
    ADD COLUMN IF NOT EXISTS cancel_requested_by TEXT;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS cancellation_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancellation_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_analysis_sessions_status ON analysis_sessions(status, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_jobs_cancel_requested ON jobs(status, cancellation_requested_at);
