-- ==============================================================================
-- 007: Deployment Audit Log
-- Tracks every deployment across all servers for institutional traceability.
-- "What version was on node22 on September 15th?"
-- ==============================================================================

CREATE TABLE IF NOT EXISTS deployment_audit_log (
    id              SERIAL PRIMARY KEY,
    deployed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    hostname        TEXT NOT NULL,
    role            TEXT NOT NULL,
    git_commit      TEXT NOT NULL,
    git_branch      TEXT NOT NULL DEFAULT 'main',
    domain          TEXT,
    verify_result   TEXT,
    failures        INTEGER DEFAULT 0,
    warnings        INTEGER DEFAULT 0,
    operator        TEXT
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_deploy_audit_hostname
    ON deployment_audit_log (hostname, deployed_at DESC);

CREATE INDEX IF NOT EXISTS idx_deploy_audit_role
    ON deployment_audit_log (role, deployed_at DESC);

CREATE INDEX IF NOT EXISTS idx_deploy_audit_commit
    ON deployment_audit_log (git_commit);

-- Example queries:
-- Latest deployment per server:
--   SELECT DISTINCT ON (hostname) * FROM deployment_audit_log ORDER BY hostname, deployed_at DESC;
--
-- All deployments for a specific commit:
--   SELECT * FROM deployment_audit_log WHERE git_commit = 'abc1234';
--
-- Failed deployments in the last 7 days:
--   SELECT * FROM deployment_audit_log WHERE verify_result = 'FAIL' AND deployed_at > NOW() - INTERVAL '7 days';
