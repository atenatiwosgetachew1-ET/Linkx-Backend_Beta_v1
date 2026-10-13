-- Migration: 006_cluster_bootstrap.sql
-- Description: Consolidates runtime dynamic tables (evidence, thresholds, lineage, classification, xvigilance, managed_secrets)
-- so new cluster deployments have 100% schema readiness from boot.

-- 1. Managed Secrets (Ciphertext storage for password_ref lookups)
CREATE TABLE IF NOT EXISTS managed_secrets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scope_type TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    secret_type TEXT NOT NULL,
    ciphertext TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_managed_secrets_scope ON managed_secrets(scope_type, scope_id, secret_type, created_at DESC);

-- 2. Security Audit Events
CREATE TABLE IF NOT EXISTS security_audit_events (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT,
    target_type TEXT,
    target_id TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    ip_address TEXT,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_security_audit_events_created ON security_audit_events(created_at DESC);

-- 3. Link Analysis Evidence Ledger
CREATE TABLE IF NOT EXISTS link_analysis_evidence (
    id BIGSERIAL PRIMARY KEY,
    trace_id TEXT NOT NULL,
    correlation_id TEXT,
    transaction_id TEXT,
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT 'accountno',
    session_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    is_flagged BOOLEAN NOT NULL DEFAULT FALSE,
    flagged_rules JSONB,
    linked_accounts_count INT NOT NULL DEFAULT 0,
    network_centrality_score NUMERIC(5, 2),
    max_path_length INT,
    duration_ms NUMERIC(10, 2),
    request_payload JSONB NOT NULL,
    response_payload JSONB NOT NULL,
    analyzed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_evidence_trace_entity UNIQUE (trace_id, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_link_evidence_entity ON link_analysis_evidence (entity_id, analyzed_at DESC);

-- 4. Global Rule Thresholds
CREATE TABLE IF NOT EXISTS global_rule_thresholds (
    version_id SERIAL PRIMARY KEY,
    config_data JSONB NOT NULL,
    updated_by VARCHAR(255) DEFAULT 'system',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO global_rule_thresholds (config_data, updated_by)
SELECT '{
  "smurfing_single_tx_threshold": 300000,
  "smurfing_min_tx_count": 3,
  "smurfing_cumulative_threshold": 900000,
  "reporting_threshold": 300000,
  "global_min_anomaly_amount": 100.0,
  "circular_flow_check_amounts": false,
  "circular_flow_amount_tolerance": 0.05,
  "circular_flow_min_amount": 200.0,
  "fund_flow_max_downstream": 5,
  "fund_flow_hub_threshold": 1000,
  "fund_flow_min_amount": 200.0,
  "late_night_start": 2300,
  "late_night_end": 400,
  "late_night_min_amount": 500.0,
  "hub_spoke_min_counterparties": 3,
  "hub_spoke_min_amount": 500.0,
  "hub_spoke_min_single_amount": 500.0,
  "activity_spike_multiplier": 3,
  "activity_spike_min_daily_count": 10,
  "activity_spike_min_amount": 500.0,
  "rapid_withdrawal_amount_tolerance": 0.1,
  "rapid_withdrawal_min_amount": 250.0,
  "abnormal_balance_min_change": 500.0,
  "abnormal_balance_min_tx_amount": 1000.0,
  "just_below_threshold_min_count": 2,
  "just_below_threshold_ratio": 0.90
}'::jsonb, 'system_bootstrap'
WHERE NOT EXISTS (SELECT 1 FROM global_rule_thresholds);

-- 5. Global Entity Classification
CREATE TABLE IF NOT EXISTS global_entity_classification (
    version_id SERIAL PRIMARY KEY,
    config_data JSONB NOT NULL,
    updated_by VARCHAR(255) DEFAULT 'system',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO global_entity_classification (config_data, updated_by)
SELECT '{
  "trusted_entities": [],
  "risk_entities": [],
  "pep_entities": [],
  "sanction_entities": []
}'::jsonb, 'system_bootstrap'
WHERE NOT EXISTS (SELECT 1 FROM global_entity_classification);

-- 6. Global Score Lineage
CREATE TABLE IF NOT EXISTS global_score_lineage (
    version_id SERIAL PRIMARY KEY,
    config_data JSONB NOT NULL,
    updated_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO global_score_lineage (config_data, updated_by)
SELECT '{
  "base_scores": {
    "HIGH_RISK_LINK": 50,
    "CIRCULAR_FLOW": 30,
    "EFFECTIVE_FLOW": 25,
    "SMURFING": 20,
    "SHARED_IDENTIFIER": 20,
    "HUB_AND_SPOKE": 10,
    "RAPID_FAN_OUT": 10,
    "ABNORMAL_BALANCE_CHANGE": 10,
    "LATE_NIGHT_TX": 15,
    "JUST_BELOW_THRESHOLD": 20,
    "RAPID_WITHDRAWAL": 25,
    "ACCOUNT_ACTIVITY_SPIKE": 15,
    "FUND_FLOW": 10,
    "DORMANT_TO_ACTIVE": 25,
    "PEP_INVOLVED": 50,
    "SANCTIONED_ENTITY_MATCH": 100,
    "FRAUD_AGGREGATOR": 50
  },
  "node_thresholds": [
    { "min_nodes": 10000, "add_points": 30 },
    { "min_nodes": 5000, "add_points": 20 },
    { "min_nodes": 1000, "add_points": 10 },
    { "min_nodes": 100, "add_points": 5 }
  ],
  "money_thresholds": [
    { "min_amount": 10000000, "add_points": 40 },
    { "min_amount": 5000000, "add_points": 30 },
    { "min_amount": 1000000, "add_points": 20 },
    { "min_amount": 500000, "add_points": 10 }
  ]
}'::jsonb, 'system_bootstrap'
WHERE NOT EXISTS (SELECT 1 FROM global_score_lineage);

-- 7. XVigilance Checkpoints & Slice Runs
CREATE TABLE IF NOT EXISTS xvigilance_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    feed_name TEXT UNIQUE NOT NULL,
    last_window_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    is_paused BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xvigilance_slice_runs (
    run_id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    records_count BIGINT DEFAULT 0,
    duration_ms NUMERIC(10, 2),
    error_message TEXT,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO xvigilance_checkpoints (feed_name, last_window_end, status, is_paused)
VALUES ('hourly_transaction_detective', NOW() - INTERVAL '1 hour', 'running', false)
ON CONFLICT (feed_name) DO NOTHING;
