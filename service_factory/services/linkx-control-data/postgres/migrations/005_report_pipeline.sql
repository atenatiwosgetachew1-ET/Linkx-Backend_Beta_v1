-- Migration: 005_report_pipeline
-- Description: Core tables for the unified reporting pipeline (Parent, XVigilance, Sibling)

-- 1. Unified Reports Table
CREATE TABLE IF NOT EXISTS linkx_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_type TEXT NOT NULL CHECK (report_type IN ('PARENT_RECEIVED', 'XVIGILANCE_FINDING', 'SERVICE_EVIDENCE')),
    status TEXT NOT NULL DEFAULT 'NEW' CHECK (status IN ('NEW', 'INVESTIGATING', 'RESOLVED', 'SYNCED', 'FLAGGED', 'ARCHIVED')),
    source_system TEXT NOT NULL,
    external_reference_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for optimal performance
CREATE INDEX IF NOT EXISTS idx_linkx_reports_type_status ON linkx_reports(report_type, status);
CREATE INDEX IF NOT EXISTS idx_linkx_reports_ext_ref ON linkx_reports(external_reference_id);
CREATE INDEX IF NOT EXISTS idx_linkx_reports_created ON linkx_reports(created_at DESC);

-- 2. Report Evidence Links Table
CREATE TABLE IF NOT EXISTS linkx_report_evidence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id UUID NOT NULL REFERENCES linkx_reports(id) ON DELETE CASCADE,
    artifact_id UUID NOT NULL, -- Logical foreign key to artifacts
    evidence_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_linkx_report_evidence_report_id ON linkx_report_evidence(report_id);

-- 3. Outbox Table for Synchronizing back to Parent/Siblings safely
CREATE TABLE IF NOT EXISTS report_sync_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id UUID NOT NULL REFERENCES linkx_reports(id) ON DELETE CASCADE,
    target_system TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED', 'CONFLICT')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_report_sync_outbox_status ON report_sync_outbox(status) WHERE status IN ('PENDING', 'PROCESSING', 'FAILED', 'CONFLICT');

-- Triggers for auto-updating `updated_at`
CREATE OR REPLACE FUNCTION update_linkx_reports_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_linkx_reports_updated_at
BEFORE UPDATE ON linkx_reports
FOR EACH ROW
EXECUTE FUNCTION update_linkx_reports_updated_at();

CREATE OR REPLACE FUNCTION update_report_sync_outbox_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_report_sync_outbox_updated_at
BEFORE UPDATE ON report_sync_outbox
FOR EACH ROW
EXECUTE FUNCTION update_report_sync_outbox_updated_at();

