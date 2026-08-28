import os
import sys

# Ensure the root src folder is in the path so we can import from batch_manager
sys.path.append(os.path.join(os.path.dirname(__file__), "../service_factory/services/linkx-api/src"))

from batch_manager.utils.postgres_utils import get_postgres_connection

sql = """
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
"""

def main():
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        print("Table 'link_analysis_evidence' created successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")

if __name__ == "__main__":
    main()
