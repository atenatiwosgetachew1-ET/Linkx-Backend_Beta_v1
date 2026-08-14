from linkx_xvigilance.db import connect

_SCHEMA_INITIALIZED = False


def ensure_xvigilance_schema():
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return

    with connect(application_name="xvigilance-schema-init") as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '3s'")
            cur.execute("SET LOCAL statement_timeout = '15s'")

            # 1. Checkpoints table for persistent high-water marks
            cur.execute("""
            CREATE TABLE IF NOT EXISTS xvigilance_checkpoints (
                feed_name TEXT PRIMARY KEY,
                last_window_end TIMESTAMPTZ NOT NULL,
                total_records_analyzed BIGINT NOT NULL DEFAULT 0,
                total_slices_completed BIGINT NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)

            # 2. Audit & execution log for every 1-hour slice
            cur.execute("""
            CREATE TABLE IF NOT EXISTS xvigilance_slice_runs (
                id BIGSERIAL PRIMARY KEY,
                feed_name TEXT NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                window_end TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                records_count INTEGER DEFAULT 0,
                duration_ms INTEGER,
                overrun_occurred BOOLEAN DEFAULT FALSE,
                summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ
            )
            """)

            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_xvigilance_slice_window 
            ON xvigilance_slice_runs (feed_name, window_start, window_end);
            """)

            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_xvigilance_slice_status 
            ON xvigilance_slice_runs (status, created_at DESC);
            """)

        conn.commit()
    _SCHEMA_INITIALIZED = True
