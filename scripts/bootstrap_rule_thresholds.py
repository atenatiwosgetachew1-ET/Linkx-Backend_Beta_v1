import psycopg
import os
import json

dsn = None
with open('/opt/linkx-worker/.env', 'r') as f:
    for line in f:
        if line.startswith('LINKX_POSTGRES_DSN='):
            dsn = line.strip().split('=', 1)[1].strip(' "\'')

if not dsn:
    print("Error: LINKX_POSTGRES_DSN not found. Make sure you are running this on Node-21.")
    exit(1)

try:
    print("Connecting to database...")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # 1. Create the table using an Append-Only design for a perfect audit trail
            cur.execute("""
                CREATE TABLE IF NOT EXISTS global_rule_thresholds (
                    version_id SERIAL PRIMARY KEY,
                    config_data JSONB NOT NULL,
                    updated_by VARCHAR(255) DEFAULT 'system',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 2. Check if we already seeded it
            cur.execute("SELECT COUNT(*) FROM global_rule_thresholds;")
            count = cur.fetchone()[0]
            
            if count == 0:
                # 3. Insert the baseline defaults calibrated for Ethiopian banking
                baseline_config = {
                    "smurfing_single_tx_threshold": 300000,
                    "smurfing_min_tx_count": 3,
                    "smurfing_cumulative_threshold": 900000,

                    "reporting_threshold": 300000,

                    "circular_flow_check_amounts": False,

                    "late_night_start": 2300,
                    "late_night_end": 400,

                    "hub_spoke_min_counterparties": 3,

                    "activity_spike_multiplier": 3,
                    "activity_spike_min_daily_count": 10,

                    "rapid_withdrawal_amount_tolerance": 0.1
                }
                cur.execute("""
                    INSERT INTO global_rule_thresholds (config_data, updated_by)
                    VALUES (%s, 'system_bootstrap')
                """, [json.dumps(baseline_config)])
                print("✅ Baseline rule thresholds seeded successfully!")
                print(f"Config: {json.dumps(baseline_config, indent=2)}")
            else:
                cur.execute("SELECT config_data FROM global_rule_thresholds ORDER BY created_at DESC LIMIT 1;")
                existing = cur.fetchone()[0]
                print(f"⚠️  Table already has {count} version(s). Skipping seed.")
                print(f"Current config: {json.dumps(existing, indent=2)}")
            
        conn.commit()
    print("\n✅ Phase 1 complete. Table 'global_rule_thresholds' is ready.")

except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)
