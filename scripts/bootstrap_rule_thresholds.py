#!/usr/bin/env python3
import os
import json

try:
    import psycopg
except ImportError:
    import psycopg2 as psycopg

dsn = os.getenv('LINKX_POSTGRES_DSN') or os.getenv('DATABASE_URL')

if not dsn:
    env_paths = [
        '/opt/linkx-worker/.env',
        '/opt/linkx-backend-api/.env',
        '/opt/linkx-backend-update/.env',
        '.env'
    ]
    for p in env_paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                for line in f:
                    if line.startswith('LINKX_POSTGRES_DSN='):
                        dsn = line.strip().split('=', 1)[1].strip(" \"'")
                        break
            if dsn:
                break

if not dsn:
    print("[-] Error: LINKX_POSTGRES_DSN not found in environment or .env files.")
    exit(1)

FULL_BASELINE_CONFIG = {
    "smurfing_single_tx_threshold": 300000,
    "smurfing_min_tx_count": 3,
    "smurfing_cumulative_threshold": 900000,
    "reporting_threshold": 300000,
    "global_min_anomaly_amount": 100.0,
    "circular_flow_check_amounts": False,
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
    "just_below_threshold_ratio": 0.90,
}

try:
    print("[+] Connecting to PostgreSQL database...")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # 1. Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS global_rule_thresholds (
                    version_id SERIAL PRIMARY KEY,
                    config_data JSONB NOT NULL,
                    updated_by VARCHAR(255) DEFAULT 'system',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 2. Check existing configurations
            cur.execute("SELECT config_data FROM global_rule_thresholds ORDER BY created_at DESC LIMIT 1;")
            row = cur.fetchone()

            if not row or not row[0]:
                cur.execute("""
                    INSERT INTO global_rule_thresholds (config_data, updated_by)
                    VALUES (%s::jsonb, 'system_bootstrap')
                """, [json.dumps(FULL_BASELINE_CONFIG)])
                conn.commit()
                print("✅ Seeded initial baseline rule thresholds successfully!")
                print(f"Config: {json.dumps(FULL_BASELINE_CONFIG, indent=2)}")
            else:
                existing = row[0]
                missing_keys = {k: v for k, v in FULL_BASELINE_CONFIG.items() if k not in existing}
                if missing_keys:
                    merged = {**FULL_BASELINE_CONFIG, **existing}
                    cur.execute("""
                        INSERT INTO global_rule_thresholds (config_data, updated_by)
                        VALUES (%s::jsonb, 'system_phase3_calibration')
                    """, [json.dumps(merged)])
                    conn.commit()
                    print(f"✅ Appended new calibrated threshold keys: {list(missing_keys.keys())}")
                    print(f"Active Config: {json.dumps(merged, indent=2)}")
                else:
                    print("✅ All calibrated rule threshold keys are already present in global_rule_thresholds.")
                    print(f"Active Config: {json.dumps(existing, indent=2)}")

        conn.commit()
    print("\n✅ Phase 3 complete. Database global_rule_thresholds is verified and up-to-date.")

except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)
