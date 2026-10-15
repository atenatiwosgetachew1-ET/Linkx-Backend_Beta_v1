"""Rule engine thresholds — dynamic configuration from PostgreSQL.

Loads default anomaly detection thresholds with optional database override.
"""

import os

try:
    import psycopg
except ImportError:
    psycopg = None


def fetch_rule_thresholds():
    defaults = {
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
        "activity_spike_multiplier": 3,
        "activity_spike_min_daily_count": 10,
        "activity_spike_min_amount": 500.0,
        "rapid_withdrawal_amount_tolerance": 0.1,
        "rapid_withdrawal_min_amount": 250.0,
        "abnormal_balance_min_change": 500.0,
        "hub_spoke_min_single_amount": 500.0,
        "abnormal_balance_min_tx_amount": 1000.0,
        "just_below_threshold_min_count": 2,
        "just_below_threshold_ratio": 0.90,
    }
    try:
        dsn = os.getenv('DATABASE_URL') or os.getenv('LINKX_POSTGRES_DSN')

        if dsn and psycopg is not None:
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT config_data FROM global_rule_thresholds ORDER BY created_at DESC LIMIT 1")
                    row = cur.fetchone()
                    if row and row[0]:
                        defaults.update(row[0])
    except Exception as e:
        print(f"fetch_rule_thresholds error: {e}", flush=True)
    return defaults
