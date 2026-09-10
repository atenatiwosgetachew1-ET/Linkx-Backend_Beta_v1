import os
import json
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

# Try loading from the standard worker location
load_dotenv("/opt/linkx-worker/.env")

dsn = os.environ.get("LINKX_POSTGRES_DSN")
if not dsn:
    print("Error: LINKX_POSTGRES_DSN not found in environment.")
    exit(1)

baseline_config = {
    "base_scores": {
        "HIGH_RISK_LINK": 50,
        "CIRCULAR_FLOW": 30,
        "SMURFING": 20,
        "SHARED_IDENTIFIER": 20,
        "HUB_AND_SPOKE": 10,
        "RAPID_FAN_OUT": 10,
        "ABNORMAL_BALANCE_CHANGE": 10
    },
    "node_thresholds": [
        {"min_nodes": 10000, "add_points": 30},
        {"min_nodes": 5000, "add_points": 20},
        {"min_nodes": 1000, "add_points": 10},
        {"min_nodes": 100, "add_points": 5}
    ],
    "money_thresholds": [
        {"min_amount": 10000000, "add_points": 40},
        {"min_amount": 5000000, "add_points": 30},
        {"min_amount": 1000000, "add_points": 20},
        {"min_amount": 500000, "add_points": 10}
    ]
}

try:
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    
    # Create the append-only table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS risk_scoring_config (
            version_id SERIAL PRIMARY KEY,
            config_data JSONB NOT NULL,
            created_by VARCHAR(100) DEFAULT 'system_init',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Check if a config already exists to prevent duplicate baselines
    cur.execute("SELECT COUNT(*) FROM risk_scoring_config;")
    count = cur.fetchone()[0]
    
    if count == 0:
        cur.execute(
            "INSERT INTO risk_scoring_config (config_data, created_by) VALUES (%s, %s);",
            (json.dumps(baseline_config), 'system_baseline')
        )
        print("Successfully created risk_scoring_config table and inserted baseline configuration.")
    else:
        print(f"Table already exists and contains {count} versions. Skipping baseline insertion.")
        
    conn.commit()
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Database error: {e}")
