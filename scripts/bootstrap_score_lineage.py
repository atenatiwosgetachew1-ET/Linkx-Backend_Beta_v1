import psycopg, os, json
from datetime import datetime, timezone

dsn = None

env_paths = ['/opt/linkx-backend-api/.env', '/opt/linkx-worker/.env', '.env']
env_file = next((p for p in env_paths if os.path.exists(p)), None)
if env_file:
    with open(env_file, 'r') as f:
        for line in f:
            if line.startswith('LINKX_POSTGRES_DSN='):
                dsn = line.strip().split('=', 1)[1].strip(' "\'')

default_config = {
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
}

conn = psycopg.connect(dsn)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS global_score_lineage (
    version_id SERIAL PRIMARY KEY,
    config_data JSONB NOT NULL,
    updated_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
""")

cur.execute("SELECT COUNT(*) FROM global_score_lineage")
count = cur.fetchone()[0]

if count == 0:
    cur.execute(
        "INSERT INTO global_score_lineage (config_data, updated_by) VALUES (%s, %s)",
        (json.dumps(default_config), "system_init")
    )
    print("Seeded initial score lineage configuration.")
else:
    print("global_score_lineage already contains data.")

conn.commit()
cur.close()
conn.close()
