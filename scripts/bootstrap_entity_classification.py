import os
import psycopg
import json

# Parse the .env file manually so we don't need 'dotenv'
env_path = "/opt/linkx-worker/.env"
dsn = None
if os.path.exists(env_path):
    with open(env_path, "r") as f:
        for line in f:
            if line.startswith("LINKX_POSTGRES_DSN="):
                dsn = line.strip().split("=", 1)[1].strip('"').strip("'")
                break

if not dsn:
    print("Error: LINKX_POSTGRES_DSN not found. Make sure you are running this on Node-21.")
    exit(1)

try:
    print("Connecting to database...")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # 1. Create the table using an Append-Only design for a perfect audit trail
            cur.execute("""
                CREATE TABLE IF NOT EXISTS global_entity_classification (
                    version_id SERIAL PRIMARY KEY,
                    config_data JSONB NOT NULL,
                    updated_by VARCHAR(255) DEFAULT 'system',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 2. Check if we already seeded it
            cur.execute("SELECT COUNT(*) FROM global_entity_classification;")
            count = cur.fetchone()[0]
            
            if count == 0:
                # 3. Insert the baseline empty arrays
                baseline_config = {
                    "trusted_entities": [],
                    "risk_entities": [],
                    "pep_entities": [],
                    "sanction_entities": []
                }
                cur.execute("""
                    INSERT INTO global_entity_classification (config_data, updated_by)
                    VALUES (%s, %s)
                """, (json.dumps(baseline_config), 'system_bootstrap'))
                print("Successfully created the 'global_entity_classification' table and inserted the baseline configuration!")
            else:
                print(f"Table already exists and contains {count} configuration versions. No insertion needed.")
        
        conn.commit()

except Exception as e:
    print(f"Database error: {e}")
