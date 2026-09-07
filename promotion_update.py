import re

filepath = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py"
with open(filepath, "r") as f:
    code = f.read()

new_imports = """from batch_manager.analyzing.analyzer import realtime_neo4j_message_ingest, rule_to_node_label
from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver
from db import get_pg_connection
import uuid
import json"""

code = code.replace("from batch_manager.analyzing.analyzer import realtime_neo4j_message_ingest", new_imports)

promotion_code = """
def promote_anomalies_to_postgres(credentials, session_id, window_id):
    driver = create_neo4j_driver(credentials)
    node_label = rule_to_node_label("bank transactions", session_id)
    safe_label = f"`{str(node_label).replace('`', '')}`"
    
    anomalies = []
    try:
        with driver.session() as session:
            # Find all nodes involved in anomalous relationships
            result = session.run(f\"\"\"
            MATCH (n:{safe_label})-[r]->(m:{safe_label})
            WHERE type(r) IN ['SMURFING', 'CIRCULAR_FLOW']
            RETURN n.ACCOUNTNO as account, type(r) as anomaly_type, properties(r) as reason, n.TRANSACTIONDATE as date
            \"\"\")
            for record in result:
                anomalies.append({
                    "entity_id": record["account"],
                    "anomaly_type": record["anomaly_type"],
                    "reason": record["reason"],
                    "date": record["date"]
                })
    except Exception as e:
        print(f"[xVigilance-Consumer] Error querying Neo4j for anomalies: {e}", flush=True)
        return
        
    if not anomalies:
        print(f"[xVigilance-Consumer] No anomalies found in window {window_id}. Graph is perfectly clean.", flush=True)
        return
        
    print(f"[xVigilance-Consumer] 🚨 Detective detected {len(anomalies)} anomalous records! Promoting to Evidence Dashboard...", flush=True)
    
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                for anomaly in anomalies:
                    evidence_json = json.dumps({
                        "anomaly_type": anomaly["anomaly_type"],
                        "details": anomaly["reason"],
                        "window_id": window_id
                    })
                    cur.execute(\"\"\"
                        INSERT INTO link_analysis_evidence (
                            id, session_id, entity_id, is_flagged, 
                            risk_score, evidence_data, request_payload, analyzed_at
                        ) VALUES (
                            %s, %s, %s, true, 
                            99.9, %s::jsonb, '{}'::jsonb, NOW()
                        )
                    \"\"\", (str(uuid.uuid4()), session_id, anomaly["entity_id"], evidence_json))
            conn.commit()
        print(f"[xVigilance-Consumer] Successfully promoted {len(anomalies)} alerts to the Postgres Dashboard!", flush=True)
    except Exception as e:
        print(f"[xVigilance-Consumer] Error inserting evidence to Postgres: {e}", flush=True)

"""

# Insert the promotion code before consume_firehose
code = code.replace("def consume_firehose():", promotion_code + "\ndef consume_firehose():")

# Call the promotion code inside the watermark handler
old_watermark_logic = """                    print("[xVigilance-Consumer] Triggering LA_Script_rules (Smurfing, Circular Flow, etc)...", flush=True)
                    # NOTE: Here we would trigger Layer 2 & 3
                    # For now, we are in Phase 2 Ingestion
                    print("[xVigilance-Consumer] Window finalized successfully.", flush=True)"""

new_watermark_logic = """                    print("[xVigilance-Consumer] Ingestion complete. Scanning Graph for LA_Script_rules violations (Smurfing, Circular Flow)...", flush=True)
                    promote_anomalies_to_postgres(credentials, session_id, data.get('window_id'))
                    print(f"[xVigilance-Consumer] Window {data.get('window_id')} finalized successfully.", flush=True)"""

code = code.replace(old_watermark_logic, new_watermark_logic)

with open(filepath, "w") as f:
    f.write(code)

print("Consumer updated!")
