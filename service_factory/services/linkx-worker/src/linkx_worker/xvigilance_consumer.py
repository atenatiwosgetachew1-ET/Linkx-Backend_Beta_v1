import os
import json
import time
import pandas as pd
from datetime import datetime
import signal
import sys

from batch_manager.services.risk_scoring_kafka_service import DEFAULT_KAFKA_BROKERS, _neo4j_credentials, _base_analyzer_payload
from batch_manager.analyzing.analyzer import realtime_neo4j_message_ingest, rule_to_node_label
from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver
import psycopg
import uuid
import json
from kafka import KafkaConsumer

RUNNING = True

def handle_shutdown(signum, frame):
    global RUNNING
    print("\n[xVigilance-Consumer] Shutting down gracefully...", flush=True)
    RUNNING = False


def promote_anomalies_to_postgres(credentials, session_id, window_id):
    driver = create_neo4j_driver(credentials)
    node_label = rule_to_node_label("bank transactions", session_id)
    safe_label = f"`{str(node_label).replace('`', '')}`"
    
    anomalies = []
    try:
        with driver.session() as session:
            # Find all nodes involved in anomalous relationships
            result = session.run(f"""
            MATCH (n:{safe_label})-[r]->(m:{safe_label})
            WHERE type(r) IN ['SMURFING', 'CIRCULAR_FLOW']
            RETURN n.ACCOUNTNO as account, type(r) as anomaly_type, properties(r) as reason, n.TRANSACTIONDATE as date
            """)
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
        with psycopg.connect(os.getenv('LINKX_POSTGRES_DSN')) as conn:
            with conn.cursor() as cur:
                for anomaly in anomalies:
                    evidence_json = json.dumps({
                        "anomaly_type": anomaly["anomaly_type"],
                        "details": anomaly["reason"],
                        "window_id": window_id
                    })
                    cur.execute("""
                        INSERT INTO link_analysis_evidence (
                            id, session_id, entity_id, is_flagged, 
                            risk_score, evidence_data, request_payload, analyzed_at
                        ) VALUES (
                            %s, %s, %s, true, 
                            99.9, %s::jsonb, '{}'::jsonb, NOW()
                        )
                    """, (str(uuid.uuid4()), session_id, anomaly["entity_id"], evidence_json))
            conn.commit()
        print(f"[xVigilance-Consumer] Successfully promoted {len(anomalies)} alerts to the Postgres Dashboard!", flush=True)
    except Exception as e:
        print(f"[xVigilance-Consumer] Error inserting evidence to Postgres: {e}", flush=True)


def consume_firehose():
    global RUNNING
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    topic = "dev.xvigilance.transactions.raw.v1"
    brokers = os.getenv("LINKX_KAFKA_BOOTSTRAP_SERVERS", "172.27.23.106:9092")
    group_id = "linkx-xvigilance-worker-ingestion"
    session_id = "xvigilance-daemon"
    
    server_list = [b.strip() for b in brokers.split(",") if b.strip()]
    try:
        c = KafkaConsumer(
            topic,
            bootstrap_servers=server_list,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            consumer_timeout_ms=2000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None
        )
    except Exception as e:
        print(f"[xVigilance-Consumer] CRITICAL: Could not connect to Kafka broker at {brokers}. Error: {e}", flush=True)
        return

    print("=" * 70, flush=True)
    print(f" xVigilance Governed Ingestion Consumer Online", flush=True)
    print(f" Listening to: {topic}", flush=True)
    print("=" * 70, flush=True)

    buffer = []
    batch_size = 500
    batch_number = 1

    credentials = _neo4j_credentials(session_id)
    payload = _base_analyzer_payload(session_id, credentials)
    payload["rule"] = "bank transactions"

    while RUNNING:
        try:
            for msg in c:
                if not RUNNING:
                    break
                    
                data = msg.value
                if not data:
                    continue
                    
                # Check if it's the watermark
                is_watermark = False
                if msg.headers:
                    for k, v in msg.headers:
                        if k == "type" and v == b"watermark":
                            is_watermark = True

                if is_watermark:
                    print(f"[xVigilance-Consumer] Received WATERMARK for window {data.get('window_id')}. Flushing buffer...", flush=True)
                    if buffer:
                        df = pd.DataFrame(buffer)
                        print(f"[xVigilance-Consumer] Ingesting {len(df)} remaining records to Neo4j...", flush=True)
                        realtime_neo4j_message_ingest(payload, df, batch_number)
                        buffer.clear()
                        batch_number += 1
                    print("[xVigilance-Consumer] Ingestion complete. Scanning Graph for LA_Script_rules violations (Smurfing, Circular Flow)...", flush=True)
                    promote_anomalies_to_postgres(credentials, session_id, data.get('window_id'))
                    print(f"[xVigilance-Consumer] Window {data.get('window_id')} finalized successfully.", flush=True)
                    continue

                # It's a standard transaction
                buffer.append(data)
                
                if len(buffer) >= batch_size:
                    df = pd.DataFrame(buffer)
                    print(f"[xVigilance-Consumer] Ingesting micro-batch {batch_number} ({len(df)} records) to Neo4j...", flush=True)
                    try:
                        realtime_neo4j_message_ingest(payload, df, batch_number)
                    except Exception as e:
                        print(f"[xVigilance-Consumer] Error during Neo4j insertion: {e}", flush=True)
                    buffer.clear()
                    batch_number += 1

        except Exception as e:
            if not RUNNING:
                break
            time.sleep(0.5)

    c.close()

if __name__ == "__main__":
    consume_firehose()
