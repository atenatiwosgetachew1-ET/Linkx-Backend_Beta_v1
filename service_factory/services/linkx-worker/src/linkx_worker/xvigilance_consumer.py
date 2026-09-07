import os
import json
import time
import pandas as pd
from datetime import datetime
import signal
import sys

from batch_manager.services.risk_scoring_kafka_service import DEFAULT_KAFKA_BROKERS, _neo4j_credentials, _base_analyzer_payload
from batch_manager.analyzing.analyzer import realtime_neo4j_message_ingest
from kafka import KafkaConsumer

RUNNING = True

def handle_shutdown(signum, frame):
    global RUNNING
    print("\n[xVigilance-Consumer] Shutting down gracefully...", flush=True)
    RUNNING = False

def consume_firehose():
    global RUNNING
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    topic = "dev.xvigilance.transactions.raw.v1"
    brokers = os.getenv("LINKX_KAFKA_BOOTSTRAP_SERVERS", "172.27.23.106:9092")
    group_id = "linkx-xvigilance-worker-ingestion"
    session_id = "XVIGILANCE_FINDINGS"
    
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

    credentials = {
        "url": os.getenv("LINKX_NEO4J_URL", "bolt://172.27.23.43:7687"),
        "username": os.getenv("LINKX_NEO4J_USERNAME", "neo4j"),
        "password": os.getenv("LINKX_NEO4J_PASSWORD", "password"),
        "session_id": session_id,
    }
    
    payload = {
        "id": "batch_data",
        "type": "new",
        "session_id": session_id,
        "run_id": session_id,
        "spark_conf": {
            "storage_ip": os.getenv("LINKX_ACTIVE_STORAGE_ADDRESS", "172.27.23.43:5000"),
            "spark_port": os.getenv("LINKX_SPARK_PORT", "7077"),
        },
        "tool": "neo4j",
        "tool_credentials": credentials,
        "log_file": f"{session_id}.log",
        "action": "Link Analysis",
        "rule": "bank transactions"
    }

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
                    print("[xVigilance-Consumer] Triggering LA_Script_rules (Smurfing, Circular Flow, etc)...", flush=True)
                    # NOTE: Here we would trigger Layer 2 & 3
                    # For now, we are in Phase 2 Ingestion
                    print("[xVigilance-Consumer] Window finalized successfully.", flush=True)
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
