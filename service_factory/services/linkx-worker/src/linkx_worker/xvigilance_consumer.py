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
    
    graphs = {}
    total_anomalies = 0
    try:
        with driver.session() as session:
            result = session.run(f"MATCH (n:{safe_label})-[r]->(m:{safe_label}) WHERE r.reason IS NOT NULL RETURN n, r, m")
            for record in result:
                n = record["n"]
                r = record["r"]
                m = record["m"]
                
                total_anomalies += 1
                anomaly_type = getattr(r, 'type', 'UNKNOWN_ANOMALY')
                reason_text = r.get("reason", "Multiple Anomalies Detected")
                
                if anomaly_type not in graphs:
                    graphs[anomaly_type] = {
                        "nodes": {},
                        "edges": [],
                        "reason": reason_text
                    }
                
                n_id = str(n.get("ACCOUNTNO") or getattr(n, 'element_id', 'unknown_n'))
                m_id = str(m.get("ACCOUNTNO") or getattr(m, 'element_id', 'unknown_m'))
                r_id = str(getattr(r, 'element_id', 'unknown_r'))
                
                n_props = dict(n)
                m_props = dict(m)
                r_props = dict(r)
                
                graphs[anomaly_type]["nodes"][n_id] = {
                    "id": n_id,
                    "label": n_props.get("NodeId", n_id),
                    **n_props
                }
                
                graphs[anomaly_type]["nodes"][m_id] = {
                    "id": m_id,
                    "label": m_props.get("NodeId", m_id),
                    **m_props
                }
                
                graphs[anomaly_type]["edges"].append({
                    "id": r_id,
                    "from": n_id,
                    "to": m_id,
                    "label": anomaly_type,
                    **r_props
                })
    except Exception as e:
        print(f"[xVigilance-Consumer] Error querying Neo4j for anomalies: {e}", flush=True)
        return
        
    if not graphs:
        print(f"[xVigilance-Consumer] No anomalies found in window {window_id}. Graph is perfectly clean.", flush=True)
        return
        
    print(f"[xVigilance-Consumer] 🚨 Detective detected {total_anomalies} anomalous records! Generating unified risk scoring payloads...", flush=True)
    
    try:
        with psycopg.connect(os.getenv('LINKX_POSTGRES_DSN')) as conn:
            with conn.cursor() as cur:
                for anomaly_type, graph_data in graphs.items():
                    nodes_list = list(graph_data["nodes"].values())
                    edges_list = graph_data["edges"]
                    
                    # Calculate node degrees to find the masterminds
                    from collections import defaultdict
                    node_degrees = defaultdict(int)
                    for edge in edges_list:
                        node_degrees[edge["from"]] += 1
                        node_degrees[edge["to"]] += 1
                        
                    top_nodes = sorted(node_degrees.keys(), key=lambda x: node_degrees[x], reverse=True)
                    top_5_nodes = top_nodes[:5]
                    top_node_str = ", ".join(top_5_nodes)
                    primary_account = top_5_nodes[0] if top_5_nodes else "UNKNOWN"
                    
                    # Generate linked_entities array
                    linked_entities = []
                    for edge in edges_list[:50]:  # Limit to 50
                        linked_entities.append({
                            "accountno": edge["to"],
                            "relationship": edge["label"],
                            "risk_contribution": 0.95,
                            "is_flagged": True
                        })
                    
                    trace_id = str(uuid.uuid4())
                    ts_now = datetime.now().isoformat() + "Z"
                    
                    response_payload = {
                        "schema_version": "1.0",
                        "event_type": "analysis.link.flagged",
                        "success": True,
                        "message": f"Link analysis flagged for accounts [{top_node_str}]: {len(edges_list)} linked",
                        "data": {
                            "accountno": top_5_nodes,
                            "entity_id": primary_account,
                            "linked_accounts_count": len(nodes_list),
                            "flagged_entity_links": len(edges_list),
                            "beneficiary_blacklisted": True,
                            "flagged_rules": [anomaly_type],
                            "network_centrality_score": 0.95,
                            "max_path_length": 2,
                            "linked_entities": linked_entities,
                            "graph": {
                                "nodes": nodes_list,
                                "edges": edges_list
                            }
                        },
                        "meta": {
                            "trace_id": trace_id,
                            "span_id": str(uuid.uuid4())[:8],
                            "traceparent": f"00-{trace_id}-00-01",
                            "correlation_id": trace_id,
                            "timestamp": ts_now,
                            "service": {
                                "name": "link-analysis-service",
                                "version": "1.0.0"
                            },
                            "aggregation_key": {
                                "type": "accountno",
                                "value": primary_account
                            }
                        }
                    }
                    
                    evidence_json = json.dumps(response_payload, default=str)
                    
                    cur.execute("""
                        INSERT INTO link_analysis_evidence (
                            trace_id, session_id, entity_id, event_type, is_flagged, 
                            response_payload, request_payload, analyzed_at
                        ) VALUES (
                            %s, %s, %s, 'analysis.link.flagged', true, 
                            %s::jsonb, '{}'::jsonb, NOW()
                        )
                    """, (trace_id, 'XVIGILANCE_FINDINGS', primary_account, evidence_json))
                    
                    report_payload = {
                        "trace_id": trace_id,
                        "entity_id": primary_account,
                        "anomaly_type": anomaly_type,
                        "reason": graph_data["reason"]
                    }
                    cur.execute("""
                        INSERT INTO linkx_reports (report_type, source_system, external_reference_id, payload, status)
                        VALUES (%s, %s, %s, %s, %s)
                    """, ('SERVICE_EVIDENCE', 'link-analysis-service', trace_id, json.dumps(report_payload, default=str), 'FLAGGED'))
            conn.commit()
        print(f"[xVigilance-Consumer] Successfully promoted {len(graphs)} grouped anomaly graphs to the Postgres Dashboard!", flush=True)
    except Exception as e:
        print(f"[xVigilance-Consumer] Error inserting grouped evidence to Postgres: {e}", flush=True)






def fetch_db_mapping():
    try:
        with psycopg.connect(os.getenv('LINKX_POSTGRES_DSN')) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config FROM session_configs WHERE session_id = 'xvigilance_system' AND window_id = ''")
                row = cur.fetchone()
                if row and row[0] and "column_mapping" in row[0]:
                    return row[0]["column_mapping"]
    except Exception as e:
        print(f"[xVigilance-Consumer] Failed to load DB mapping: {e}", flush=True)
    return {}


def normalize_transaction_dataframe(df):
    import pandas as pd
    import numpy as np
    
    # 1. DB-Driven Mapping (Safe Application)
    db_mappings = fetch_db_mapping()
    if db_mappings:
        for src_col, tgt_col in db_mappings.items():
            if src_col in df.columns:
                if tgt_col in df.columns:
                    conflicts = df[tgt_col].notna() & df[src_col].notna() & (df[tgt_col] != df[src_col])
                    if conflicts.any():
                        print(f"[xVigilance-Normalizer] WARNING: {conflicts.sum()} mapping conflicts for {src_col}->{tgt_col}", flush=True)
                    df[tgt_col] = df[tgt_col].combine_first(df[src_col])
                else:
                    df[tgt_col] = df[src_col]
                    
    # 2. Canonical Fallback Mappings
    fallbacks = {
        "SENDERACCOUNTID": "ACCOUNTNO",
        "RECEIVERACCOUNTID": "BENACCOUNTNO",
        "TRANSFERAMOUNT": "AMOUNT"
    }
    for src_col, tgt_col in fallbacks.items():
        if src_col in df.columns:
            if tgt_col in df.columns:
                conflicts = df[tgt_col].notna() & df[src_col].notna() & (df[tgt_col] != df[src_col])
                if conflicts.any():
                    print(f"[xVigilance-Normalizer] WARNING: {conflicts.sum()} canonical fallback conflicts for {src_col}->{tgt_col}", flush=True)
                df[tgt_col] = df[tgt_col].combine_first(df[src_col])
            else:
                df[tgt_col] = df[src_col]
                
    # 3. Canonical Account Mapping (Stable String Formatting)
    def _format_account(x):
        if pd.isna(x):
            return x
        if isinstance(x, float) and x.is_integer():
            return str(int(x))
        return str(x)
        
    for col in ["ACCOUNTNO", "BENACCOUNTNO"]:
        if col in df.columns:
            df[col] = df[col].apply(_format_account)
            
    # 4. Amount Normalization
    if "AMOUNT" in df.columns:
        df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors='coerce')
        
    # 5. Timestamp Normalization (Authoritative from CREATEDDATE)
    if "CREATEDDATE" in df.columns:
        numeric_dates = pd.to_numeric(df["CREATEDDATE"], errors="coerce")
        valid_mask = numeric_dates.notna() & (numeric_dates > 1000000000000)
        if valid_mask.any():
            dt_series = pd.to_datetime(numeric_dates[valid_mask], unit="ms", utc=True)
            df.loc[valid_mask, "TRANSACTIONDATE"] = dt_series.dt.strftime("%Y-%m-%d")
            df.loc[valid_mask, "TRANSACTIONTIME"] = dt_series.dt.strftime("%H:%M:%S")
            df.loc[valid_mask, "TRANSACTIONTIMESTAMP"] = dt_series.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
    # 6. Lowercase Aliases for Graph Mapping
    if "ACCOUNTNO" in df.columns:
        df["accountno"] = df["ACCOUNTNO"]
    if "BENACCOUNTNO" in df.columns:
        df["benaccountno"] = df["BENACCOUNTNO"]
        
    # 7. Validation Logging
    missing_acc = df["ACCOUNTNO"].isna().sum() if "ACCOUNTNO" in df.columns else len(df)
    missing_ben = df["BENACCOUNTNO"].isna().sum() if "BENACCOUNTNO" in df.columns else len(df)
    missing_amt = df["AMOUNT"].isna().sum() if "AMOUNT" in df.columns else len(df)
    missing_tdate = df["TRANSACTIONDATE"].isna().sum() if "TRANSACTIONDATE" in df.columns else len(df)
    missing_ttime = df["TRANSACTIONTIME"].isna().sum() if "TRANSACTIONTIME" in df.columns else len(df)
    
    print(
        f"[xVigilance-Normalizer]\n"
        f"rows={len(df)}\n"
        f"missing ACCOUNTNO={missing_acc}\n"
        f"missing BENACCOUNTNO={missing_ben}\n"
        f"missing AMOUNT={missing_amt}\n"
        f"missing TRANSACTIONDATE={missing_tdate}\n"
        f"missing TRANSACTIONTIME={missing_ttime}", 
        flush=True
    )
    
    return df

def consume_firehose():
    global RUNNING
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    topic = "dev.xvigilance.transactions.raw.v2"
    brokers = os.getenv("LINKX_KAFKA_BOOTSTRAP_SERVERS", "172.27.23.106:9092")
    group_id = "linkx-xvigilance-super-final-500"
    session_id = "xvigilance-daemon"
    
    server_list = [b.strip() for b in brokers.split(",") if b.strip()]
    try:
        c = KafkaConsumer(
            topic,
            bootstrap_servers=server_list,
            group_id=group_id,
            auto_offset_reset="earliest",
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
                        
                        # --- NORMALIZATION APPLIED HERE ---
                        df = normalize_transaction_dataframe(df)
                        # -------------------------------------

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
                    
                    # --- NORMALIZATION APPLIED HERE ---
                    df = normalize_transaction_dataframe(df)
                    # ----------------------------------
                    
                    print(f"[xVigilance-Consumer] Ingesting micro-batch {batch_number} ({len(df)} records) to Neo4j...", flush=True)
                    try:
                        realtime_neo4j_message_ingest(payload, df, batch_number)
                    except Exception as e:
                        print(f"[xVigilance-Consumer] Error during Neo4j insertion: {e}", flush=True)
                    buffer.clear()
                    batch_number += 1

        except Exception as e:
            print(f"[xVigilance-Consumer] ERROR in consumer loop: {e}", flush=True)
            import traceback
            traceback.print_exc()
            if not RUNNING:
                break
            time.sleep(0.5)

    c.close()

if __name__ == "__main__":
    consume_firehose()
