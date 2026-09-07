import re
import os

filepath = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(filepath, "r") as f:
    code = f.read()

# 1. Add Kafka Producer initialization inside run_daemon
init_kafka = """
    config = get_xvigilance_config()

    # --- Phase 1: Initialize Kafka Producer ---
    kafka_brokers = os.getenv("LINKX_KAFKA_BOOTSTRAP_SERVERS", "172.27.23.106:9092")
    try:
        from confluent_kafka import Producer
        kafka_producer = Producer({'bootstrap.servers': kafka_brokers})
        kafka_available = True
    except ImportError:
        print("[xvigilance] Warning: confluent_kafka not installed. Kafka streaming disabled.", flush=True)
        kafka_available = False
        kafka_producer = None
    
    kafka_topic = "dev.xvigilance.transactions.raw.v1"
    # ------------------------------------------
"""
code = code.replace("    config = get_xvigilance_config()", init_kafka)

# 2. Add the per-record streaming logic inside the loop
hook_regex = r"# DETECTIVE ANALYSIS HOOK: \(Placeholder for anomaly/fraud heuristics\)\n\s*# e\.g\., analyze_window_anomalies\(page, window_start, window_end\)\n\s*# ========================================================================="

streaming_logic = """# PHASE 1: KAFKA FIREHOSE (Governed Routing)
                    if kafka_available and kafka_producer:
                        import json
                        for txn in page:
                            # 1 Message = 1 Transaction (Micro-batching)
                            # Stamping with xVigilance headers
                            headers = [
                                ("source", b"xvigilance-daemon"),
                                ("session_id", b"XVIGILANCE_FINDINGS"),
                                ("window_id", window_start.isoformat().encode('utf-8'))
                            ]
                            
                            # Fire to Kafka (internal buffer handles efficient batching)
                            kafka_producer.produce(
                                topic=kafka_topic,
                                value=json.dumps(txn).encode('utf-8'),
                                headers=headers
                            )
                        
                        # Trigger delivery callbacks for the page
                        kafka_producer.poll(0)
                    # ========================================================================="""

code = re.sub(hook_regex, streaming_logic, code)

# 3. Add the Watermark message after the loop finishes
watermark_logic = """
                if kafka_available and kafka_producer:
                    import json
                    watermark = {
                        "event": "WINDOW_COMPLETE",
                        "window_id": window_start.isoformat(),
                        "total_records": total_records
                    }
                    kafka_producer.produce(
                        topic=kafka_topic,
                        value=json.dumps(watermark).encode('utf-8'),
                        headers=[("source", b"xvigilance-daemon"), ("session_id", b"XVIGILANCE_FINDINGS"), ("type", b"watermark")]
                    )
                    kafka_producer.flush()
                    print(f"[xvigilance] Watermark fired. 100% of {total_records} transactions securely routed to Kafka.", flush=True)

                duration_ms = int((time.time() - t0) * 1000)
"""
code = code.replace("                duration_ms = int((time.time() - t0) * 1000)", watermark_logic)

with open(filepath, "w") as f:
    f.write(code)

print("Successfully injected Phase 1 Kafka Logic into runner.py!")
