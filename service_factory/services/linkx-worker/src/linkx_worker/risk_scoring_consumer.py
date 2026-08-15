import argparse
import json
import os
import signal
import socket
import sys
import time

from batch_manager.config_defaults import get_default_session_config
from batch_manager.services.risk_scoring_kafka_service import (
    DEFAULT_INPUT_TOPIC,
    DEFAULT_KAFKA_BROKERS,
    execute_formal_link_analysis,
    publish_risk_scoring_response,
)

RUNNING = True


def handle_shutdown(signum, frame):
    global RUNNING
    print(f"\n[RiskScoringConsumer] Received signal {signum}. Shutting down gracefully...", flush=True)
    RUNNING = False


def run_consumer_loop(brokers=None, input_topic=None, group_id=None, once=False):
    global RUNNING
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    defaults = get_default_session_config("risk_scoring_daemon")
    brokers = brokers or defaults.get("active_kafka_adress") or DEFAULT_KAFKA_BROKERS
    input_topic = input_topic or defaults.get("kafka_risk_scoring_input_topic") or DEFAULT_INPUT_TOPIC
    group_id = group_id or os.getenv("LINKX_KAFKA_RISK_GROUP_ID", "linkx-risk-analysis-worker")
    worker_id = f"linkx-risk-worker@{socket.gethostname()}:{os.getpid()}"

    print("=" * 70, flush=True)
    print(f" LinkX Risk Scoring Kafka Consumer Daemon Online", flush=True)
    print(f" Worker ID:    {worker_id}", flush=True)
    print(f" Brokers:      {brokers}", flush=True)
    print(f" Input Topic:  {input_topic}", flush=True)
    print(f" Consumer Grp: {group_id}", flush=True)
    print("=" * 70, flush=True)

    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        print("[!] confluent-kafka is required. Please install it via pip.", flush=True)
        return

    conf = {
        "bootstrap.servers": brokers,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }

    c = Consumer(conf)
    c.subscribe([input_topic])
    print(f"[-] Subscribed to topic: '{input_topic}'. Awaiting calculated score requests...", flush=True)

    processed_count = 0

    while RUNNING:
        msg = c.poll(timeout=2.0)
        if msg is None:
            if once and processed_count > 0:
                break
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"[!] Kafka consumer error: {msg.error()}", flush=True)
            continue

        try:
            key_str = msg.key().decode("utf-8") if msg.key() else "None"
            val_bytes = msg.value()
            event_data = json.loads(val_bytes.decode("utf-8"))
            
            print(f"\n[RiskScoringConsumer] Processing incoming event (Key={key_str}, Partition={msg.partition()}, Offset={msg.offset()})...", flush=True)
            
            response_event, status = execute_formal_link_analysis(event_data)
            if response_event:
                published = publish_risk_scoring_response(response_event, brokers=brokers)
                dest = response_event.get("meta", {}).get("messaging", {}).get("destination_name")
                event_type = response_event.get("event_type")
                acc = response_event.get("data", {}).get("accountno")
                print(f"[RiskScoringConsumer] COMPLETED: account={acc}, event_type={event_type} -> Published to {dest} (success={published})", flush=True)
                processed_count += 1
            else:
                print(f"[!] Analysis pipeline failed: {status}", flush=True)

        except Exception as exc:
            print(f"[!] Error processing event: {exc}", flush=True)

        if once:
            break

    c.close()
    print(f"[RiskScoringConsumer] Daemon stopped. Total events processed: {processed_count}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="LinkX Risk Scoring Kafka Consumer Daemon")
    parser.add_argument("--brokers", default=None, help="Kafka broker bootstrap servers")
    parser.add_argument("--topic", default=None, help="Input Kafka topic name")
    parser.add_argument("--group-id", default=None, help="Kafka consumer group id")
    parser.add_argument("--once", action="store_true", help="Process one message and exit")
    args = parser.parse_args()

    run_consumer_loop(
        brokers=args.brokers,
        input_topic=args.topic,
        group_id=args.group_id,
        once=args.once,
    )


if __name__ == "__main__":
    main()
