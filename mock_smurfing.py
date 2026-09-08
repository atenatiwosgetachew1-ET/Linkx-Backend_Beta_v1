import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

def send_mock_smurfing():
    brokers = "172.27.23.106:9092"
    topic = "dev.xvigilance.transactions.raw.v2"
    
    producer = KafkaProducer(
        bootstrap_servers=brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    now_ms = int(time.time() * 1000)
    window_start = datetime.now(timezone.utc)
    
    print(f"Pushing mock smurfing transactions to {topic}...")
    
    headers = [
        ("source", b"xvigilance-daemon"),
        ("session_id", b"XVIGILANCE_FINDINGS"),
        ("window_id", window_start.isoformat().encode('utf-8'))
    ]
    
    for i in range(4):
        txn = {
            "ACCOUNTNO": "MOCK_SENDER_123",
            "BENACCOUNTNO": "MOCK_RECEIVER_456",
            "TRANSACTIONDATE": now_ms + (i * 1000),
            "AMOUNT": 8000,
            "es_id": f"mock_txn_{i}"
        }
        producer.send(topic, value=txn, headers=headers)
        
    producer.flush()
    print("Injected 4 smurfing transactions.")
    
    print("Pushing watermark...")
    watermark = {
        "event": "WINDOW_COMPLETE",
        "window_id": window_start.isoformat(),
        "total_records": 4
    }
    producer.send(
        topic,
        value=watermark,
        headers=[("source", b"xvigilance-daemon"), ("session_id", b"XVIGILANCE_FINDINGS"), ("type", b"watermark")]
    )
    producer.flush()
    print("Done! Check worker logs.")

if __name__ == "__main__":
    send_mock_smurfing()
