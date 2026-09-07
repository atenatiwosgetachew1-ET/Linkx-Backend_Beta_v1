import re

filepath = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(filepath, "r") as f:
    code = f.read()

# Wrap the KafkaProducer init in a broader try/except block
old_init = """    try:
        from kafka import KafkaProducer as Producer
        kafka_producer = Producer(bootstrap_servers=kafka_brokers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        kafka_available = True
    except ImportError:
        print("[xvigilance] Warning: kafka-python not installed. Kafka streaming disabled.", flush=True)
        kafka_available = False
        kafka_producer = None"""

new_init = """    try:
        from kafka import KafkaProducer as Producer
        import json
        kafka_producer = Producer(
            bootstrap_servers=kafka_brokers.split(',') if ',' in kafka_brokers else kafka_brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        kafka_available = True
        print(f"[xvigilance] Successfully connected to Kafka Brokers: {kafka_brokers}", flush=True)
    except ImportError:
        print("[xvigilance] Warning: kafka-python not installed. Kafka streaming disabled.", flush=True)
        kafka_available = False
        kafka_producer = None
    except Exception as e:
        print(f"[xvigilance] CRITICAL: Could not connect to Kafka broker at {kafka_brokers}. Error: {e}", flush=True)
        print("[xvigilance] Streaming is temporarily disabled until broker recovers.", flush=True)
        kafka_available = False
        kafka_producer = None"""

code = code.replace(old_init, new_init)

with open(filepath, "w") as f:
    f.write(code)

print("Fixed Kafka crash!")
