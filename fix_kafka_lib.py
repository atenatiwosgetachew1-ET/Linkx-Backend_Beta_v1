import re

filepath = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(filepath, "r") as f:
    code = f.read()

# Replace the confluent_kafka import with kafka-python
code = code.replace("from confluent_kafka import Producer", "from kafka import KafkaProducer as Producer")
code = code.replace("kafka_producer = Producer({'bootstrap.servers': kafka_brokers})", "kafka_producer = Producer(bootstrap_servers=kafka_brokers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))")
code = code.replace("print(\"[xvigilance] Warning: confluent_kafka not installed. Kafka streaming disabled.\", flush=True)", "print(\"[xvigilance] Warning: kafka-python not installed. Kafka streaming disabled.\", flush=True)")

# Replace the produce logic inside the loop
old_produce_logic = """                            # Fire to Kafka (internal buffer handles efficient batching)
                            kafka_producer.produce(
                                topic=kafka_topic,
                                value=json.dumps(txn).encode('utf-8'),
                                headers=headers
                            )
                        
                        # Trigger delivery callbacks for the page
                        kafka_producer.poll(0)"""

new_produce_logic = """                            # Fire to Kafka (internal buffer handles efficient batching)
                            kafka_producer.send(
                                topic=kafka_topic,
                                value=txn,
                                headers=headers
                            )"""

code = code.replace(old_produce_logic, new_produce_logic)

# Replace the watermark logic
old_watermark = """                    kafka_producer.produce(
                        topic=kafka_topic,
                        value=json.dumps(watermark).encode('utf-8'),
                        headers=[("source", b"xvigilance-daemon"), ("session_id", b"XVIGILANCE_FINDINGS"), ("type", b"watermark")]
                    )"""

new_watermark = """                    kafka_producer.send(
                        topic=kafka_topic,
                        value=watermark,
                        headers=[("source", b"xvigilance-daemon"), ("session_id", b"XVIGILANCE_FINDINGS"), ("type", b"watermark")]
                    )"""

code = code.replace(old_watermark, new_watermark)

with open(filepath, "w") as f:
    f.write(code)

print("Fixed Kafka library import!")
