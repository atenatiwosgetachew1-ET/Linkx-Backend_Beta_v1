from kafka.admin import KafkaAdminClient, NewTopic
import time

broker = "172.27.23.106:9092"
topic_name = "dev.xvigilance.transactions.raw.v2"

print(f"Connecting to Kafka at {broker}...")
admin = KafkaAdminClient(bootstrap_servers=broker)

print(f"Deleting topic: {topic_name}")
try:
    admin.delete_topics([topic_name])
    print("Delete command sent. Waiting for cluster to process...")
    time.sleep(5)
except Exception as e:
    print(f"Topic might not exist or error: {e}")

print(f"Recreating topic: {topic_name}")
try:
    new_topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
    admin.create_topics([new_topic])
    print("Successfully recreated pristine Kafka topic!")
except Exception as e:
    print(f"Failed to create topic: {e}")

admin.close()
