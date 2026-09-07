import re
import os

# 1. Add the DEV 500 Limit back to runner.py
runner_path = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(runner_path, "r") as f:
    runner_code = f.read()

old_inner = """                            kafka_producer.send(
                                topic=kafka_topic,
                                value=txn,
                                headers=headers
                            )"""

new_inner = """                            kafka_producer.send(
                                topic=kafka_topic,
                                value=txn,
                                headers=headers
                            )
                            if total_records - len(page) + (page.index(txn) + 1) >= 500:
                                break"""

runner_code = runner_code.replace(old_inner, new_inner)

old_outer = """                    # ========================================================================="""
new_outer = """                    if total_records >= 500:
                        break
                    # ========================================================================="""

runner_code = runner_code.replace(old_outer, new_outer)

with open(runner_path, "w") as f:
    f.write(runner_code)


# 2. Reset the Kafka Group ID in the consumer
consumer_path = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py"
with open(consumer_path, "r") as f:
    consumer_code = f.read()

consumer_code = consumer_code.replace('group_id="linkx-xvigilance-worker-ingestion"', 'group_id="linkx-xvigilance-trial-run"')

with open(consumer_path, "w") as f:
    f.write(consumer_code)

print("Code set to Trial Mode!")
