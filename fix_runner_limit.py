import re

filepath = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(filepath, "r") as f:
    code = f.read()

# Remove the old limit logic outside the loop
code = code.replace("if total_records >= 500:\n                        print(f\"[xvigilance] DEV LIMIT REACHED. Stopping at {total_records} records.\", flush=True)\n                        break", "")

# Add the limit logic inside the inner loop
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

code = code.replace(old_inner, new_inner)

# Also break the outer loop if we hit 500
old_outer = """                    # ========================================================================="""
new_outer = """                    if total_records >= 500:
                        break
                    # ========================================================================="""

code = code.replace(old_outer, new_outer)

with open(filepath, "w") as f:
    f.write(code)

print("Fixed inner loop limit!")
