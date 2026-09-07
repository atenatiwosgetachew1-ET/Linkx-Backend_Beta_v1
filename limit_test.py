import re

filepath = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(filepath, "r") as f:
    code = f.read()

# Add a limit break condition
old_logic = """                    print(f"[xvigilance] Extracted and routed micro-batch of {len(page)} records to Kafka.", flush=True)"""
new_logic = """                    print(f"[xvigilance] Extracted and routed micro-batch of {len(page)} records to Kafka.", flush=True)
                    if total_records >= 500:
                        print(f"[xvigilance] DEV LIMIT REACHED. Stopping at {total_records} records.", flush=True)
                        break"""

code = code.replace(old_logic, new_logic)

with open(filepath, "w") as f:
    f.write(code)

print("Added DEV limit!")
