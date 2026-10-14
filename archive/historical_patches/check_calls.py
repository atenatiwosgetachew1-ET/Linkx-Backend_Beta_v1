import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

calls = re.findall(r'CALL\s*\([^\)]+\)\s*\{.*?\}\s*IN TRANSACTIONS', text, re.DOTALL)
print(f"Found {len(calls)} instances of CALL (variables) {{}} IN TRANSACTIONS")
for i, call in enumerate(calls):
    print(f"\n--- CALL {i+1} ---")
    print(call[:200] + "\n...\n" + call[-100:])
