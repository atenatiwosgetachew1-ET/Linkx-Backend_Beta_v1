import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    content = f.read()

# Remove the incorrectly placed FRAUD_AGGREGATOR block from fast_ingest_batch
# We know it starts at line 440 and ends before `finally:` at line 456.
# Let's use regex to remove it if it is right after `SET n = row, n.node_identity = 'Entity Node'`

bad_block_pattern = r"(\s*# ---- 14\. FRAUD_AGGREGATOR ----\s*try:[\s\S]*?print\(f\"  \[Rule\] FRAUD_AGGREGATOR ✗ \{str\(e\)\[:100\]\}\", flush=True\)\s*)(finally:\s*driver\.close\(\)\s*def fetch_global_entities)"

# Wait, it's safer to just delete the lines 440 to 455 inclusive
lines = content.split('\n')
new_lines = []
skip = False
for i, line in enumerate(lines):
    if i == 439 and "# ---- 14. FRAUD_AGGREGATOR ----" in line:
        skip = True
    
    if skip and line.strip() == "finally:" and "driver.close()" in lines[i+1]:
        skip = False
        new_lines.append(line)
        continue
        
    if not skip:
        new_lines.append(line)

with open(file_path, 'w') as f:
    f.write('\n'.join(new_lines))

print("Fixed consumer.")
