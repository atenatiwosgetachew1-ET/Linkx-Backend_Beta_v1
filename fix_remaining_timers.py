import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    content = f.read()

# Replace any print(f"  [Rule] XYZ ✓", flush=True) with the timer version
content = re.sub(
    r'print\(f?"  \[Rule\] (.*?) ✓", flush=True\)',
    r'print(f"  [Rule] \1 ✓ ({(__import__(\'datetime\').datetime.now() - start_time).total_seconds():.2f}s)", flush=True)',
    content
)

with open(file_path, 'w') as f:
    f.write(content)

print("Forced timer replacements.")
