import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    content = f.read()

content = content.replace(
    'print("  [Rule] LATE_NIGHT_TX ✓", flush=True)',
    'print(f"  [Rule] LATE_NIGHT_TX ✓ ({(__import__(\'datetime\').datetime.now() - start_time).total_seconds():.2f}s)", flush=True)'
)

content = content.replace(
    'print("  [Rule] JUST_BELOW_THRESHOLD ✓", flush=True)',
    'print(f"  [Rule] JUST_BELOW_THRESHOLD ✓ ({(__import__(\'datetime\').datetime.now() - start_time).total_seconds():.2f}s)", flush=True)'
)

content = content.replace(
    'print("  [Rule] RAPID_WITHDRAWAL ✓", flush=True)',
    'print(f"  [Rule] RAPID_WITHDRAWAL ✓ ({(__import__(\'datetime\').datetime.now() - start_time).total_seconds():.2f}s)", flush=True)'
)

with open(file_path, 'w') as f:
    f.write(content)

print("Patched remaining timers.")
