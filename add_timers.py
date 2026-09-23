import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    content = f.read()

# Make sure datetime is imported
if "from datetime import datetime" not in content and "import datetime" not in content:
    content = "import datetime\n" + content

# Function to inject timers.
# We look for lines like: print("  [Rule] RULE_NAME ✓", flush=True)
# And we want to capture the start time right before `with driver.session() as s:`
# Actually, the easiest way is to use regex to find:
# try:
#     with driver.session() as s:
#         query = ...
#         s.run(...)
#     rules_completed.append("XYZ")
#     print("  [Rule] XYZ ✓", flush=True)

# Let's do a simple regex substitution for the print statement.
# Wait, we need the start time.
# We can replace `try:\n            with driver.session() as s:`
# with `try:\n            start_time = datetime.datetime.now()\n            with driver.session() as s:`

content = re.sub(
    r'try:\n(\s*)with driver.session\(\) as s:',
    r'try:\n\g<1>start_time = __import__("datetime").datetime.now()\n\g<1>with driver.session() as s:',
    content
)

# And replace `print("  [Rule] XYZ ✓", flush=True)`
# with `print(f"  [Rule] XYZ ✓ ({(datetime.datetime.now() - start_time).total_seconds():.2f}s)", flush=True)`

content = re.sub(
    r'print\("  \[Rule\] (.*?) ✓", flush=True\)',
    r'print(f"  [Rule] \1 ✓ ({(__import__("datetime").datetime.now() - start_time).total_seconds():.2f}s)", flush=True)',
    content
)

with open(file_path, 'w') as f:
    f.write(content)

print("Added timing instrumentation.")
