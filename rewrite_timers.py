import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    # Ensure start_time is set after try:
    if line.strip() == "try:" and lines[i-1].strip().startswith("# ----"):
        new_lines.append(line)
        new_lines.append(line.replace("try:", "start_time = __import__('datetime').datetime.now()"))
        continue
        
    # Replace plain success prints
    if "print(\"  [Rule]" in line and "✓\"" in line and "s)\"" not in line:
        # It's a plain print like print("  [Rule] SMURFING ✓", flush=True)
        # We replace it with the f-string version
        match = re.search(r'print\("  \[Rule\] (.*?) ✓", flush=True\)', line)
        if match:
            rule_name = match.group(1)
            indent = line[:len(line) - len(line.lstrip())]
            new_lines.append(f'{indent}print(f"  [Rule] {rule_name} ✓ ({{(__import__(\'datetime\').datetime.now() - start_time).total_seconds():.2f}}s)", flush=True)\n')
            continue
            
    new_lines.append(line)

with open(file_path, 'w') as f:
    f.writelines(new_lines)

print("Timers rewritten.")
