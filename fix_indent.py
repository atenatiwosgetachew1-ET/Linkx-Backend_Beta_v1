file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    if "start_time = __import__('datetime').datetime.now()" in line:
        # Check if the previous line is 'try:'
        if i > 0 and "try:" in lines[i-1]:
            # This line needs to be indented 4 spaces more than the 'try:' line
            indent = lines[i-1][:len(lines[i-1]) - len(lines[i-1].lstrip())]
            new_lines.append(indent + "    " + line.lstrip())
            continue
    new_lines.append(line)

with open(file_path, 'w') as f:
    f.writelines(new_lines)

print("Indentation fixed.")
