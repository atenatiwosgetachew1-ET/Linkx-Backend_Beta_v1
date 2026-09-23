file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

# Revert out_count <= 50 back to out_count < 1000 to strictly respect the original semantics
content = content.replace('out_count <= 50', 'out_count < 1000')

with open(file_path, 'w') as f:
    f.write(content)

print("Reverted bounds back to 1000.")
