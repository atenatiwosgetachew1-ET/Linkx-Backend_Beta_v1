file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

# Replace out_count < 1000 with out_count <= 50
content = content.replace('out_count < 1000', 'out_count <= 50')

with open(file_path, 'w') as f:
    f.write(content)

print("Updated bounds to 50.")
