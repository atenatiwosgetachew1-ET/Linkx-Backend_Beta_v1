import re
path = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py"
with open(path, "r") as f:
    text = f.read()

text = re.sub(r'group_id\s*=\s*".+"', 'group_id = "linkx-xvigilance-super-final-500"', text)

with open(path, "w") as f:
    f.write(text)
