import re

filepath = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py"
with open(filepath, "r") as f:
    code = f.read()

code = code.replace('session_id = "XVIGILANCE_FINDINGS"', 'session_id = "xvigilance-daemon"')

with open(filepath, "w") as f:
    f.write(code)

print("Fixed consumer session id!")
