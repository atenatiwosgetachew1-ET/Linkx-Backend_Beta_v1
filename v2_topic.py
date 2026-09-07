import re

runner_path = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(runner_path, "r") as f:
    text = f.read()
text = text.replace('dev.xvigilance.transactions.raw.v1', 'dev.xvigilance.transactions.raw.v2')
with open(runner_path, "w") as f:
    f.write(text)

consumer_path = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py"
with open(consumer_path, "r") as f:
    text = f.read()
text = text.replace('dev.xvigilance.transactions.raw.v1', 'dev.xvigilance.transactions.raw.v2')
with open(consumer_path, "w") as f:
    f.write(text)
