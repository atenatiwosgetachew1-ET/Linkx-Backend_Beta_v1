import re

fetcher_path = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/fetcher.py"
with open(fetcher_path, "r") as f:
    text = f.read()

# Make sure we don't duplicate it if it's already there
if "DEV LIMIT" not in text:
    text = text.replace('            if records:\n                yield records', '            if records:\n                yield records[:500]\n                break')

with open(fetcher_path, "w") as f:
    f.write(text)
