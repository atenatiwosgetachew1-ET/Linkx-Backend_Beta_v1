import re

# 1. Clean runner.py
runner_path = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(runner_path, "r") as f:
    text = f.read()
text = text.replace('print("[xvigilance] DEV LIMIT: Hard stop after 1 window.", flush=True)\n            break', 'if once:\n                print("[xvigilance] Run-once mode finished.", flush=True)\n                break')
with open(runner_path, "w") as f:
    f.write(text)

# 2. Clean fetcher.py
fetcher_path = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/fetcher.py"
with open(fetcher_path, "r") as f:
    text = f.read()
text = text.replace('            # DEV LIMIT: ONLY YIELD EXACTLY ONE PAGE OF 500\n            yield page[:500]\n            print("[fetcher] DEV LIMIT: Stopping after 500 records.", flush=True)\n            break', '            yield page')
with open(fetcher_path, "w") as f:
    f.write(text)
