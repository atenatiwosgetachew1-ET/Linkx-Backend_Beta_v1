import re

runner_path = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(runner_path, "r") as f:
    text = f.read()

text = text.replace('if once:\n                print("[xvigilance] Run-once mode finished.", flush=True)\n                break', 'print("[xvigilance] DEV LIMIT: Hard stop after 1 window.", flush=True)\n            break')

with open(runner_path, "w") as f:
    f.write(text)
