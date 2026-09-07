import re

runner_path = "/var/www/linkx-backend/service_factory/services/Linkx_xmaintenance/src/linkx_xvigilance/runner.py"
with open(runner_path, "r") as f:
    runner_code = f.read()

# Remove all the garbage break statements
runner_code = runner_code.replace("""                    if total_records >= 500:
                        break
                    if total_records >= 500:
                        break""", "")

runner_code = runner_code.replace("""                    if total_records >= 500:
                        break""", "")

runner_code = runner_code.replace("""                            if total_records - len(page) + (page.index(txn) + 1) >= 500:
                                break""", "")

with open(runner_path, "w") as f:
    f.write(runner_code)
