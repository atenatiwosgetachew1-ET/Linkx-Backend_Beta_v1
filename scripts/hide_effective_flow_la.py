import re

def hide_effective_flow_la(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # Remove r.reason from EFFECTIVE_FLOW
    content = re.sub(r"r\.reason = 'funds flow through trusted intermediary',\s*", "", content)

    with open(file_path, "w") as f:
        f.write(content)

hide_effective_flow_la("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
