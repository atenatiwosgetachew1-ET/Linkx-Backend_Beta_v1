import json
import re

def normalize_rule_key(name):
    key = str(name or "").strip().lower()
    key = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
    return key or "analysis_rule"

def generate_python_rule(json_data, output_file):
    rule_json = json.dumps(json_data, indent=2, sort_keys=True)
    code = f'''import json

from batch_manager.processing.uploaded_rule_engine import run_uploaded_rules


RULE_SET = json.loads({rule_json!r})


def main(driver, session_id, nodes_label=None, log_file=None, high_risk_accounts=None, threshold_multiplier=3):
    return run_uploaded_rules(
        driver,
        session_id,
        nodes_label or RULE_SET.get("node_label", "Node"),
        log_file,
        RULE_SET,
        incremental=False,
    )


def incremental(driver, session_id, nodes_label, batch_id, log_file, high_risk_accounts=None, threshold_multiplier=3):
    return run_uploaded_rules(
        driver,
        session_id,
        nodes_label or RULE_SET.get("node_label", "Node"),
        log_file,
        RULE_SET,
        batch_id=batch_id,
        incremental=True,
    )
'''
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(code)
    return True
