import json
import re
from datetime import datetime

def normalize_rule_key(name):
    key = str(name or "").strip().lower()
    key = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
    return key or "analysis_rule"

def generate_python_rule(json_data, output_file):
    rule_name = json_data.get("rule_name", "AnalysisRule")
    node_label_default = json_data.get("node_label", "Node")

    code_lines = [
        "from datetime import datetime",
        "from logger import log_writer",
        "",
        f"def main(driver, session_id, nodes_label='{node_label_default}', log_file=None, high_risk_accounts=None, threshold_multiplier=3):",
        f"    log_writer(log_file, f'[{datetime.now()}] [Info] Starting analysis: {rule_name}')",
        "    if high_risk_accounts is None:",
        "        high_risk_accounts = []",
        "    label = f'`{nodes_label}`'",
        "    with driver.session() as session:",
    ]

    for rule in json_data["rules"]:
        rid = rule["id"]
        rel_name = rule["relationship"]["name"]
        rel_props = rule["relationship"].get("properties", {})
        bgcolor = rel_props.get("bgcolor", "#CCC")        

        # Safe Neo4j MERGE command
        code_lines += [
            f"        # Rule: {rid} - {rule.get('description','')}",
            f"        session.run(f\"\"\"",
            f"            MATCH (t:{{label}})",
            f"            WITH t",
            f"            ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME",
            f"            UNWIND range(0, size(collect(t))-2) AS i",
            f"            WITH collect(t)[i] AS a, collect(t)[i+1] AS b",
            f"            MERGE (a)-[r:{rel_name} {{session_id: $session_id}}]->(b)",
            f"            SET r.bgcolor = '{bgcolor}'", 
            f"        \"\"\", session_id=session_id)",
            ""
        ]

    code_lines.append(f"log_writer(log_file, f'[{datetime.now()}] [Success] Analysis completed')")

    with open(output_file, "w") as f:
        try:
            f.write("\n".join(code_lines))
            return True
        except Exception as e:
            return False
    print(f"Python rule file generated: {output_file}")
