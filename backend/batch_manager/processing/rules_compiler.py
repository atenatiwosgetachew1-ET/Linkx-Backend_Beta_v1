import json
from datetime import datetime

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
        "    with driver.session() as session:",
    ]

    for rule in json_data["rules"]:
        rid = rule["id"]
        match_label = rule["match"].get("node_label", node_label_default)
        rel_name = rule["relationship"]["name"]
        rel_props = rule["relationship"].get("properties", {})
        bgcolor = rel_props.get("bgcolor", "#CCC")        

        # Safe Neo4j MERGE command
        code_lines += [
            f"        # Rule: {rid} - {rule.get('description','')}",
            f"        session.run(\"\"\"",
            f"            MATCH (t:{match_label})",
            f"            WITH t",
            f"            ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME",
            f"            UNWIND range(0, size(collect(t))-2) AS i",
            f"            WITH collect(t)[i] AS a, collect(t)[i+1] AS b",
            f"            MERGE (a)-[r:{rel_name}]->(b)",
            f"            SET r.bgcolor = '{bgcolor}', r.session_id = '{session_id}'", 
            f"        \"\"\")",
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
