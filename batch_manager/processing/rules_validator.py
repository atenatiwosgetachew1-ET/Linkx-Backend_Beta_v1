import json
import re
from jsonschema import validate, ValidationError

RULE_SCHEMA = {
    "type": "object",
    "properties": {
        "rule_name": {"type": "string"},
        "node_label": {"type": "string"},
        "rules": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "type", "match", "relationship"],
                "properties": {
                    "id": {"type": "string"},
                    "description": {"type": "string"},
                    "type": {"type": "string", "enum": ["SEQUENTIAL","PREVIOUS","SELF","CIRCULAR","WINDOWED","PAIRWISE"]},
                    "match": {"type": "object"},
                    "filter": {"type": "object"},
                    "relationship": {"type": "object"},
                    "thresholds": {"type": "object"},
                    "advanced": {"type": "object"}
                }
            }
        }
    },
    "required": ["rule_name", "rules"]
}

def validate_rules_json(json_file_path: str):
    with open(json_file_path) as f:
        data = json.load(f)
    try:
        validate(instance=data, schema=RULE_SCHEMA)
        for rule in data.get("rules", []):
            rel_name = rule.get("relationship", {}).get("name")
            if not rel_name:
                raise ValueError(f"Rule '{rule.get('id', '')}' is missing relationship.name")
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", rel_name):
                raise ValueError(f"Invalid relationship name '{rel_name}'")
        print("JSON is valid ")
        return data
    except ValidationError as e:
        raise ValueError(f"Invalid JSON: {e.message}")
